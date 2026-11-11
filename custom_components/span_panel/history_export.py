"""Export SPAN's metered energy history to CSV, straight from SPAN's cloud.

The SPAN app can only e-mail a month at a time, one file per panel, in local
wall-clock time with repeated names. This asks SPAN's history API
(`GetHistoryAggregation`, the one the energy sensors already use) for any range
at any of its resolutions, and writes one CSV per panel:

- times as UTC and as local time *with* its offset, so the autumn DST hour is
  never ambiguous;
- one column per circuit, named with its panel spaces, so circuits that share a
  name stay apart; an export column only where the circuit exported anything;
- the site's flows that carry data on this panel (grid, home, battery, solar).

Files land in `<config>/span_history/`. Two ways in:

- the `span_panel.export_history` action, for automations and the developer
  tools, which answers with each file's path and totals;
- the `span_panel/export_history` websocket command, for the card, which also
  answers with a short-lived signed link to download each file.

Only over SPAN's cloud: the local eBus API has no history.
"""

from __future__ import annotations

from collections.abc import Iterable
import csv
from datetime import UTC, datetime, timedelta
import io
import logging
from pathlib import Path
import re
from typing import Any, cast

from aiohttp import web
from homeassistant.components import websocket_api
from homeassistant.components.http import HomeAssistantView
from homeassistant.components.http.auth import async_sign_path
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
import homeassistant.helpers.config_validation as cv
from homeassistant.util import dt as dt_util
from homeassistant.util.json import JsonValueType
import voluptuous as vol

from .cloud.client import SpanCloudClient
from .cloud.link import async_linked_cloud_panel, cloud_account
from .cloud.span_client.cloud_history import Bucket, Resolution
from .const import DOMAIN
from .runtime import loaded_runtime_data

_LOGGER = logging.getLogger(__name__)

EXPORT_DIR = "span_history"
DOWNLOAD_URL = "/api/span_panel/history"
LINK_LIFETIME = timedelta(minutes=10)

RESOLUTIONS: dict[str, Resolution] = {
    "minute": Resolution.MINUTELY,
    "quarter_hour": Resolution.QUARTER_HOURLY,
    "hour": Resolution.HOURLY,
    "day": Resolution.DAILY,
    "month": Resolution.MONTHLY,
}

# Longest range one export may ask for, per resolution: a guard against a typo
# asking for years of minutes, not a limit SPAN is known to set.
MAX_RANGE: dict[str, timedelta] = {
    "minute": timedelta(days=31),
    "quarter_hour": timedelta(days=366),
    "hour": timedelta(days=3 * 366),
    "day": timedelta(days=10 * 366),
    "month": timedelta(days=10 * 366),
}

# SiteMeasurement components, in SPAN's order, with the column label each gets.
SITE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("grid_import", "Grid import"),
    ("grid_export", "Grid export"),
    ("home_import", "Home import"),
    ("home_export", "Home export"),
    ("battery_import", "Battery import"),
    ("battery_export", "Battery export"),
    ("solar_to_grid", "Solar to grid"),
    ("solar_to_home", "Solar to home"),
    ("solar_to_battery", "Solar to battery"),
    ("battery_to_home", "Battery to home"),
    ("grid_to_home", "Grid to home"),
)

_SITE_NODE = "site"
_FILENAME = re.compile(r"^[A-Za-z0-9_.-]+\.csv$")


# --- the CSV --------------------------------------------------------------------


def _spaces_label(spaces: Iterable[int]) -> str:
    return "+".join(str(space) for space in sorted(spaces))


def _circuit_order(node: str, labels: dict[str, tuple[str, tuple[int, ...]]]) -> tuple[int, str]:
    _name, spaces = labels.get(node, ("", ()))
    return (min(spaces) if spaces else 10_000, node)


def build_csv(
    history: dict[str, dict[int, Bucket]],
    labels: dict[str, tuple[str, tuple[int, ...]]],
    time_zone: Any,
) -> tuple[str, int, dict[str, float]]:
    """Build the CSV text, its row count, and each column's total in kWh.

    Columns are only those with something in them: a circuit's export when it
    exported anything, a site flow when the panel meters it. A bucket SPAN did
    not return is left empty, not written as zero.
    """
    columns: list[tuple[str, str, str]] = []  # (header, node, component)
    circuits = sorted(
        (node for node in history if node.startswith("circuit-")),
        key=lambda node: _circuit_order(node, labels),
    )
    for node in circuits:
        name, spaces = labels.get(node, (node, ()))
        where = f" [{_spaces_label(spaces)}]" if spaces else ""
        columns.append((f"{name}{where} (kWh)", node, "import"))
        if any(b.values.get("export") for b in history[node].values()):
            columns.append((f"{name}{where} export (kWh)", node, "export"))
    site = history.get(_SITE_NODE, {})
    for component, label in SITE_COLUMNS:
        if any(component in b.values for b in site.values()):
            columns.append((f"{label} (kWh)", _SITE_NODE, component))

    starts = sorted({start for buckets in history.values() for start in buckets})
    totals = {header: 0.0 for header, _node, _component in columns}
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["Start (UTC)", "Start (local)", "End (UTC)", *totals])
    for start in starts:
        end = max(
            (history[node][start].end_ms for node in history if start in history[node]),
            default=start,
        )
        begun = datetime.fromtimestamp(start / 1000, UTC)
        row = [
            begun.isoformat(),
            begun.astimezone(time_zone).isoformat(),
            datetime.fromtimestamp(end / 1000, UTC).isoformat(),
        ]
        for header, node, component in columns:
            bucket = history.get(node, {}).get(start)
            value = None if bucket is None else bucket.values.get(component)
            if value is None:
                row.append("")
            else:
                totals[header] += value
                row.append(f"{value:.7f}")
        writer.writerow(row)
    return out.getvalue(), len(starts), totals


# --- one export -----------------------------------------------------------------


def _cloud_entries(hass: HomeAssistant, entry_id: str | None) -> list[ConfigEntry]:
    """Return the loaded panels whose history SPAN's cloud can be asked for.

    A panel read through the cloud, or a local panel with a SPAN account linked.
    """
    entries = [
        entry
        for entry in hass.config_entries.async_loaded_entries(DOMAIN)
        if (entry_id is None or entry.entry_id == entry_id)
        and (runtime := loaded_runtime_data(entry)) is not None
        and (
            isinstance(runtime.coordinator.client, SpanCloudClient)
            or cloud_account(entry) is not None
        )
    ]
    if not entries:
        raise ServiceValidationError(
            "No SPAN panel connected through SPAN's cloud, or with a SPAN account linked, "
            "is loaded.",
            translation_domain=DOMAIN,
            translation_key="history_no_cloud_panels",
        )
    return entries


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt_util.get_default_time_zone())
    return dt_util.as_utc(value)


def _check_range(start: datetime, end: datetime, resolution: str) -> None:
    if end <= start:
        raise ServiceValidationError(
            "The end of the range must come after its start.",
            translation_domain=DOMAIN,
            translation_key="history_bad_range",
        )
    if end - start > MAX_RANGE[resolution]:
        raise ServiceValidationError(
            f"At {resolution} resolution one export covers at most {MAX_RANGE[resolution].days} days.",
            translation_domain=DOMAIN,
            translation_key="history_range_too_long",
            translation_placeholders={
                "resolution": resolution,
                "days": str(MAX_RANGE[resolution].days),
            },
        )


async def async_export_history(
    hass: HomeAssistant,
    *,
    start: datetime,
    end: datetime | None,
    resolution: str,
    entry_id: str | None = None,
) -> list[dict[str, Any]]:
    """Export each matching cloud panel's history to a CSV file; describe each file."""
    start_utc = _as_utc(start)
    end_utc = _as_utc(end) if end is not None else dt_util.utcnow()
    _check_range(start_utc, end_utc, resolution)
    entries = _cloud_entries(hass, entry_id)
    time_zone = dt_util.get_default_time_zone()
    folder = Path(hass.config.path(EXPORT_DIR))
    results: list[dict[str, Any]] = []
    for entry in entries:
        runtime = loaded_runtime_data(entry)
        assert runtime is not None
        coordinator = runtime.coordinator
        # The file is named for the panel as Home Assistant knows it.
        serial = coordinator.data.serial_number
        window = (
            int(start_utc.timestamp() * 1000),
            int(end_utc.timestamp() * 1000),
            RESOLUTIONS[resolution],
            str(time_zone),
        )
        try:
            if isinstance(coordinator.client, SpanCloudClient):
                history, labels = await coordinator.client.fetch_history(*window)
            else:
                async with async_linked_cloud_panel(hass, entry, coordinator.data) as panel:
                    history, labels = await panel.client.fetch_history(*window)
        except Exception as err:
            _LOGGER.warning("History export for %s failed: %s", serial, err)
            raise HomeAssistantError(
                f"SPAN's history for {serial} could not be read: {err}",
                translation_domain=DOMAIN,
                translation_key="history_fetch_failed",
                translation_placeholders={"panel": serial, "reason": str(err)},
            ) from err
        text, rows, totals = build_csv(history, labels, time_zone)
        filename = (
            f"span_{serial}_{start_utc.astimezone(time_zone):%Y%m%d-%H%M}"
            f"_{end_utc.astimezone(time_zone):%Y%m%d-%H%M}_{resolution}.csv"
        )
        path = folder / filename
        await hass.async_add_executor_job(_write, path, text)
        results.append(
            {
                "panel": serial,
                "title": entry.title,
                "file": filename,
                "path": str(path),
                "rows": rows,
                "totals_kwh": {name: round(total, 4) for name, total in totals.items()},
            }
        )
    return results


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


# --- the ways in ----------------------------------------------------------------

_EXPORT_FIELDS = {
    vol.Required("start"): cv.datetime,
    vol.Optional("end"): cv.datetime,
    vol.Optional("resolution", default="hour"): vol.In(list(RESOLUTIONS)),
    vol.Optional("config_entry_id"): cv.string,
}


async def _async_handle_export(call: ServiceCall) -> ServiceResponse:
    exports = await async_export_history(
        call.hass,
        start=call.data["start"],
        end=call.data.get("end"),
        resolution=call.data["resolution"],
        entry_id=call.data.get("config_entry_id"),
    )
    return {"exports": cast(list[JsonValueType], exports)}


@websocket_api.websocket_command(
    {
        vol.Required("type"): "span_panel/export_history",
        vol.Required("start"): str,
        vol.Optional("end"): str,
        vol.Optional("resolution", default="hour"): vol.In(list(RESOLUTIONS)),
        vol.Optional("config_entry_id"): str,
    }
)
@websocket_api.require_admin
@websocket_api.async_response
async def handle_export_history(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Export history as the action does, and answer with a download link per file."""
    start = dt_util.parse_datetime(msg["start"])
    end = dt_util.parse_datetime(msg["end"]) if msg.get("end") else None
    if start is None or (msg.get("end") and end is None):
        connection.send_error(msg["id"], "invalid", "start and end must be ISO date-times")
        return
    try:
        exports = await async_export_history(
            hass,
            start=start,
            end=end,
            resolution=msg["resolution"],
            entry_id=msg.get("config_entry_id"),
        )
    except HomeAssistantError as err:
        connection.send_error(msg["id"], "export_failed", str(err))
        return
    for export in exports:
        export["url"] = async_sign_path(
            hass,
            f"{DOWNLOAD_URL}/{export['file']}",
            LINK_LIFETIME,
            refresh_token_id=connection.refresh_token_id,
        )
    connection.send_result(msg["id"], {"exports": exports})


class HistoryDownloadView(HomeAssistantView):
    """Serve an exported file to a signed-in user, or through a signed link."""

    url = DOWNLOAD_URL + "/{filename}"
    name = "api:span_panel:history"
    requires_auth = True

    async def get(self, request: web.Request, filename: str) -> web.StreamResponse:
        """Send the file as a download."""
        if not _FILENAME.fullmatch(filename):
            return web.Response(status=404)
        hass: HomeAssistant = request.app["hass"]
        path = Path(hass.config.path(EXPORT_DIR)) / filename
        if not await hass.async_add_executor_job(path.is_file):
            return web.Response(status=404)
        return web.FileResponse(
            path,
            headers={
                "Content-Type": "text/csv; charset=utf-8",
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )


def async_register_history_export(hass: HomeAssistant) -> None:
    """Register the action, the websocket command and the download view, once."""
    hass.services.async_register(
        DOMAIN,
        "export_history",
        _async_handle_export,
        schema=vol.Schema(_EXPORT_FIELDS),
        supports_response=SupportsResponse.OPTIONAL,
    )
    websocket_api.async_register_command(hass, handle_export_history)
    http = getattr(hass, "http", None)
    if http is not None:
        http.register_view(HistoryDownloadView())
