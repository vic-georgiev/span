"""Circuit names as the SPAN account has them, for a panel read locally.

A circuit is renamed in SPAN's cloud: from the SPAN app, or from Home
Assistant through the linked account. The app shows the new name at once.
The panel's own local data does not follow: observed on firmware r202639,
`info/name` and SPAN's on-premise page kept the old name after renames from
either place. So where a panel has an account linked, the account's names
are the ones shown.

They are pulled, not streamed: when the entry is set up, once a day, and
after a rename made here. Each pull opens the account for a few seconds.
The names, when they were pulled, and the last changes seen are kept with
the link, so diagnostics can say what changed, when, and where it was seen.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from span_panel_api import SpanPanelSnapshot

from .link import LinkedCloudPanel, async_linked_cloud_panel, cloud_account

_LOGGER = logging.getLogger(__name__)

# Kept with the link: {"9/11": name} by breaker spaces, the pull's time, and
# the latest changes as {"at", "spaces", "from", "to", "seen"}.
CONF_NAMES = "names"
CONF_NAMES_PULLED_AT = "names_pulled_at"
CONF_NAME_CHANGES = "name_changes"

SEEN_IN_ACCOUNT = "span_account"  # found changed on a pull: renamed outside Home Assistant
SEEN_HERE = "home_assistant"  # renamed from Home Assistant

PULL_INTERVAL = timedelta(days=1)
# A change reloads the entry, which would pull again straight away.
_FRESH = timedelta(minutes=5)
_KEPT_CHANGES = 25


def spaces_key(tabs: list[int]) -> str:
    """Name a circuit by its breaker spaces, which both sides report alike."""
    return "/".join(str(tab) for tab in sorted(tabs))


def account_names(entry: ConfigEntry) -> dict[str, str]:
    """Return the linked account's circuit names by breaker spaces; empty without any."""
    account = cloud_account(entry)
    return dict(account.data.get(CONF_NAMES, {})) if account else {}


def with_account_names(snapshot: SpanPanelSnapshot, names: dict[str, str]) -> SpanPanelSnapshot:
    """Return the snapshot with each circuit named as the account names it."""
    if not names:
        return snapshot
    renamed = {
        circuit_id: replace(circuit, name=name)
        for circuit_id, circuit in snapshot.circuits.items()
        if circuit.tabs
        and (name := names.get(spaces_key(circuit.tabs))) is not None
        and name != circuit.name
    }
    if not renamed:
        return snapshot
    return replace(snapshot, circuits={**snapshot.circuits, **renamed})


def _record(hass: HomeAssistant, entry: ConfigEntry, names: dict[str, str], seen: str) -> bool:
    """Keep the account's names with the link, and log what changed. True if anything did."""
    account = cloud_account(entry)
    if account is None:
        return False
    known: dict[str, str] = account.data.get(CONF_NAMES, {})
    merged = {**known, **names}
    pulled = seen == SEEN_IN_ACCOUNT
    if merged == known and not (pulled and CONF_NAMES_PULLED_AT not in account.data):
        return False
    now = dt_util.utcnow().isoformat()
    # A name first learned on a pull is not a change; one set from here always is.
    changes = [
        {"at": now, "spaces": spaces, "from": known.get(spaces), "to": name, "seen": seen}
        for spaces, name in names.items()
        if known.get(spaces) != name and (spaces in known or not pulled)
    ]
    for change in changes:
        _LOGGER.info(
            "Circuit on spaces %s renamed from %r to %r (seen in %s)",
            change["spaces"],
            change["from"],
            change["to"],
            seen,
        )
    data = {
        **account.data,
        CONF_NAMES: merged,
        CONF_NAME_CHANGES: [*account.data.get(CONF_NAME_CHANGES, []), *changes][-_KEPT_CHANGES:],
    }
    if pulled:
        data[CONF_NAMES_PULLED_AT] = now
    hass.config_entries.async_update_subentry(entry, account, data=data)
    return True


def record_rename(hass: HomeAssistant, entry: ConfigEntry, tabs: list[int], name: str) -> None:
    """Keep a name SPAN has just confirmed for a rename made from Home Assistant."""
    if tabs:
        _record(hass, entry, {spaces_key(tabs): name}, SEEN_HERE)


def read_names(panel: LinkedCloudPanel) -> dict[str, str]:
    """Return the cloud panel's circuit names by breaker spaces."""
    return {
        spaces_key(circuit.tabs): circuit.name
        for circuit in panel.cloud_circuits()
        if circuit.tabs and circuit.name
    }


async def async_pull_names(
    hass: HomeAssistant, entry: ConfigEntry, local: SpanPanelSnapshot, *, force: bool = False
) -> None:
    """Read the account's circuit names, and keep them if any differ from the last pull.

    Best effort: a pull that fails leaves the names as they were and is logged.
    """
    account = cloud_account(entry)
    if account is None:
        return
    pulled_at = dt_util.parse_datetime(account.data.get(CONF_NAMES_PULLED_AT) or "")
    if not force and pulled_at is not None and dt_util.utcnow() - pulled_at < _FRESH:
        return
    try:
        async with async_linked_cloud_panel(hass, entry, local) as panel:
            names = read_names(panel)
    except Exception as err:  # noqa: BLE001 -- best effort; the names stay as they were
        _LOGGER.warning("Could not read circuit names from the SPAN account: %s", err)
        return
    _record(hass, entry, names, SEEN_IN_ACCOUNT)


def names_diagnostics(entry: ConfigEntry, panel_names: dict[str, str]) -> dict[str, Any] | None:
    """Say, per circuit, what the panel calls it and what the account does, and what changed."""
    account = cloud_account(entry)
    if account is None:
        return None
    names: dict[str, str] = account.data.get(CONF_NAMES, {})
    return {
        "pulled_at": account.data.get(CONF_NAMES_PULLED_AT),
        "changes": account.data.get(CONF_NAME_CHANGES, []),
        "differing": {
            spaces: {"panel": panel_names.get(spaces), "account": name}
            for spaces, name in names.items()
            if spaces in panel_names and panel_names[spaces] != name
        },
    }
