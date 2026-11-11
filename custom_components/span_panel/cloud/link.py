"""A local panel's link to its SPAN account, used on demand.

A panel on SPAN API is read and controlled locally. A few things only SPAN's
cloud can do: write a circuit's name, and read the panel's metered history.
For those, the panel's entry can carry a SPAN account as a subentry. Nothing
runs while it is idle: each use opens the account's stream, does the one thing
asked, and lets the stream go again. The link adds no device and no entity.

The two sides name one panel differently. SPAN API knows it by its gateway
serial and numbers its circuits one way; the cloud knows it by its board
serial and numbers them another. What both report identically is the breaker
spaces a circuit occupies, so a circuit is found on the other side by its
spaces, and a linked account whose panel does not hold every one of the local
panel's circuits is refused as the wrong panel.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from span_panel_api import SpanPanelSnapshot

from custom_components.span_panel.const import (
    CONF_CLOUD_DEVICE_UUID,
    CONF_CLOUD_PANEL_KEY,
    CONF_CLOUD_TOKENS,
    CONF_CLOUD_USER_ID,
    DEFAULT_SNAPSHOT_INTERVAL,
    DOMAIN,
)
from custom_components.span_panel.options import SNAPSHOT_UPDATE_INTERVAL

if TYPE_CHECKING:
    from .client import SpanCloudClient

SUBENTRY_CLOUD_ACCOUNT = "cloud_account"

type Spaces = tuple[int, ...]


class CloudPanelMismatch(HomeAssistantError):
    """The linked account's panel is not the panel this entry reads locally."""


def cloud_account(entry: ConfigEntry) -> ConfigSubentry | None:
    """Return the SPAN account linked to a panel's entry, if it has one."""
    for subentry in entry.subentries.values():
        if subentry.subentry_type == SUBENTRY_CLOUD_ACCOUNT:
            return subentry
    return None


def _circuits_by_spaces(snapshot: SpanPanelSnapshot) -> dict[Spaces, str]:
    """Map each circuit's breaker spaces to its id. A circuit with no spaces has no place."""
    return {
        tuple(sorted(circuit.tabs)): circuit_id
        for circuit_id, circuit in snapshot.circuits.items()
        if circuit.tabs
    }


class LinkedCloudPanel:
    """The cloud's view of a local panel, for the length of one use."""

    def __init__(self, client: SpanCloudClient, local: SpanPanelSnapshot) -> None:
        """Pair the cloud client with the local panel's circuits, by breaker spaces."""
        self.client = client
        self._local = local
        self._cloud_ids: dict[Spaces, str] = {}

    async def async_match(self) -> None:
        """Find each local circuit on the cloud side, or refuse the panel as the wrong one."""
        self._cloud_ids = _circuits_by_spaces(await self.client.get_snapshot())
        missing = sorted(set(_circuits_by_spaces(self._local)) - set(self._cloud_ids))
        if missing:
            raise CloudPanelMismatch(
                f"The linked SPAN account's panel ({self.client.serial}) has no circuit on "
                f"spaces {', '.join('/'.join(map(str, spaces)) for spaces in missing)}: "
                "it is not this panel. Remove the SPAN account from this panel and add "
                "the right one.",
                translation_domain=DOMAIN,
                translation_key="cloud_panel_mismatch",
                translation_placeholders={"panel": self.client.serial},
            )

    def cloud_circuit_id(self, circuit_id: str) -> str:
        """Return the cloud's id for a local circuit. `ValueError` if it has none."""
        circuit = self._local.circuits.get(circuit_id)
        cloud_id = self._cloud_ids.get(tuple(sorted(circuit.tabs))) if circuit else None
        if cloud_id is None:
            raise ValueError(f"no circuit {circuit_id} on this panel that SPAN's cloud knows")
        return cloud_id

    async def rename_circuit(self, circuit_id: str, name: str) -> str:
        """Rename a local circuit through the cloud. Returns the name as sent."""
        return await self.client.rename_circuit(self.cloud_circuit_id(circuit_id), name)


@asynccontextmanager
async def async_linked_cloud_panel(
    hass: HomeAssistant, entry: ConfigEntry, local: SpanPanelSnapshot
) -> AsyncIterator[LinkedCloudPanel]:
    """Open the linked account for one use, and let its stream go afterwards.

    Raises `HomeAssistantError` when the entry has no account linked, and
    `CloudPanelMismatch` when the account's panel is not this one. Whatever the
    sign-in or the stream raises passes through for the caller to report.
    """
    # Imported here so a local-only install never loads the cloud client.
    from .client import SpanCloudClient  # noqa: PLC0415
    from .hub import async_acquire_feed  # noqa: PLC0415

    account = cloud_account(entry)
    if account is None:
        raise HomeAssistantError(
            "This panel has no SPAN account linked.",
            translation_domain=DOMAIN,
            translation_key="cloud_account_missing",
        )
    data: dict[str, Any] = dict(account.data)
    hub, feed = await async_acquire_feed(
        hass,
        tokens=data[CONF_CLOUD_TOKENS],
        user_id=data[CONF_CLOUD_USER_ID],
        device_uuid=data[CONF_CLOUD_DEVICE_UUID],
        panel_key=data[CONF_CLOUD_PANEL_KEY],
    )
    # From here the client owns the feed: closing it is what releases the stream.
    client = SpanCloudClient(
        hass,
        hub,
        feed,
        snapshot_interval=entry.options.get(SNAPSHOT_UPDATE_INTERVAL, DEFAULT_SNAPSHOT_INTERVAL),
    )
    try:
        await client.connect()
        panel = LinkedCloudPanel(client, local)
        await panel.async_match()
        yield panel
    finally:
        await client.close()
