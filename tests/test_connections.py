"""One panel, two connections: a local entry and a cloud entry never step on each other.

The cloud entries here run the real hub, client and platforms over the fake
backend `test_cloud_transport` uses. The local entries are never set up: what
decides the cloud entry's role is only that an enabled local entry exists.
"""

from __future__ import annotations

from unittest.mock import patch

from homeassistant.config_entries import ConfigEntryDisabler, ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.span_panel import _async_reload_cloud_connection, _cloud_standby
from custom_components.span_panel.cloud import hub as hub_module
from custom_components.span_panel.connections import (
    CLOUD_UNIQUE_ID_PREFIX,
    async_namespace_cloud_entries,
    cloud_entry_for,
    cloud_unique_id,
    entry_serial,
    local_owner,
    serial_key,
)
from custom_components.span_panel.const import (
    CONF_API_VERSION,
    CONF_CLOUD_DEVICE_UUID,
    CONF_CLOUD_TOKENS,
    CONF_CLOUD_USER_ID,
    DOMAIN,
)
from custom_components.span_panel.diagnostics import async_get_config_entry_diagnostics

from .test_cloud_transport import SERIAL_16, SERIAL_40, SITE, FakeStream, _entry


def _cloud_entry(serial: str, model: str, *, disabled: bool = False) -> MockConfigEntry:
    """Build a cloud entry from before the split, optionally disabled."""
    entry = _entry(serial, model)
    if disabled:
        entry.disabled_by = ConfigEntryDisabler.USER
    return entry


def _local_entry(serial: str, *, disabled: bool = False) -> MockConfigEntry:
    """Build a local (SPAN API) entry for `serial`, keyed as upstream's config flow keys it."""
    return MockConfigEntry(
        domain=DOMAIN,
        version=7,
        minor_version=4,
        unique_id=serial,
        title=f"SPAN Panel {serial}",
        data={CONF_API_VERSION: "v2", "host": "192.0.2.40"},
        disabled_by=ConfigEntryDisabler.USER if disabled else None,
    )


def test_a_serial_matches_however_it_is_spelled() -> None:
    assert serial_key(" XG-2532-3HGE7 ") == serial_key("xg-2532-3hge7")
    assert serial_key("") is None
    assert serial_key(None) is None


async def test_cloud_entries_from_before_the_split_get_their_own_unique_id(
    hass: HomeAssistant,
) -> None:
    old = _entry(SERIAL_40, "MAIN 40")
    disabled = _cloud_entry(SERIAL_16, "MAIN 16", disabled=True)
    disabled.add_to_hass(hass)
    old.add_to_hass(hass)
    local = _local_entry("xg-0000-local")
    local.add_to_hass(hass)

    async_namespace_cloud_entries(hass)

    assert old.unique_id == cloud_unique_id(SERIAL_40)
    # A disabled entry holds its unique id just the same, so it moves too.
    assert disabled.unique_id == cloud_unique_id(SERIAL_16)
    # A local entry keeps the bare serial upstream gives it.
    assert local.unique_id == "xg-0000-local"
    assert entry_serial(old) == SERIAL_40
    assert entry_serial(local) == "xg-0000-local"

    async_namespace_cloud_entries(hass)  # idempotent
    assert old.unique_id == cloud_unique_id(SERIAL_40)
    assert not old.unique_id.startswith(CLOUD_UNIQUE_ID_PREFIX * 2)


async def test_only_an_enabled_local_entry_owns_the_panel(hass: HomeAssistant) -> None:
    cloud = _entry(SERIAL_40, "MAIN 40")
    cloud.add_to_hass(hass)
    async_namespace_cloud_entries(hass)
    assert local_owner(hass, SERIAL_40) is None

    disabled_local = _local_entry(SERIAL_40.lower(), disabled=True)
    disabled_local.add_to_hass(hass)
    assert local_owner(hass, SERIAL_40) is None

    local = _local_entry(SERIAL_16.lower())
    local.add_to_hass(hass)
    assert local_owner(hass, SERIAL_16) is local
    # The cloud entry is never the owner, and is found by serial in any case.
    assert cloud_entry_for(hass, SERIAL_40.lower()) is cloud
    assert cloud_entry_for(hass, SERIAL_16) is None


async def test_a_locally_owned_panels_cloud_entry_stands_by(hass: HomeAssistant) -> None:
    """The local MAIN 40 owns it; its cloud entry builds nothing; the MAIN 16 is untouched."""
    FakeStream.backends = {}
    cloud_40 = _entry(SERIAL_40, "MAIN 40")
    cloud_16 = _entry(SERIAL_16, "MAIN 16")
    cloud_16.add_to_hass(hass)

    with (
        patch.object(hub_module, "discover", return_value=[SITE]),
        patch.object(hub_module, "SiteStream", FakeStream),
        patch.object(hub_module, "write_tokens_if_missing"),
    ):
        # Loading the integration sets up every entry it has, so the local
        # stand-in (which would try to reach a real panel) joins afterwards and
        # is never set up. Local reports the serial in lower case; SPAN's cloud
        # may not.
        assert await hass.config_entries.async_setup(cloud_16.entry_id)
        await hass.async_block_till_done()
        _local_entry(SERIAL_40.lower()).add_to_hass(hass)
        cloud_40.add_to_hass(hass)
        assert await hass.config_entries.async_setup(cloud_40.entry_id)
        await hass.async_block_till_done()

        assert cloud_40.state is ConfigEntryState.LOADED
        assert cloud_40.entry_id in _cloud_standby(hass)
        registry = er.async_get(hass)
        assert not [e for e in registry.entities.values() if e.config_entry_id == cloud_40.entry_id]

        # The other panel's cloud connection runs as before.
        assert cloud_16.state is ConfigEntryState.LOADED
        assert cloud_16.entry_id not in _cloud_standby(hass)
        assert [e for e in registry.entities.values() if e.config_entry_id == cloud_16.entry_id]

        # Standing by unloads cleanly.
        assert await hass.config_entries.async_unload(cloud_40.entry_id)
        assert cloud_40.entry_id not in _cloud_standby(hass)
        assert await hass.config_entries.async_unload(cloud_16.entry_id)
        await hass.async_block_till_done()


async def test_the_cloud_entry_is_reloaded_only_when_its_role_changes(
    hass: HomeAssistant,
) -> None:
    cloud = _entry(SERIAL_40, "MAIN 40")
    cloud.add_to_hass(hass)
    async_namespace_cloud_entries(hass)
    local = _local_entry(SERIAL_40.lower())
    local.add_to_hass(hass)
    cloud.mock_state(hass, ConfigEntryState.LOADED)

    with patch.object(hass.config_entries, "async_schedule_reload") as reload:
        # Running, and the local connection just came: step back.
        _async_reload_cloud_connection(hass, local, standing_by=False)
        reload.assert_called_once_with(cloud.entry_id)
        reload.reset_mock()

        # Running, and the local connection just went: nothing to change.
        _async_reload_cloud_connection(hass, local, standing_by=True)
        reload.assert_not_called()

        # Standing by, and the local connection just went: look again.
        _cloud_standby(hass).add(cloud.entry_id)
        _async_reload_cloud_connection(hass, local, standing_by=True)
        reload.assert_called_once_with(cloud.entry_id)
        reload.reset_mock()

        # Standing by, and the local connection came (again): already right.
        _async_reload_cloud_connection(hass, local, standing_by=False)
        reload.assert_not_called()

        # A cloud entry that is not loaded is left to its own setup.
        cloud.mock_state(hass, ConfigEntryState.NOT_LOADED)
        _async_reload_cloud_connection(hass, local, standing_by=True)
        reload.assert_not_called()


async def test_a_cloud_panels_diagnostics_download_without_its_sign_in(hass: HomeAssistant) -> None:
    """A cloud panel has no device tree to export, and its account stays out of the file."""
    FakeStream.backends = {}
    entry = _entry(SERIAL_40, "MAIN 40")
    entry.add_to_hass(hass)

    with (
        patch.object(hub_module, "discover", return_value=[SITE]),
        patch.object(hub_module, "SiteStream", FakeStream),
        patch.object(hub_module, "write_tokens_if_missing"),
    ):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        report = await async_get_config_entry_diagnostics(hass, entry)

        assert report["device_tree"] is None
        assert report["device_tree_unavailable"] == "cloud_connection"
        stored = report["config_entry"]["data"]
        for key in (CONF_CLOUD_TOKENS, CONF_CLOUD_USER_ID, CONF_CLOUD_DEVICE_UUID):
            assert stored[key] == "**REDACTED**"
        # What tells one connection's circuits from another's: the breaker spaces.
        assert all("tabs" in circuit for circuit in report["circuits"].values())

        assert await hass.config_entries.async_unload(entry.entry_id)
        await hass.async_block_till_done()
