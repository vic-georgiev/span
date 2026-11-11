"""Circuit names from the linked SPAN account: pulled, kept with the link, shown over the panel's."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

from homeassistant.config_entries import ConfigSubentryData
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry
from span_panel_api import SpanMqttClient

from custom_components.span_panel.cloud.link import SUBENTRY_CLOUD_ACCOUNT, cloud_account
from custom_components.span_panel.cloud.names import (
    CONF_NAME_CHANGES,
    CONF_NAMES,
    CONF_NAMES_PULLED_AT,
    account_names,
    async_pull_names,
    names_diagnostics,
    record_rename,
    with_account_names,
)
from custom_components.span_panel.const import CONF_API_VERSION, DOMAIN
from custom_components.span_panel.coordinator import SpanPanelCoordinator

from .factories import SpanCircuitSnapshotFactory, SpanPanelSnapshotFactory
from .test_cloud_link import ACCOUNT, LOCAL_SERIAL, _cloud
from .test_cloud_transport import SERIAL_40, FakeStream


def _entry(hass: HomeAssistant, **account: object) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=LOCAL_SERIAL,
        data={CONF_API_VERSION: "v2"},
        subentries_data=[
            ConfigSubentryData(
                data={**ACCOUNT, **account},
                subentry_type=SUBENTRY_CLOUD_ACCOUNT,
                title="MAIN 40",
                unique_id=SERIAL_40,
            )
        ],
    )
    entry.add_to_hass(hass)
    return entry


def _local():
    """Build the panel as it names itself; the fake account calls space 9 "Kitchen"."""
    return SpanPanelSnapshotFactory.create(
        serial_number=LOCAL_SERIAL,
        circuits={
            "circuit-1": SpanCircuitSnapshotFactory.create(
                circuit_id="circuit-1", tabs=[9], name="Kitchen / Outlets"
            ),
            "circuit-8": SpanCircuitSnapshotFactory.create(
                circuit_id="circuit-8", tabs=[13, 11], name="A/C Condenser"
            ),
        },
    )


def test_the_accounts_name_is_shown_over_the_panels() -> None:
    named = with_account_names(_local(), {"9": "Kitchen", "11/13": "A/C Condenser"})
    assert named.circuits["circuit-1"].name == "Kitchen"
    assert named.circuits["circuit-8"].name == "A/C Condenser"
    # Nothing to rename: the snapshot is passed through untouched.
    local = _local()
    assert with_account_names(local, {}) is local
    assert with_account_names(local, {"11/13": "A/C Condenser"}) is local


async def test_a_pull_keeps_the_accounts_names_and_a_first_pull_is_no_change(
    hass: HomeAssistant,
) -> None:
    FakeStream.backends = {}
    entry = _entry(hass)
    discover, stream, tokens = _cloud()
    with discover, stream, tokens:
        await async_pull_names(hass, entry, _local())
    assert account_names(entry) == {"9": "Kitchen", "11/13": "A/C Condenser"}
    account = cloud_account(entry)
    assert account.data[CONF_NAMES_PULLED_AT]
    assert account.data[CONF_NAME_CHANGES] == []
    # Nothing is left running.
    assert FakeStream.backends[SERIAL_40].stopped


async def test_a_rename_made_in_the_span_app_is_seen_on_the_next_pull(
    hass: HomeAssistant,
) -> None:
    FakeStream.backends = {}
    entry = _entry(
        hass,
        **{
            CONF_NAMES: {"9": "Pantry", "11/13": "A/C Condenser"},
            CONF_NAMES_PULLED_AT: "2026-01-01T00:00:00+00:00",
        },
    )
    discover, stream, tokens = _cloud()
    with discover, stream, tokens:
        await async_pull_names(hass, entry, _local())
    assert account_names(entry)["9"] == "Kitchen"
    (change,) = cloud_account(entry).data[CONF_NAME_CHANGES]
    assert (change["spaces"], change["from"], change["to"], change["seen"]) == (
        "9",
        "Pantry",
        "Kitchen",
        "span_account",
    )
    diagnostics = names_diagnostics(entry, {"9": "Kitchen / Outlets", "11/13": "A/C Condenser"})
    assert diagnostics["differing"] == {"9": {"panel": "Kitchen / Outlets", "account": "Kitchen"}}
    assert diagnostics["changes"] == [change]


async def test_a_fresh_pull_is_not_repeated(hass: HomeAssistant) -> None:
    FakeStream.backends = {}
    entry = _entry(hass)
    discover, stream, tokens = _cloud()
    with discover, stream, tokens:
        await async_pull_names(hass, entry, _local())
        FakeStream.backends = {}
        # The change reloads the entry, whose setup asks again: nothing is opened.
        await async_pull_names(hass, entry, _local())
    assert FakeStream.backends == {}


def test_a_rename_from_home_assistant_is_kept_and_logged(hass: HomeAssistant) -> None:
    entry = _entry(hass, **{CONF_NAMES: {"9": "Kitchen"}})
    record_rename(hass, entry, [9], "Garage Fridge")
    assert account_names(entry) == {"9": "Garage Fridge"}
    (change,) = cloud_account(entry).data[CONF_NAME_CHANGES]
    assert (change["from"], change["to"], change["seen"]) == (
        "Kitchen",
        "Garage Fridge",
        "home_assistant",
    )


async def test_the_coordinator_shows_the_accounts_names_and_remembers_the_panels(
    hass: HomeAssistant,
) -> None:
    entry = _entry(hass, **{CONF_NAMES: {"9": "Garage Fridge"}})
    client = MagicMock(spec=SpanMqttClient)
    client.get_snapshot.return_value = _local()
    coordinator = SpanPanelCoordinator(hass, cast(SpanMqttClient, client), entry)
    snapshot = await coordinator._async_update_data()
    assert snapshot.circuits["circuit-1"].name == "Garage Fridge"
    assert coordinator.panel_names["9"] == "Kitchen / Outlets"
