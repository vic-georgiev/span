"""A local panel's SPAN account: used on demand, matched by breaker spaces, never left running.

The account side runs the real hub and cloud client over the fake backend
`test_cloud_transport` uses. The local side is a snapshot: the link reads the
panel's circuits and their spaces from it and nothing else.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigSubentryData
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.exceptions import HomeAssistantError
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.span_panel.cloud import hub as hub_module
from custom_components.span_panel.cloud.link import (
    CONF_RESERVED_BREAKERS,
    SUBENTRY_CLOUD_ACCOUNT,
    CloudPanelMismatch,
    LinkedCloudPanel,
    async_linked_cloud_panel,
    cloud_account,
)
from custom_components.span_panel.config_flow import SpanPanelConfigFlow
from custom_components.span_panel.const import (
    CONF_API_VERSION,
    CONF_CLOUD_DEVICE_UUID,
    CONF_CLOUD_MODEL,
    CONF_CLOUD_PANEL_KEY,
    CONF_CLOUD_TOKENS,
    CONF_CLOUD_USER_ID,
    DOMAIN,
)
from custom_components.span_panel.diagnostics import TO_REDACT
from custom_components.span_panel.websocket import _reserved_breakers

from .factories import SpanCircuitSnapshotFactory, SpanPanelSnapshotFactory
from .test_cloud_transport import (
    SERIAL_16,
    SERIAL_40,
    SITE,
    TOKENS,
    FakeBackend,
    FakeStream,
    _entry,
)

LOCAL_SERIAL = "xg-test-40"

ACCOUNT = {
    CONF_CLOUD_TOKENS: TOKENS,
    CONF_CLOUD_USER_ID: "user-1",
    CONF_CLOUD_DEVICE_UUID: "6F3D0D3E-6E33-4C50-9C6E-6A6E2B7A3B11",
    CONF_CLOUD_PANEL_KEY: SERIAL_40,
    CONF_CLOUD_MODEL: "MAIN 40",
}


def _local_entry(*, linked: bool = True) -> MockConfigEntry:
    """Build a local (SPAN API) panel entry, with or without a SPAN account."""
    subentries = (
        [
            ConfigSubentryData(
                data=ACCOUNT,
                subentry_type=SUBENTRY_CLOUD_ACCOUNT,
                title=f"MAIN 40 {SERIAL_40}",
                unique_id=SERIAL_40,
            )
        ]
        if linked
        else []
    )
    return MockConfigEntry(
        domain=DOMAIN,
        version=7,
        minor_version=4,
        unique_id=LOCAL_SERIAL,
        title="SPAN Panel",
        data={CONF_API_VERSION: "v2", "host": "192.0.2.40"},
        subentries_data=subentries,
    )


def _local_snapshot(spaces: dict[str, list[int]]):
    """Build the local panel's view: its own circuit ids, on the given breaker spaces."""
    return SpanPanelSnapshotFactory.create(
        serial_number=LOCAL_SERIAL,
        circuits={
            circuit_id: SpanCircuitSnapshotFactory.create(circuit_id=circuit_id, tabs=tabs)
            for circuit_id, tabs in spaces.items()
        },
    )


# The fake cloud MAIN 40 holds circuit 21 on space 9 and circuit 22 on 11/13.
# Locally the same breakers go by other ids, and a remote CT has no space at all.
SAME_PANEL = {"circuit-1": [9], "circuit-8": [13, 11], "remote-ct-3": []}


def _cloud():
    return (
        patch.object(hub_module, "discover", return_value=[SITE]),
        patch.object(hub_module, "SiteStream", FakeStream),
        patch.object(hub_module, "write_tokens_if_missing"),
    )


async def test_a_rename_reaches_the_cloud_circuit_on_the_same_spaces(hass: HomeAssistant) -> None:
    FakeStream.backends = {}
    entry = _local_entry()
    discover, stream, tokens = _cloud()
    with discover, stream, tokens:
        async with async_linked_cloud_panel(hass, entry, _local_snapshot(SAME_PANEL)) as panel:
            assert panel.cloud_circuit_id("circuit-1") == "21"
            assert panel.cloud_circuit_id("circuit-8") == "22"
            with pytest.raises(ValueError, match="remote-ct-3"):
                panel.cloud_circuit_id("remote-ct-3")
            assert await panel.rename_circuit("circuit-8", "  Heat   pump ") == "Heat pump"
            backend = FakeStream.backends[SERIAL_40]
            assert not backend.stopped

    assert backend.renames == [("circuit-22", "Heat pump")]
    # Nothing is left running: the stream went with the last user.
    assert backend.stopped
    assert not hass.data[DOMAIN][hub_module.HUBS_KEY]


async def test_an_account_panel_with_other_breakers_is_refused(hass: HomeAssistant) -> None:
    FakeStream.backends = {}
    entry = _local_entry()
    discover, stream, tokens = _cloud()
    with discover, stream, tokens, pytest.raises(CloudPanelMismatch, match="spaces 15, 20/22"):
        async with async_linked_cloud_panel(
            hass, entry, _local_snapshot({"circuit-1": [9], "a": [15], "b": [20, 22]})
        ):
            pytest.fail("a panel that is not this one must not be handed out")

    # Refused, and still nothing left running.
    assert FakeStream.backends[SERIAL_40].stopped
    assert not hass.data[DOMAIN][hub_module.HUBS_KEY]


async def test_a_panel_without_an_account_says_so(hass: HomeAssistant) -> None:
    entry = _local_entry(linked=False)
    assert cloud_account(entry) is None
    with pytest.raises(HomeAssistantError, match="no SPAN account"):
        async with async_linked_cloud_panel(hass, entry, _local_snapshot(SAME_PANEL)):
            pytest.fail("there is no account to open")


def _running(snapshot: object) -> object:
    """Patch the panel's runtime, so the link step reads `snapshot` as the panel now."""
    return patch(
        "custom_components.span_panel.cloud.config_flow.loaded_runtime_data",
        return_value=None if snapshot is None else MagicMock(coordinator=MagicMock(data=snapshot)),
    )


async def _sign_in(hass: HomeAssistant, entry: MockConfigEntry) -> dict:
    """Open "Add SPAN account" on the panel and sign in."""
    with patch(
        "custom_components.span_panel.cloud.config_flow._sign_in",
        return_value=(TOKENS, "user-1", [SITE]),
    ):
        result = await hass.config_entries.subentries.async_init(
            (entry.entry_id, SUBENTRY_CLOUD_ACCOUNT), context={"source": "user"}
        )
        assert result["step_id"] == "user"
        return await hass.config_entries.subentries.async_configure(
            result["flow_id"], {"email": "owner@example.com", "password": "not-stored"}
        )


async def test_signing_in_finds_and_links_the_panel(hass: HomeAssistant) -> None:
    FakeStream.backends = {}
    entry = _local_entry(linked=False)
    entry.add_to_hass(hass)

    # Locally only the breaker on space 9 is published; the account's MAIN 40
    # also has 11/13, as SPAN API leaves out a MAIN 40's battery breaker. The
    # account's MAIN 16 has no breaker on space 9, so it is not this panel.
    discover, stream, tokens = _cloud()
    with discover, stream, tokens, _running(_local_snapshot({"circuit-1": [9]})):
        result = await _sign_in(hass, entry)
        # Found by its breakers: nothing to choose.
        assert result["type"] is FlowResultType.CREATE_ENTRY
        # Read once while linking, and nothing left running afterwards.
        assert all(backend.stopped for backend in FakeStream.backends.values())

        account = cloud_account(entry)
        assert account is not None
        assert account.data[CONF_CLOUD_PANEL_KEY] == SERIAL_40
        assert account.data[CONF_RESERVED_BREAKERS] == [{"tabs": [11, 13], "name": "A/C Condenser"}]
        assert account.data[CONF_CLOUD_TOKENS] == TOKENS
        assert "not-stored" not in str(account.data)
        # The panel's own entry is untouched: still local, same identity.
        assert entry.unique_id == LOCAL_SERIAL
        assert entry.data[CONF_API_VERSION] == "v2"


async def test_a_panel_linked_to_another_entry_is_not_considered(hass: HomeAssistant) -> None:
    FakeStream.backends = {}
    _local_entry().add_to_hass(hass)  # already holds the account's MAIN 40
    other = MockConfigEntry(domain=DOMAIN, unique_id="xg-test-other", data={CONF_API_VERSION: "v2"})
    other.add_to_hass(hass)
    discover, stream, tokens = _cloud()
    # This panel's breakers would fit the MAIN 40, which is taken.
    with discover, stream, tokens, _running(_local_snapshot({"circuit-1": [9]})):
        result = await _sign_in(hass, other)
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "no_matching_panel"
    assert cloud_account(other) is None


async def test_no_panel_with_these_breakers_links_nothing(hass: HomeAssistant) -> None:
    FakeStream.backends = {}
    entry = _local_entry(linked=False)
    entry.add_to_hass(hass)
    discover, stream, tokens = _cloud()
    with discover, stream, tokens, _running(_local_snapshot({"a": [15], "b": [20, 22]})):
        result = await _sign_in(hass, entry)
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "no_matching_panel"
    assert cloud_account(entry) is None


async def test_a_panel_that_is_not_running_is_not_linked_blind(hass: HomeAssistant) -> None:
    entry = _local_entry(linked=False)
    entry.add_to_hass(hass)
    with _running(None):
        result = await _sign_in(hass, entry)
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "panel_not_loaded"
    assert cloud_account(entry) is None


async def test_two_panels_with_these_breakers_are_asked_about(hass: HomeAssistant) -> None:
    FakeStream.backends = {}
    entry = _local_entry(linked=False)
    entry.add_to_hass(hass)
    discover, stream, tokens = _cloud()
    # A panel with no breaker in a space fits any panel, so both are offered.
    with discover, stream, tokens, _running(_local_snapshot({"remote-ct": []})):
        result = await _sign_in(hass, entry)
        assert result["type"] is FlowResultType.FORM and result["step_id"] == "panel"
        result = await hass.config_entries.subentries.async_configure(
            result["flow_id"], {CONF_CLOUD_PANEL_KEY: SERIAL_16}
        )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert cloud_account(entry).data[CONF_CLOUD_PANEL_KEY] == SERIAL_16


def test_only_a_local_panel_is_offered_an_account() -> None:
    assert SUBENTRY_CLOUD_ACCOUNT in SpanPanelConfigFlow.async_get_supported_subentry_types(
        _local_entry(linked=False)
    )
    # One account per panel: a linked panel is not offered another.
    assert not SpanPanelConfigFlow.async_get_supported_subentry_types(_local_entry())
    # A panel read through the cloud already is its account.
    assert not SpanPanelConfigFlow.async_get_supported_subentry_types(_entry(SERIAL_40, "MAIN 40"))


def test_the_linked_sign_in_stays_out_of_diagnostics() -> None:
    report = async_redact_data(_local_entry().as_dict(), TO_REDACT)
    stored = report["subentries"][0]["data"]
    for key in (CONF_CLOUD_TOKENS, CONF_CLOUD_USER_ID, CONF_CLOUD_DEVICE_UUID):
        assert stored[key] == "**REDACTED**"
    assert stored[CONF_CLOUD_PANEL_KEY] == SERIAL_40


def _linked_with(breakers: list[dict]) -> MockConfigEntry:
    entry = _local_entry(linked=False)
    return MockConfigEntry(
        domain=DOMAIN,
        unique_id=LOCAL_SERIAL,
        subentries_data=[
            ConfigSubentryData(
                data={**ACCOUNT, CONF_RESERVED_BREAKERS: breakers},
                subentry_type=SUBENTRY_CLOUD_ACCOUNT,
                title="MAIN 40",
                unique_id=SERIAL_40,
            )
        ],
        data=dict(entry.data),
    )


BATTERY_BREAKER = {"tabs": [46, 48], "name": "Battery"}


def test_the_one_battery_takes_the_one_reserved_breaker() -> None:
    entry = _linked_with([BATTERY_BREAKER])
    sub_devices = {"dev-bess": {"type": "bess"}, "dev-mid": {"type": "mid"}}
    assert _reserved_breakers(entry, sub_devices) == [
        {"tabs": [46, 48], "name": "Battery", "battery_device_id": "dev-bess"}
    ]


def test_which_battery_is_on_which_breaker_is_not_guessed() -> None:
    entry = _linked_with([BATTERY_BREAKER, {"tabs": [42, 44], "name": "Spare"}])
    reserved = _reserved_breakers(entry, {"dev-bess": {"type": "bess"}})
    assert [breaker["battery_device_id"] for breaker in reserved] == [None, None]
    # Without a battery the breaker is still shown, by the name SPAN gives it.
    assert _reserved_breakers(_linked_with([BATTERY_BREAKER]), {}) == [
        {"tabs": [46, 48], "name": "Battery", "battery_device_id": None}
    ]


def test_no_account_no_reserved_breakers() -> None:
    assert _reserved_breakers(_local_entry(linked=False), {"dev-bess": {"type": "bess"}}) == []


async def test_the_cloud_panel_names_its_circuits_as_the_local_one_does(
    hass: HomeAssistant,
) -> None:
    FakeStream.backends = {}
    local = SpanPanelSnapshotFactory.create(
        serial_number=LOCAL_SERIAL,
        circuits={
            "circuit-1": SpanCircuitSnapshotFactory.create(
                circuit_id="circuit-1", tabs=[9], name="Kitchen"
            ),
            "circuit-8": SpanCircuitSnapshotFactory.create(
                circuit_id="circuit-8", tabs=[11, 13], name="Something else"
            ),
        },
    )
    discover, stream, tokens = _cloud()
    with discover, stream, tokens:
        async with async_linked_cloud_panel(hass, _local_entry(), local) as panel:
            assert panel.names_in_common() == 1


async def test_of_two_panels_with_one_layout_the_one_named_alike_is_linked(
    hass: HomeAssistant,
) -> None:
    FakeStream.backends = {}
    entry = _local_entry(linked=False)
    entry.add_to_hass(hass)
    discover, stream, tokens = _cloud()

    def names(panel: LinkedCloudPanel) -> int:
        return 3 if panel.client.serial == SERIAL_16 else 0

    # Both of the account's panels fit; only the MAIN 16 names circuits alike.
    with (
        discover,
        stream,
        tokens,
        _running(_local_snapshot({"remote-ct": []})),
        patch.object(LinkedCloudPanel, "names_in_common", names),
    ):
        result = await _sign_in(hass, entry)
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert cloud_account(entry).data[CONF_CLOUD_PANEL_KEY] == SERIAL_16


async def test_a_rename_waits_for_spans_circuits_after_the_telemetry(
    hass: HomeAssistant,
) -> None:
    """Opened on demand, the stream describes the panel before its circuits.

    The rename names a circuit, which the backend only knows once its subscribe
    snapshot lands, seconds after the first telemetry on a real account. A link
    that renamed on the telemetry alone found no circuit to rename.
    """
    FakeStream.backends = {}
    late = FakeBackend(SERIAL_40)
    late.circuits_late = True
    FakeStream.backends[SERIAL_40] = late
    discover, stream, tokens = _cloud()
    with (
        discover,
        stream,
        tokens,
        patch.object(FakeStream, "panel_backend", lambda _self, *_args, **_kwargs: late),
    ):
        hass.loop.call_later(0.2, late.circuits_ready.set)
        async with async_linked_cloud_panel(
            hass, _local_entry(), _local_snapshot(SAME_PANEL)
        ) as panel:
            assert late.circuits_ready.is_set()
            assert await panel.rename_circuit("circuit-1", "Pantry") == "Pantry"
    assert late.renames == [("circuit-21", "Pantry")]
