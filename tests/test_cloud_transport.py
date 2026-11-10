"""The SPAN cloud transport: a site's stream feeding the integration's own entities.

The vendored client's network half is replaced by a fake backend that speaks its
callbacks, so these run the real hub, client, snapshot mapping, coordinator and
platforms end to end.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
import time
from typing import Any
from unittest.mock import patch
import uuid

from homeassistant import config_entries
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import entity_registry as er, issue_registry as ir
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.span_panel.cloud import hub as hub_module
from custom_components.span_panel.cloud.clock import house_clock
from custom_components.span_panel.cloud.snapshot import (
    PanelIdentity,
    build_snapshot,
    field_metadata,
)
from custom_components.span_panel.cloud.span_client import (
    backend as backend_module,
    cloud_grpc,
)
from custom_components.span_panel.cloud.span_client.backend import (
    CloudBackend,
    PanelInfo,
    RenameNotApplied,
    SiteInfo,
)
from custom_components.span_panel.cloud.span_client.cloud_commands import (
    MAX_CIRCUIT_NAME_LENGTH,
    backup_priority_payload,
    build_load_management_request,
    build_rename_request,
    clean_circuit_name,
    powerup_rank_payload,
    shed_policy_payload,
)
from custom_components.span_panel.cloud.span_client.cloud_history import (
    Bucket,
    Resolution,
    plan_export_windows,
)
from custom_components.span_panel.cloud.span_client.cloud_pb import (
    field_bytes,
    field_message,
    field_string,
    field_varint,
    parse,
)
from custom_components.span_panel.cloud.span_client.cloud_traits import (
    parse_panel_traits,
    parse_trait_snapshot,
)
from custom_components.span_panel.cloud.span_client.models import (
    DataType,
    EnergySample,
    NodeKind,
    PanelSchema,
    PropertySpec,
    Reading,
)
from custom_components.span_panel.const import (
    API_VERSION_CLOUD,
    CONF_API_VERSION,
    CONF_CLOUD_DEVICE_UUID,
    CONF_CLOUD_MODEL,
    CONF_CLOUD_PANEL_KEY,
    CONF_CLOUD_TOKENS,
    CONF_CLOUD_USER_ID,
    DOMAIN,
)
from custom_components.span_panel.options import ALLOW_CONTEXTLESS_CONTROL

TOKENS = {
    "access_token": "a",
    "id_token": "i",
    "refresh_token": "r",
    "expires_at": 9_999_999_999.0,
    "token_type": "Bearer",
}
SERIAL_40 = "XC-TEST-40"
SERIAL_16 = "XC-TEST-16"
SITE = SiteInfo(
    site_id="site-1",
    hardware_ids=("gw-40", "gw-16"),
    panels=(
        PanelInfo("site-1", SERIAL_40, "1-02100-04", "gw-40"),
        PanelInfo("site-1", SERIAL_16, "1-04100-01", "gw-16"),
    ),
    gateways=("gw-40", "gw-16"),
)


def _spec(
    node: str, kind: NodeKind, prop: str, unit: str | None = None, **extra: Any
) -> PropertySpec:
    settable = prop == "relay"
    return PropertySpec(
        node_id=node,
        node_kind=kind,
        property_id=prop,
        name=prop,
        datatype=DataType.ENUM if settable else DataType.FLOAT,
        unit=unit,
        settable=settable,
        enum_values=("OPEN", "CLOSED", "UNKNOWN") if settable else (),
        **extra,
    )


def _panel_schema(
    serial: str, circuits: dict[int, tuple[str, tuple[int, ...]]], *, flows: bool
) -> PanelSchema:
    """Build what `schema_from_frame` makes of a panel: circuits, its own node, a feed."""
    schema = PanelSchema(serial=serial, software_version="spanos2/r202642/04")
    for instance, (label, spaces) in circuits.items():
        node = f"circuit-{instance}"
        details = {"node_name": label, "spaces": spaces, "breaker_amps": 20, "volts": 120}
        schema.add(_spec(node, NodeKind.CIRCUIT, "power", "W", **details))
        schema.add(_spec(node, NodeKind.CIRCUIT, "current", "A", **details))
        schema.add(_spec(node, NodeKind.CIRCUIT, "relay", node_name=label))
    for prop, unit in (("power", "W"), ("voltage_l1", "V"), ("voltage_l2", "V")):
        schema.add(_spec("panel", NodeKind.CORE, prop, unit))
    schema.add(_spec("feed-99", NodeKind.LUGS, "power", "W"))
    if flows:
        for flow in ("grid", "home"):
            schema.add(_spec("site", NodeKind.POWER_FLOWS, flow, "W", node_name="Site"))
    return schema


PANELS = {
    SERIAL_40: (
        _panel_schema(
            SERIAL_40,
            {21: ("Kitchen", (9,)), 22: ("A/C Condenser", (11, 13))},
            flows=True,
        ),
        {
            "circuit-21/power": "152.400",
            "circuit-21/current": "1.300",
            "circuit-21/relay": "CLOSED",
            "circuit-22/power": "2410.000",
            "circuit-22/current": "10.100",
            "circuit-22/relay": "CLOSED",
            "panel/power": "2600.000",
            "panel/voltage_l1": "121.200",
            "panel/voltage_l2": "120.800",
            "feed-99/power": "2611.000",
            "site/grid": "2611.000",
            "site/home": "2611.000",
        },
    ),
    SERIAL_16: (
        _panel_schema(SERIAL_16, {21: ("Garage", (1,))}, flows=False),
        {
            "circuit-21/power": "40.000",
            "circuit-21/current": "0.400",
            "circuit-21/relay": "OPEN",
            "panel/power": "40.000",
            "panel/voltage_l1": "121.100",
            "panel/voltage_l2": "120.700",
            "feed-99/power": "40.000",
        },
    ),
}


class FakeBackend:
    """Speaks the vendored backend's callbacks, from data instead of a stream."""

    def __init__(self, serial: str) -> None:
        """Start with no commands and no polls."""
        self.serial = serial
        self.commands: list[tuple[str, str]] = []
        self.renames: list[tuple[str, str]] = []
        self.settings: list[tuple[str, str, object]] = []
        self.history_calls: list[tuple[int, int, object, str]] = []
        self.on_reading: Callable[[Reading], None] | None = None
        self.started = False
        self.stopped = False
        self.energy_polls = 0

    def start(self, on_schema, on_reading) -> None:
        """Describe the panel, then report its readings."""
        self.started = True
        self.on_reading = on_reading
        schema, readings = PANELS[self.serial]
        on_schema(schema)
        for key, value in readings.items():
            on_reading(Reading(key=key, value=value, timestamp=0.0))

    def stop(self, join_timeout: float = 10.0) -> None:
        """Record that the stream was stopped."""
        self.stopped = True

    def send_command(self, key: str, value: str) -> None:
        """Record the command and echo it back as a reading, as SPAN does."""
        self.commands.append((key, value))
        assert self.on_reading is not None
        self.on_reading(Reading(key=key, value=value, timestamp=0.0))

    def set_backup_priority(self, node_id: str, priority: int) -> None:
        """Record the priority and report it back, as the panel's next snapshot would."""
        self.settings.append((node_id, "backup_priority", priority))
        self._report(f"{node_id}/backup_priority", str(priority))

    def set_always_on(self, node_id: str, always_on: bool) -> None:
        """Record always-on and report the shed policy it becomes."""
        self.settings.append((node_id, "always_on", always_on))
        self._report(f"{node_id}/shed_policy", "1" if always_on else "2")

    def set_powerup_rank(self, node_id: str, rank: int) -> None:
        """Record the rank and report it back."""
        self.settings.append((node_id, "powerup_rank", rank))
        self._report(f"{node_id}/powerup_rank", str(rank))

    def fetch_history(self, start_ms, end_ms, resolution, *, time_zone="UTC"):
        """Two hourly buckets for one circuit and the site, starting at the range start."""
        self.history_calls.append((start_ms, end_ms, resolution, time_zone))
        hour = 3_600_000
        return {
            "circuit-21": {
                start_ms + i * hour: Bucket(
                    start_ms + i * hour, start_ms + (i + 1) * hour, {"import": 0.25, "export": 0.0}
                )
                for i in range(2)
            },
            "site": {
                start_ms: Bucket(
                    start_ms, start_ms + hour, {"grid_import": 1.5, "home_import": 1.4}
                ),
            },
        }

    def circuit_labels(self):
        """Name and spaces of the one circuit the fake history carries."""
        return {"circuit-21": ("Garage", (9,))}

    def _report(self, key: str, value: str) -> None:
        # Stamped now: numeric readings only move forward in time.
        assert self.on_reading is not None
        self.on_reading(Reading(key=key, value=value, timestamp=time.time()))

    def rename_circuit(self, node_id: str, name: str) -> str:
        """Record the rename, cleaning the name as the real backend does."""
        cleaned = clean_circuit_name(name)
        self.renames.append((node_id, cleaned))
        return cleaned

    def fetch_energy(
        self, covered_through_ms: int = 0, *, time_zone: str = "UTC"
    ) -> list[EnergySample]:
        """Report a new 15-minute bucket of 0.25 kWh on every poll."""
        start = self.energy_polls * 900_000
        self.energy_polls += 1
        return [EnergySample("circuit-21/power", start, start + 900_000, 0.25)]


class FakeStream:
    """Stands in for `SiteStream`: hands each panel its own fake backend."""

    backends: dict[str, FakeBackend] = {}

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        """Take the real SiteStream's arguments and ignore them."""

    def panel_backend(self, key: str, serial: str | None, on_auth_failed=None) -> FakeBackend:
        """Hand out a fake backend per panel and keep it for the test."""
        backend = FakeBackend(serial or key)
        FakeStream.backends[key] = backend
        return backend


def _entry(serial: str, model: str) -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        version=7,
        minor_version=3,
        unique_id=serial,
        title=f"SPAN {model} {serial}",
        data={
            CONF_API_VERSION: API_VERSION_CLOUD,
            "device_name": f"SPAN {model} {serial}",
            CONF_CLOUD_TOKENS: TOKENS,
            CONF_CLOUD_USER_ID: "user-1",
            CONF_CLOUD_DEVICE_UUID: "6F3D0D3E-6E33-4C50-9C6E-6A6E2B7A3B11",
            CONF_CLOUD_PANEL_KEY: serial,
            CONF_CLOUD_MODEL: model,
        },
        options={
            "use_device_prefix": True,
            "use_circuit_numbers": False,
            # A test service call carries no user; the gate refuses those otherwise.
            ALLOW_CONTEXTLESS_CONTROL: True,
        },
    )


def test_snapshot_maps_a_main_40_onto_the_integrations_terms() -> None:
    schema, readings = PANELS[SERIAL_40]
    snapshot = build_snapshot(
        PanelIdentity(SERIAL_40, "MAIN 40"),
        schema,
        readings,
        lambda key: 1.5 if key == "circuit-21/power" else None,
    )

    assert snapshot.instant_grid_power_w == 2611.0  # the main feed, not the busbar
    assert snapshot.l1_voltage == 121.2
    # The panel's wire frame: negative while importing; the grid sensor flips it.
    assert snapshot.power_flow_grid == -2611.0
    kitchen = snapshot.circuits["21"]
    assert kitchen.name == "Kitchen"
    assert kitchen.instant_power_w == 152.4
    assert kitchen.consumed_energy_wh == 1500.0
    assert kitchen.produced_energy_wh is None  # not measured, so not invented
    assert kitchen.relay_state == "CLOSED"
    assert kitchen.is_user_controllable
    assert kitchen.breaker_rating_a == 20.0
    # SPAN's own numbers, as on the panel and in the app: a MAIN 40 runs 9..48,
    # so its card is 48 tall with the main-breaker module's rows empty.
    assert snapshot.panel_size == 48
    assert kitchen.tabs == [9]
    assert snapshot.circuits["22"].tabs == [11, 13]


def test_the_main_feed_wins_over_the_feed_through_lugs() -> None:
    """A MAIN 40 can report its lugs (41) beside its main feed (2) as non-circuits."""
    schema = _panel_schema(SERIAL_40, {21: ("Kitchen", (9,))}, flows=False)
    schema.add(_spec("feed-41", NodeKind.LUGS, "power", "W"))
    schema.add(_spec("feed-2", NodeKind.LUGS, "power", "W"))
    readings = {"feed-41/power": "355.0", "feed-2/power": "2611.0", "feed-99/power": "1.0"}
    snapshot = build_snapshot(
        PanelIdentity(SERIAL_40, "MAIN 40"), schema, readings, lambda _k: None
    )

    assert snapshot.instant_grid_power_w == 2611.0


def test_a_breaker_without_a_rating_gets_no_switch() -> None:
    """The battery's breaker on a MAIN 40 (46/48) has a relay but no rating."""
    schema = _panel_schema(SERIAL_40, {21: ("Kitchen", (9,))}, flows=False)
    schema.add(_spec("circuit-51", NodeKind.CIRCUIT, "power", "W", spaces=(46, 48)))
    schema.add(_spec("circuit-51", NodeKind.CIRCUIT, "relay"))
    snapshot = build_snapshot(PanelIdentity(SERIAL_40, "MAIN 40"), schema, {}, lambda _k: None)

    assert snapshot.circuits["21"].is_user_controllable
    assert not snapshot.circuits["51"].is_user_controllable


def test_space_numbers_are_never_renumbered() -> None:
    """Breakers keep SPAN's numbers whichever spaces are in use."""
    schema = _panel_schema(SERIAL_16, {21: ("A", (9,)), 22: ("B", (24,))}, flows=False)
    snapshot = build_snapshot(PanelIdentity(SERIAL_16, "MAIN 16"), schema, {}, lambda _key: None)

    # As the SPAN app shows a MAIN 16: 9..24.
    assert snapshot.panel_size == 24
    assert snapshot.circuits["21"].tabs == [9]
    assert snapshot.circuits["22"].tabs == [24]


def test_the_largest_panel_takes_the_sites_flows() -> None:
    """A MAIN 16 fed through a MAIN 40 is downstream, whichever the cloud lists first."""
    site = SiteInfo(
        site_id="site-1",
        hardware_ids=("gw-16", "gw-40"),
        panels=tuple(reversed(SITE.panels)),
        gateways=("gw-16", "gw-40"),
    )
    assert site.lead.serial == SERIAL_16
    assert hub_module.upstream_panel(site).serial == SERIAL_40


def test_fields_the_cloud_does_not_carry_are_declared_unresolved() -> None:
    schema, readings = PANELS[SERIAL_40]
    snapshot = build_snapshot(
        PanelIdentity(SERIAL_40, "MAIN 40"), schema, readings, lambda _k: None
    )
    meta = field_metadata(snapshot)

    assert meta["panel.door_state"].resolved is False
    assert meta["panel.wlan_link"].resolved is True
    assert meta["circuit.always_on"].resolved is True
    assert meta["circuit.is_never_backup"].resolved is False
    assert meta["circuit.priority"].resolved is True
    assert meta["pcs.active"].resolved is False
    assert meta["circuit.instant_power_w"].resolved is True
    assert meta["panel.power_flow_grid"].resolved is True
    assert "panel.power_flow_battery" not in meta  # no battery: nothing to say


def _trait_ref(trait: int, instance: int) -> bytes:
    return field_message(1, field_varint(1, 1) + field_varint(3, trait)) + field_message(
        2, field_varint(1, instance)
    )


def _trait(trait: int, instance: int, body: bytes) -> bytes:
    """One snapshot entry, its value wrapped the way the cloud wraps it."""
    return field_message(
        2,
        field_message(1, field_varint(1, 1) + field_varint(2, 4) + field_varint(3, trait))
        + field_message(2, field_varint(1, instance))
        + field_message(3, field_bytes(2, field_message(1, body))),
    )


def _load_management(instance: int, config: bytes) -> bytes:
    return _trait(
        31,
        instance,
        field_message(1, _trait_ref(15, instance)) + field_message(2, config) + field_varint(3, 3),
    )


def _main_40_traits() -> bytes:
    """Build the MAIN 40's load-management and feeder traits, field for field as SPAN sends them."""
    entries = [
        _trait(15, 22, field_varint(2, 1)),  # Disposal
        _trait(15, 23, field_varint(2, 1)),  # Irrigation
        _trait(15, 30, field_varint(2, 1)),  # Hot tub
        # backup priority 3, PowerUp rank 47
        _load_management(
            22, field_varint(1, 2) + field_message(2, field_varint(1, 3)) + field_varint(22, 47)
        ),
        # no backup block at all, and set to always on
        _load_management(23, field_varint(1, 1) + field_varint(22, 42)),
        _load_management(
            30, field_varint(1, 2) + field_message(2, field_varint(1, 2)) + field_varint(22, 16)
        ),
        # the main breaker, then the feed-through lugs
        _trait(21, 2, field_varint(2, 3) + field_varint(4, 200) + field_varint(5, 200)),
        _trait(21, 41, field_varint(2, 4) + field_varint(7, 5)),
        # PowerUp: the service limit, and the Powerwall's 11.5 kW at 240 V
        _trait(32, 1, field_message(2, field_varint(1, 200_000)) + field_varint(5, 1)),
        _trait(32, 4, field_message(2, field_varint(1, 47_917)) + field_varint(5, 4)),
        # gateway interfaces: Ethernet and cellular connected, Wi-Fi not
        _trait(5, 1, field_message(1, field_varint(3, 2) + field_message(5, field_varint(1, 7)))),
        _trait(6, 1, field_message(1, field_varint(3, 3) + field_message(5, field_varint(1, 3)))),
        _trait(4, 1, field_message(1, field_varint(3, 9) + field_message(5, field_varint(1, 7)))),
    ]
    return field_message(1, field_message(1, field_string(1, "gw-40")) + b"".join(entries))


def test_the_trait_snapshot_yields_priorities_ranks_and_limits() -> None:
    raw = _main_40_traits()

    circuits = parse_trait_snapshot(raw)
    assert (circuits[22].backup_priority, circuits[22].powerup_rank) == (3, 47)
    assert (circuits[23].backup_priority, circuits[23].powerup_rank) == (None, 42)
    assert (circuits[22].shed_policy, circuits[23].shed_policy) == (2, 1)
    assert (circuits[30].backup_priority, circuits[30].powerup_rank) == (2, 16)

    panel = parse_panel_traits(raw)
    assert panel.main_breaker_amps == 200  # not the lugs
    assert panel.service_limit_a == 200.0
    assert panel.battery_limit_a == 47.917
    assert (panel.ethernet_up, panel.wifi_up, panel.cellular_up) == (True, False, True)


def test_the_battery_breaker_has_its_spaces_and_a_name() -> None:
    """The battery's breaker keeps its position in field 12, with no label."""
    spaces = [
        _trait(17, 38, field_varint(3, 46)),
        _trait(17, 40, field_varint(3, 48)),
    ]
    position = field_message(1, _trait_ref(17, 38)) + field_message(2, _trait_ref(17, 40))
    battery = _trait(15, 51, field_varint(1, 1) + field_varint(2, 3) + field_message(12, position))
    raw = field_message(
        1, field_message(1, field_string(1, "gw-40")) + b"".join([*spaces, battery])
    )

    info = parse_trait_snapshot(raw)[51]
    assert info.spaces == (46, 48)
    assert info.display_name == "Battery"
    assert info.breaker_amps is None  # SPAN gives it no rating, so it gets no switch


def test_priorities_and_powerup_reach_the_snapshot() -> None:
    schema, readings = PANELS[SERIAL_40]
    readings = {
        **readings,
        "circuit-21/backup_priority": "4",
        "circuit-21/powerup_rank": "37",
        "circuit-22/backup_priority": "3",
        "circuit-21/shed_policy": "1",
        "circuit-22/shed_policy": "2",
        "panel/main_breaker_amps": "200",
        "panel/service_limit_a": "200.0",
        "panel/battery_limit_a": "47.917",
        "panel/ethernet_link": "1",
        "panel/wifi_link": "0",
    }
    snapshot = build_snapshot(
        PanelIdentity(SERIAL_40, "MAIN 40"), schema, readings, lambda _k: None
    )

    kitchen, condenser = snapshot.circuits["21"], snapshot.circuits["22"]
    assert (kitchen.priority, kitchen.is_sheddable) == ("NEVER", False)
    assert (condenser.priority, condenser.is_sheddable) == ("SOC_THRESHOLD", True)
    assert (kitchen.pcs_managed, kitchen.pcs_priority) == (True, 37)
    assert (kitchen.always_on, condenser.always_on) == (True, False)
    assert not kitchen.is_user_controllable  # always on: no switch
    assert condenser.is_user_controllable
    # SPAN gave the condenser no rank here, which is not the same as unmanaged.
    assert (condenser.pcs_managed, condenser.pcs_priority) == (None, None)
    assert snapshot.main_breaker_rating_a == 200
    assert snapshot.pcs is not None
    assert snapshot.pcs.import_limit_a == 200.0
    assert snapshot.pcs.feed_import_limit_a == 200.0
    assert snapshot.pcs.off_grid_import_limit_a == 47.917
    assert snapshot.pcs.active is None
    assert (snapshot.eth0_link, snapshot.wlan_link, snapshot.wwan_link) == (True, False, False)


def test_a_panel_without_powerup_limits_has_no_pcs() -> None:
    schema, readings = PANELS[SERIAL_40]
    snapshot = build_snapshot(
        PanelIdentity(SERIAL_40, "MAIN 40"), schema, readings, lambda _k: None
    )
    assert snapshot.pcs is None
    assert snapshot.circuits["21"].priority == "UNKNOWN"


async def test_two_panels_on_one_site_share_one_stream(hass: HomeAssistant) -> None:
    FakeStream.backends = {}
    entry_40 = _entry(SERIAL_40, "MAIN 40")
    entry_16 = _entry(SERIAL_16, "MAIN 16")
    entry_40.add_to_hass(hass)
    entry_16.add_to_hass(hass)

    with (
        patch.object(hub_module, "discover", return_value=[SITE]) as discover,
        patch.object(hub_module, "SiteStream", FakeStream),
        patch.object(hub_module, "write_tokens_if_missing"),
    ):
        assert await hass.config_entries.async_setup(entry_40.entry_id)
        await hass.async_block_till_done()

        assert entry_40.state is ConfigEntryState.LOADED
        assert entry_16.state is ConfigEntryState.LOADED
        # One sign-in, one site, one stream for both panels.
        assert discover.call_count == 1
        assert set(FakeStream.backends) == {SERIAL_40, SERIAL_16}

        registry = er.async_get(hass)
        cloud_40 = [e for e in registry.entities.values() if e.config_entry_id == entry_40.entry_id]
        cloud_16 = [e for e in registry.entities.values() if e.config_entry_id == entry_16.entry_id]
        assert cloud_40 and cloud_16

        kitchen_power = next(
            e.entity_id
            for e in cloud_40
            if e.domain == "sensor" and "kitchen" in e.entity_id and e.entity_id.endswith("_power")
        )
        assert float(hass.states.get(kitchen_power).state) == 152.4

        grid = next(e.entity_id for e in cloud_40 if e.entity_id.endswith("_grid_power"))
        assert float(hass.states.get(grid).state) == 2611.0  # positive while importing

        # Energy comes from SPAN's metered history, not from integrating power. The
        # first bucket a circuit shows only sets its mark; the next one counts.
        energy = kitchen_power.removesuffix("_power") + "_consumed_energy"
        start = dt_util.utcnow()
        seen: list[str] = []
        # Step the clock through several poll intervals rather than aiming at one
        # timer: a reload on the first push (entity names syncing) restarts them.
        for step in range(1, 30):
            async_fire_time_changed(hass, start + timedelta(seconds=step * 10))
            await hass.async_block_till_done()
            state = hass.states.get(energy).state
            if not seen or seen[-1] != state:
                seen.append(state)
            if state not in ("unknown", "0.0"):
                break
        # Unknown until the first bucket, which only sets the mark (0), then
        # whole 0.25 kWh buckets -- however many polls a busy clock step ran.
        counted = [s for s in seen if s != "unknown"]
        assert counted[0] == "0.0", seen
        assert len(counted) == 2 and float(counted[1]) % 250 == 0 and float(counted[1]) > 0, seen

        garage_switch = next(
            e.entity_id for e in cloud_16 if e.domain == "switch" and "garage" in e.entity_id
        )
        assert hass.states.get(garage_switch).state == "off"

        await hass.services.async_call(
            "switch", "turn_on", {"entity_id": garage_switch}, blocking=True
        )
        await hass.async_block_till_done()
        assert FakeStream.backends[SERIAL_16].commands == [("circuit-21/relay", "CLOSED")]
        # Only the MAIN 16 was told anything: the circuit ids overlap across panels.
        assert FakeStream.backends[SERIAL_40].commands == []

        # Known cloud gaps are one notice per panel, not a Repair per field
        # claiming the firmware dropped it.
        issues = [
            issue
            for (domain, _id), issue in ir.async_get(hass).issues.items()
            if domain == DOMAIN
            and issue.translation_key in ("cloud_fields_unavailable", "schema_field_unresolved")
        ]
        assert sorted(i.translation_key for i in issues) == ["cloud_fields_unavailable"] * 2
        assert "circuit.relay_requester" in issues[0].translation_placeholders["fields"]
        assert "circuit.priority" not in issues[0].translation_placeholders["fields"]

        assert await hass.config_entries.async_unload(entry_40.entry_id)
        assert not FakeStream.backends[SERIAL_40].stopped  # the MAIN 16 still reads it
        assert await hass.config_entries.async_unload(entry_16.entry_id)
        await hass.async_block_till_done()
        assert all(b.stopped for b in FakeStream.backends.values())


async def test_sign_in_adds_an_entry_per_panel_picked(hass: HomeAssistant) -> None:
    with (
        patch(
            "custom_components.span_panel.cloud.config_flow._sign_in",
            return_value=(TOKENS, "user-1", [SITE]),
        ),
        patch("custom_components.span_panel.async_setup_entry", return_value=True),
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN, context={"source": config_entries.SOURCE_USER}
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"use_span_cloud": True}
        )
        assert result["step_id"] == "cloud_login"
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"email": "owner@example.com", "password": "not-stored"}
        )
        assert result["step_id"] == "cloud_panels"
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"panels": [SERIAL_40, SERIAL_16]}
        )
        assert result["type"] is FlowResultType.CREATE_ENTRY
        await hass.async_block_till_done()

    entries = hass.config_entries.async_entries(DOMAIN)
    assert {e.unique_id for e in entries} == {SERIAL_40, SERIAL_16}
    for entry in entries:
        assert entry.data[CONF_API_VERSION] == API_VERSION_CLOUD
        assert "password" not in entry.data
        assert "not-stored" not in str(entry.data)
    assert len({e.data[CONF_CLOUD_DEVICE_UUID] for e in entries}) == 1


async def test_the_real_site_stream_split_reaches_each_panels_snapshot(
    hass: HomeAssistant, monkeypatch, tmp_path
) -> None:
    """Run the vendored SiteStream's split and schema, so the mapping is tested on its output."""
    from custom_components.span_panel.cloud.client import SpanCloudClient  # noqa: PLC0415
    from custom_components.span_panel.cloud.span_client import (  # noqa: PLC0415
        backend as cloud,
    )
    from custom_components.span_panel.cloud.span_client.cloud_commands import (  # noqa: PLC0415
        SwitchTarget,
    )
    from custom_components.span_panel.cloud.span_client.cloud_telemetry import (  # noqa: PLC0415
        Channel,
        CircuitSample,
        Frame,
    )
    from custom_components.span_panel.cloud.span_client.cloud_traits import (  # noqa: PLC0415
        CircuitInfo,
    )

    # The network loop never runs; frames are handed to the stream directly.
    monkeypatch.setattr(cloud.CloudBackend, "_run", lambda self: None)
    site = SiteInfo(
        site_id="site-1",
        hardware_ids=("site-1", "gw-40", "gw-16"),
        panels=SITE.panels,
        gateways=("gw-40", "gw-16"),
    )
    hub = hub_module.CloudSiteHub(
        hass, site, tokens=tmp_path / "t.json", device_uuid=str(uuid.uuid4()), user_id="u"
    )
    await hub.async_start()
    feed_40 = hub.feed_for(SERIAL_40)
    feed_16 = hub.feed_for(SERIAL_16)
    assert feed_40 is not None and feed_16 is not None
    switch = SwitchTarget.__new__(SwitchTarget)
    feed_40.backend._circuits = {
        21: CircuitInfo(21, "Kitchen", (9,), 20, True, switch),
    }
    feed_16.backend._circuits = {
        21: CircuitInfo(21, "Garage", (1,), 15, False, switch),
    }

    def sample(instance: int, watts: float, kind: str = "two_wire") -> CircuitSample:
        return CircuitSample(
            instance_id=instance,
            kind=kind,
            quality_pct=100,
            combined=Channel(power_mw=int(watts * 1000), current_ma=1200),
        )

    frame = Frame(
        epoch_millis=1000,
        site_id="site-1",
        site_instance_id=7,
        # Instance 21 on both panels: the split is what keeps them apart. 56 is
        # not on the MAIN 40's circuit list, so it is its main feed.
        resources={
            "gw-40": [
                sample(21, 152.4),
                sample(56, 2611.0),
                CircuitSample(
                    instance_id=1,
                    kind="panel",
                    combined=Channel(power_mw=2_611_000, current_ma=11_000),
                    feedthrough=Channel(power_mw=355_000, current_ma=1_500),
                ),
            ],
            "gw-16": [sample(21, 40.0)],
        },
        site_flows={"grid": 2611.0, "home": 2611.0},
    )
    monkeypatch.setattr(cloud, "decode_frame", lambda raw: frame)
    hub_stream = feed_40.backend._stream
    hub_stream._handle_frame(b"frame")
    await hass.async_block_till_done()

    client_40 = SpanCloudClient(hass, hub, feed_40, snapshot_interval=0)
    client_16 = SpanCloudClient(hass, hub, feed_16, snapshot_interval=0)
    snap_40 = await client_40.get_snapshot()
    snap_16 = await client_16.get_snapshot()

    assert snap_40.circuits["21"].name == "Kitchen"
    assert snap_40.circuits["21"].instant_power_w == 152.4
    assert snap_40.circuits["21"].relay_state == "CLOSED"
    assert snap_40.instant_grid_power_w == 2611.0
    assert snap_40.power_flow_grid == -2611.0
    # The MAIN 40's feed-through lugs carry what the MAIN 16 draws.
    assert snap_40.feedthrough_power_w == 355.0
    assert client_40.field_metadata["panel.feedthrough_power_w"].resolved
    assert snap_16.circuits["21"].name == "Garage"
    assert snap_16.circuits["21"].instant_power_w == 40.0
    assert snap_16.circuits["21"].relay_state == "OPEN"
    assert snap_16.power_flow_grid is None  # the site's flows went to the lead panel
    await hub.async_stop()


async def test_the_house_puts_the_16_under_the_40s_lugs(
    hass: HomeAssistant, hass_ws_client, monkeypatch
) -> None:
    """A fed panel is checked against its upstream, and the house lists it once, beneath."""
    schema_40, readings_40 = PANELS[SERIAL_40]
    monkeypatch.setitem(
        PANELS, SERIAL_40, (schema_40, {**readings_40, "panel/feedthrough_power": "42.000"})
    )
    FakeStream.backends = {}
    entry_40 = _entry(SERIAL_40, "MAIN 40")
    entry_16 = _entry(SERIAL_16, "MAIN 16")
    entry_16.add_to_hass(hass)
    entry_40.add_to_hass(hass)
    hass.config_entries.async_update_entry(
        entry_16, options={**entry_16.options, "feed": {"type": "lugs", "upstream": SERIAL_40}}
    )

    with (
        patch.object(hub_module, "discover", return_value=[SITE]),
        patch.object(hub_module, "SiteStream", FakeStream),
        patch.object(hub_module, "write_tokens_if_missing"),
    ):
        assert await hass.config_entries.async_setup(entry_40.entry_id)
        await hass.async_block_till_done()

        registry = er.async_get(hass)
        check = registry.async_get_entity_id("sensor", DOMAIN, f"{SERIAL_16}_feed_check")
        assert check is not None
        assert registry.async_get_entity_id("sensor", DOMAIN, f"{SERIAL_40}_feed_check") is None
        # A few more frames, each stamped with SPAN's own time: the lugs at 1.0 s
        # and 2.0 s, the MAIN 16 at 1.2 s and 1.6 s. The check pairs them by that
        # time, not by when each happened to arrive.
        for serial, key, value, at in (
            (SERIAL_40, "panel/feedthrough_power", "42.000", 1.0),
            (SERIAL_16, "feed-99/power", "40.000", 1.2),
            (SERIAL_40, "panel/feedthrough_power", "42.000", 2.0),
            (SERIAL_16, "feed-99/power", "40.000", 1.6),
        ):
            FakeStream.backends[serial].on_reading(Reading(key=key, value=value, timestamp=at))
            await hass.async_block_till_done()
        # The next push from the MAIN 40 reaches the check on the MAIN 16, which
        # follows its upstream as well as its own panel.
        entry_40.runtime_data.coordinator.async_update_listeners()
        await hass.async_block_till_done()
        state = hass.states.get(check)
        assert float(state.state) == 2.0  # the lugs say 42 W, the MAIN 16 measures 40 W
        assert state.attributes["upstream_w"] == 42.0
        assert state.attributes["this_panel_w"] == 40.0
        assert state.attributes["aligned"] is True  # matched by SPAN's frame time

        client = await hass_ws_client(hass)
        await client.send_json({"id": 1, "type": "span_panel/house"})
        reply = await client.receive_json()
        assert reply["success"], reply
        panels = reply["result"]["panels"]
        assert [(p["serial"], p["depth"]) for p in panels] == [(SERIAL_40, 0), (SERIAL_16, 1)]
        assert panels[0]["children"] == [SERIAL_16]
        assert panels[1]["feed"] == {"type": "lugs", "upstream": SERIAL_40}
        assert panels[1]["feed_check"] == check
        assert all(p["device_id"] for p in panels)
        feedthrough = registry.async_get(panels[0]["feedthrough_power"])
        assert feedthrough is not None and feedthrough.disabled_by is None  # on over the cloud
        await client.send_json(
            {"id": 2, "type": "span_panel/panel_topology", "device_id": panels[0]["device_id"]}
        )
        topology = await client.receive_json()
        assert topology["result"]["first_space"] == 9  # below 9 is the main-breaker module
        assert topology["result"]["can_rename"] is True  # the cloud writes names
        assert topology["result"]["can_export_history"] is True

        await client.send_json(
            {
                "id": 3,
                "type": "span_panel/rename_circuit",
                "device_id": panels[0]["device_id"],
                "circuit_id": "21",
                "name": "  Kitchen   outlets ",
            }
        )
        renamed = await client.receive_json()
        assert renamed["success"], renamed
        assert renamed["result"] == {"name": "Kitchen outlets"}
        assert FakeStream.backends[SERIAL_40].renames == [("circuit-21", "Kitchen outlets")]

        await client.send_json(
            {
                "id": 4,
                "type": "span_panel/rename_circuit",
                "device_id": panels[0]["device_id"],
                "circuit_id": "21",
                "name": "   ",
            }
        )
        refused = await client.receive_json()
        assert not refused["success"]
        assert refused["error"]["code"] == "invalid"
        # Only the 40's main feed: the 16 is already inside it.
        assert reply["result"]["house_power_entities"] == [panels[0]["main_feed_power"]]


async def test_both_panels_are_shown_at_one_instant(hass: HomeAssistant, monkeypatch) -> None:
    """The MAIN 40 is a frame ahead: its lugs are shown as they stood when the 16 last spoke."""
    schema_40, readings_40 = PANELS[SERIAL_40]
    monkeypatch.setitem(
        PANELS, SERIAL_40, (schema_40, {**readings_40, "panel/feedthrough_power": "42.000"})
    )
    FakeStream.backends = {}
    entry_40 = _entry(SERIAL_40, "MAIN 40")
    entry_16 = _entry(SERIAL_16, "MAIN 16")
    entry_16.add_to_hass(hass)
    entry_40.add_to_hass(hass)

    with (
        patch.object(hub_module, "discover", return_value=[SITE]),
        patch.object(hub_module, "SiteStream", FakeStream),
        patch.object(hub_module, "write_tokens_if_missing"),
    ):
        assert await hass.config_entries.async_setup(entry_40.entry_id)
        await hass.async_block_till_done()

        # A 260 W load comes on downstream at about 1.8 s. The MAIN 40 has framed
        # it already (at 2.0 s); the MAIN 16's newest frame is from 1.6 s.
        for serial, key, value, at in (
            (SERIAL_40, "panel/feedthrough_power", "42.000", 1.0),
            (SERIAL_16, "feed-99/power", "40.000", 1.2),
            (SERIAL_16, "feed-99/power", "40.000", 1.6),
            (SERIAL_40, "panel/feedthrough_power", "302.000", 2.0),
        ):
            FakeStream.backends[serial].on_reading(Reading(key=key, value=value, timestamp=at))
            await hass.async_block_till_done()

        clock = house_clock(hass)
        assert clock.reading().at == 1.6  # the instant both panels have reached
        await clock._tick()  # noqa: SLF001 - the tick the stream's pace would fire
        await hass.async_block_till_done()

        snap_40 = entry_40.runtime_data.coordinator.data
        snap_16 = entry_16.runtime_data.coordinator.data
        assert clock.now == 1.6
        # Shown at 1.6 s on both: the lugs' 42 W, not the 302 W the 16 has not seen yet.
        assert snap_40.feedthrough_power_w == 42.0
        assert snap_16.instant_grid_power_w == 40.0

        # The MAIN 16 catches up; the clock moves on, and both show the load.
        FakeStream.backends[SERIAL_16].on_reading(
            Reading(key="feed-99/power", value="300.000", timestamp=2.2)
        )
        await hass.async_block_till_done()
        await clock._tick()  # noqa: SLF001
        await hass.async_block_till_done()
        assert clock.now == 2.0
        assert entry_40.runtime_data.coordinator.data.feedthrough_power_w == 302.0
        assert entry_16.runtime_data.coordinator.data.instant_grid_power_w == 40.0  # 1.6 s, at 2.0


def test_cloud_energy_is_whole_wh() -> None:
    """SPAN's kWh becomes whole Wh, with no float residue such as 774.4127999999."""
    from custom_components.span_panel.cloud.snapshot import _wh

    assert _wh(0.7744128) == 774.0
    assert _wh(0.7746) == 775.0
    assert _wh(None) is None


def _labelled_circuit_traits(name: str = "Kitchen", spaces: tuple[int, ...] = (9,)) -> bytes:
    """Build a circuit (1/15 #21) whose position points at its label (1/16 #70) and spaces."""
    refs = field_message(1, _trait_ref(16, 70)) + b"".join(
        field_message(2, _trait_ref(17, 80 + i)) for i in range(len(spaces))
    )
    field = 11 if len(spaces) == 1 else 13
    circuit = _trait(15, 21, field_varint(2, 1) + field_message(field, refs))
    label = _trait(16, 70, field_varint(3, 20) + field_string(4, name))
    space_entries = b"".join(
        _trait(17, 80 + i, field_varint(3, space)) for i, space in enumerate(spaces)
    )
    return field_message(
        1, field_message(1, field_string(1, "gw-40")) + circuit + label + space_entries
    )


def test_a_circuit_is_renamed_through_its_breaker() -> None:
    """The breaker's own instance (1/15) is what a rename addresses."""
    info = parse_trait_snapshot(_labelled_circuit_traits())[21]
    assert info.label == "Kitchen"
    assert info.spaces == (9,)
    assert info.rename_target is not None
    assert info.rename_target.resource_id == "gw-40"
    assert info.rename_target.instance_id == 21
    assert info.rename_target.metadata == (1, 4, 15, None)


def _rename_edit(body: bytes) -> tuple[Any, Any]:
    """Unwrap a rename request to its trait message and its breaker update."""
    message = parse(body).get_msg(1)
    payload = parse(message.get_msg(14).get_msg(2).get_bytes(1))
    return message, payload.get_msg(3)


def test_a_single_pole_rename_carries_only_the_name() -> None:
    """`homeowner_update { one_p_standard { circuit { name } } }`: nothing else to reset."""
    target = parse_trait_snapshot(_labelled_circuit_traits())[21].rename_target
    body = build_rename_request(target, " Hot  tub ", 1, requester_id="user-1", request_id="r")

    message, update = _rename_edit(body)
    metadata = message.get_msg(1)
    assert (metadata.get_int_opt(1), metadata.get_int_opt(3)) == (1, 15)
    assert message.get_msg(2).get_msg(1).get_str(1) == "gw-40"
    assert message.get_msg(2).get_msg(2).get_int_opt(1) == 21
    assert message.get_msg(14).get_msg(1).get_msg(2).get_str(1) == "user-1"
    assert update.get_msg(10).get_msg(1).get_str(1) == "Hot tub"
    assert update.get_msg(11) is None


def test_a_two_pole_rename_names_the_standard_circuit() -> None:
    """A 240 V breaker's one circuit sits under `two_p_standard { standard }`."""
    info = parse_trait_snapshot(_labelled_circuit_traits(spaces=(30, 32)))[21]
    assert info.spaces == (30, 32)
    body = build_rename_request(info.rename_target, "Dryer", 2, requester_id="user-1")

    _message, update = _rename_edit(body)
    assert update.get_msg(11).get_msg(3).get_msg(1).get_str(1) == "Dryer"
    assert update.get_msg(10) is None
    try:
        build_rename_request(info.rename_target, "Quad", 3, requester_id="user-1")
    except ValueError:
        pass
    else:
        raise AssertionError("a three-space breaker was renamed")


def test_names_are_cleaned_and_checked_before_they_leave() -> None:
    """Trimmed and single-spaced; an empty or overlong name never reaches the panel."""
    assert clean_circuit_name("  Pool   pump ") == "Pool pump"
    for bad in ("", "   ", "x" * (MAX_CIRCUIT_NAME_LENGTH + 1)):
        try:
            clean_circuit_name(bad)
        except ValueError:
            continue
        raise AssertionError(f"{bad!r} was accepted")


class _RecordingGrpc:
    """Stands in for CloudGrpcClient, recording what would have been posted."""

    sent: list[bytes] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Take the client's arguments and ignore them."""

    def __enter__(self) -> _RecordingGrpc:
        """Open nothing."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Close nothing."""

    last_name: str | None = None

    def send_messages(self, request: bytes) -> bytes:
        """Record the request, remember the name it asks for, and ack it."""
        self.sent.append(request)
        _message, update = _rename_edit(request)
        _RecordingGrpc.last_name = update.get_msg(10).get_msg(1).get_str(1)
        return b""


def _renaming_backend(tmp_path, monkeypatch, *, panel_applies: bool) -> CloudBackend:
    """Build a backend whose snapshot re-read reports the new name only if the panel took it."""
    backend = CloudBackend(tmp_path / "tokens.json", "device-uuid", user_id="user-1")
    backend._adopt_circuits(parse_trait_snapshot(_labelled_circuit_traits()))
    backend._schema_sent = True
    monkeypatch.setattr(backend, "_access_token", lambda: "token")
    monkeypatch.setattr(backend, "_requester_id", lambda token: "user-1")
    monkeypatch.setattr(backend_module, "SETTING_CONFIRM_REFRESHES", (0.0, 0.0))
    monkeypatch.setattr(backend_module, "SETTING_CONFIRM_GRACE_SECONDS", 0.05)
    monkeypatch.setattr(cloud_grpc, "CloudGrpcClient", _RecordingGrpc)
    _RecordingGrpc.sent = []

    def refresh(delay: float) -> None:
        name = _RecordingGrpc.last_name if panel_applies else "Kitchen"
        backend._adopt_circuits(parse_trait_snapshot(_labelled_circuit_traits(name)))

    monkeypatch.setattr(backend, "_request_refresh", refresh)
    return backend


def test_the_backend_renames_once_the_panel_reports_the_name(tmp_path, monkeypatch) -> None:
    """A rename posts once, waits for the panel's own word, and the next schema carries it."""
    backend = _renaming_backend(tmp_path, monkeypatch, panel_applies=True)

    assert backend.rename_circuit("circuit-21", "Pool pump") == "Pool pump"
    assert len(_RecordingGrpc.sent) == 1
    assert backend._circuits[21].label == "Pool pump"
    assert backend._schema_sent is False  # the new name goes out with the next frame

    # Asking for the name it already has sends nothing.
    assert backend.rename_circuit("circuit-21", " Pool pump ") == "Pool pump"
    assert len(_RecordingGrpc.sent) == 1

    try:
        backend.rename_circuit("circuit-99", "Nowhere")
    except ValueError:
        pass
    else:
        raise AssertionError("an unknown circuit was renamed")


def test_a_rename_the_panel_ignores_is_reported_and_never_shown(tmp_path, monkeypatch) -> None:
    """SendMessages only acknowledges receipt; the snapshot is what says it worked."""
    backend = _renaming_backend(tmp_path, monkeypatch, panel_applies=False)

    try:
        backend.rename_circuit("circuit-21", "Pool pump")
    except RenameNotApplied:
        pass
    else:
        raise AssertionError("an ignored rename was reported as done")
    assert backend._circuits[21].label == "Kitchen"
    assert backend._schema_sent is True  # nothing changed, so nothing reloads
    assert backend._setting_waits == []


def test_a_rename_made_in_the_span_app_reaches_the_schema(tmp_path) -> None:
    """The trait refresh that brings a new name also releases the schema again."""
    backend = CloudBackend(tmp_path / "tokens.json", "device-uuid")
    backend._adopt_circuits(parse_trait_snapshot(_labelled_circuit_traits("Kitchen")))
    backend._schema_sent = True
    backend._adopt_circuits(parse_trait_snapshot(_labelled_circuit_traits("Kitchen")))
    assert backend._schema_sent is True  # nothing moved, nothing to re-describe
    backend._adopt_circuits(parse_trait_snapshot(_labelled_circuit_traits("Pantry")))
    assert backend._schema_sent is False


def test_a_circuit_knows_its_load_management_entry() -> None:
    """The 1/31 entry that points back at the circuit is what a setting addresses."""
    circuits = parse_trait_snapshot(_main_40_traits())
    target = circuits[22].load_target
    assert target is not None
    assert (target.resource_id, target.instance_id, target.metadata) == (
        "gw-40",
        22,
        (1, 4, 31, None),
    )


def test_a_rank_the_homeowner_set_overrides_the_default() -> None:
    """Config #20 (preferred) wins over #22 (default) when the panel carries both."""
    raw = field_message(
        1,
        field_message(1, field_string(1, "gw-40"))
        + _trait(15, 22, field_varint(2, 1))
        + _load_management(22, field_varint(1, 2) + field_varint(20, 3) + field_varint(22, 47)),
    )
    assert parse_trait_snapshot(raw)[22].powerup_rank == 3


def _load_command(body: bytes) -> tuple[Any, Any]:
    """Unwrap a load-management request to its trait message and its command."""
    message = parse(body).get_msg(1)
    return message, parse(message.get_msg(14).get_msg(2).get_bytes(1))


def test_load_management_commands_carry_one_setting_each() -> None:
    """Backup priority, always-on and PowerUp rank, as SwitchLoadManagementCommandRequests."""
    target = parse_trait_snapshot(_main_40_traits())[22].load_target

    message, command = _load_command(
        build_load_management_request(target, backup_priority_payload(4), requester_id="u")
    )
    assert message.get_msg(1).get_int_opt(3) == 31
    assert message.get_msg(2).get_msg(2).get_int_opt(1) == 22
    assert command.get_msg(5).get_msg(1).get_int_opt(1) == 4

    _message, command = _load_command(
        build_load_management_request(target, shed_policy_payload(True), requester_id="u")
    )
    assert command.get_int_opt(6) is None  # a message, not a bare varint
    assert command.get_msg(6).get_int_opt(1) == 1
    _message, command = _load_command(
        build_load_management_request(target, shed_policy_payload(False), requester_id="u")
    )
    assert command.get_msg(6).get_int_opt(1) == 2

    _message, command = _load_command(
        build_load_management_request(target, powerup_rank_payload(5), requester_id="u")
    )
    assert command.get_msg(4).get_int_opt(1) == 5

    for bad in (lambda: backup_priority_payload(1), lambda: powerup_rank_payload(0)):
        try:
            bad()
        except ValueError:
            continue
        raise AssertionError("an out-of-range setting was built")


def test_the_backend_sets_load_management_once_the_panel_reports_it(tmp_path, monkeypatch) -> None:
    """Each setting posts once and returns when a re-read snapshot carries it."""
    backend = CloudBackend(tmp_path / "tokens.json", "device-uuid", user_id="user-1")
    backend._adopt_circuits(parse_trait_snapshot(_main_40_traits()))
    monkeypatch.setattr(backend, "_access_token", lambda: "token")
    monkeypatch.setattr(backend, "_requester_id", lambda token: "user-1")
    monkeypatch.setattr(backend_module, "SETTING_CONFIRM_REFRESHES", (0.0,))
    monkeypatch.setattr(backend_module, "SETTING_CONFIRM_GRACE_SECONDS", 0.05)
    sent: list[bytes] = []

    class Grpc(_RecordingGrpc):
        def send_messages(self, request: bytes) -> bytes:
            sent.append(request)
            return b""

    monkeypatch.setattr(cloud_grpc, "CloudGrpcClient", Grpc)
    applied = {
        "config": field_varint(1, 2) + field_message(2, field_varint(1, 3)) + field_varint(22, 47)
    }

    def refresh(delay: float) -> None:
        raw = field_message(
            1,
            field_message(1, field_string(1, "gw-40"))
            + _trait(15, 22, field_varint(2, 1))
            + _load_management(22, applied["config"]),
        )
        backend._adopt_circuits(parse_trait_snapshot(raw))

    monkeypatch.setattr(backend, "_request_refresh", refresh)

    applied["config"] = (
        field_varint(1, 2) + field_message(2, field_varint(1, 4)) + field_varint(22, 47)
    )
    backend.set_backup_priority("circuit-22", 4)
    assert len(sent) == 1
    backend.set_backup_priority("circuit-22", 4)  # already so: nothing sent
    assert len(sent) == 1

    applied["config"] = (
        field_varint(1, 1) + field_message(2, field_varint(1, 4)) + field_varint(22, 47)
    )
    backend.set_always_on("circuit-22", True)
    applied["config"] += field_varint(20, 2)
    backend.set_powerup_rank("circuit-22", 2)
    assert len(sent) == 3

    try:
        backend.set_powerup_rank("circuit-22", 9)  # the panel keeps 2
    except RenameNotApplied:
        pass
    else:
        raise AssertionError("an ignored rank was reported as set")


async def test_load_settings_are_entities_that_reach_the_panel(hass: HomeAssistant) -> None:
    """Priority select, always-on switch and PowerUp number, each sent to the right panel."""
    FakeStream.backends = {}
    schema, readings = PANELS[SERIAL_16]
    readings = {
        **readings,
        "circuit-21/backup_priority": "2",
        "circuit-21/shed_policy": "2",
        "circuit-21/powerup_rank": "4",
    }
    entry_40 = _entry(SERIAL_40, "MAIN 40")
    entry_16 = _entry(SERIAL_16, "MAIN 16")
    entry_40.add_to_hass(hass)
    entry_16.add_to_hass(hass)

    with (
        patch.dict(PANELS, {SERIAL_16: (schema, readings)}),
        patch.object(hub_module, "discover", return_value=[SITE]),
        patch.object(hub_module, "SiteStream", FakeStream),
        patch.object(hub_module, "write_tokens_if_missing"),
    ):
        assert await hass.config_entries.async_setup(entry_40.entry_id)
        await hass.async_block_till_done()

        registry = er.async_get(hass)
        garage = [
            e
            for e in registry.entities.values()
            if e.config_entry_id == entry_16.entry_id and "garage" in e.entity_id
        ]
        priority = next(e.entity_id for e in garage if e.domain == "select")
        always_on = next(e.entity_id for e in garage if e.entity_id.endswith("_always_on"))
        rank = next(e.entity_id for e in garage if e.domain == "number")
        assert hass.states.get(priority).state == "off_grid"
        assert hass.states.get(always_on).state == "off"
        assert float(hass.states.get(rank).state) == 4

        await hass.services.async_call(
            "select", "select_option", {"entity_id": priority, "option": "never"}, blocking=True
        )
        await hass.services.async_call(
            "number", "set_value", {"entity_id": rank, "value": 2}, blocking=True
        )
        # Readings are flushed, then the panels' snapshots move on the house
        # clock's next tick: two steps of the clock.
        for step in (10, 20):
            async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=step))
            await hass.async_block_till_done()
        assert hass.states.get(priority).state == "never"
        assert float(hass.states.get(rank).state) == 2

        # Always on withdraws the circuit's relay switch and priority select, so
        # the entry reloads; the fake panel then starts from its fixture again,
        # which is why its state is not checked after this.
        await hass.services.async_call("switch", "turn_on", {"entity_id": always_on}, blocking=True)
        await hass.async_block_till_done()

        assert FakeStream.backends[SERIAL_16].settings == [
            ("circuit-21", "backup_priority", 4),
            ("circuit-21", "powerup_rank", 2),
            ("circuit-21", "always_on", True),
        ]
        assert FakeStream.backends[SERIAL_40].settings == []


def test_configuration_stands_outside_the_aligned_history() -> None:
    """A setting applies as soon as it is read, and never moves the panel's watermark."""
    feed = hub_module.PanelFeed(panel=SITE.panels[0], backend=None)  # type: ignore[arg-type]
    feed.record(Reading(key="circuit-21/power", value="152.4", timestamp=100.0))
    feed.record(Reading(key="circuit-21/powerup_rank", value="2", timestamp=5_000.0))
    feed.record(Reading(key="panel/ethernet_link", value="1", timestamp=5_000.0))
    feed.readings["circuit-21/powerup_rank"] = "2"

    assert feed.watermark == 100.0
    assert "circuit-21/powerup_rank" not in feed.history
    assert feed.readings_at(100.0)["circuit-21/powerup_rank"] == "2"


def test_an_export_range_is_walked_in_chunks_the_server_accepts() -> None:
    """Aligned, consecutive chunks; a short tail reaches back to the server's minimum."""
    hour = 3_600_000
    day = 24 * hour
    start = 1_762_000_000_000  # not on an hour boundary
    windows = plan_export_windows(
        start_ms=start, end_ms=start + 40 * day, resolution=Resolution.HOURLY
    )
    assert windows[0][0] == start - start % hour
    assert len(windows) == 2
    assert windows[1][0] == windows[0][1]  # consecutive, no gap
    assert windows[-1][1] == start + 40 * day

    short = plan_export_windows(start_ms=0, end_ms=hour, resolution=Resolution.HOURLY)
    assert short == [(-hour, hour)]  # the two-hour minimum, reaching back
    assert plan_export_windows(start_ms=5, end_ms=5, resolution=Resolution.DAILY) == []


def test_the_history_csv_keeps_same_named_circuits_and_the_dst_hour_apart() -> None:
    """Spaces in the header, offsets in local time, empty where SPAN returned nothing."""
    from zoneinfo import ZoneInfo

    from custom_components.span_panel.history_export import build_csv

    hour = 3_600_000
    first = int(datetime(2026, 11, 1, 8, tzinfo=UTC).timestamp() * 1000)  # 01:00 PDT
    second = first + hour  # 01:00 PST, the same wall-clock hour
    history = {
        "circuit-21": {
            first: Bucket(first, first + hour, {"import": 0.1, "export": 0.0}),
            second: Bucket(second, second + hour, {"import": 0.2, "export": 0.0}),
        },
        "circuit-22": {first: Bucket(first, first + hour, {"import": 0.3, "export": 0.05})},
        "site": {first: Bucket(first, first + hour, {"grid_import": 0.4, "battery_import": 0.0})},
    }
    labels = {
        "circuit-21": ("Bedroom / Outlets", (11,)),
        "circuit-22": ("Bedroom / Outlets", (13,)),
    }
    text, rows, totals = build_csv(history, labels, ZoneInfo("America/Los_Angeles"))
    lines = text.splitlines()

    assert rows == 2
    assert lines[0].split(",") == [
        "Start (UTC)",
        "Start (local)",
        "End (UTC)",
        "Bedroom / Outlets [11] (kWh)",
        "Bedroom / Outlets [13] (kWh)",
        "Bedroom / Outlets [13] export (kWh)",
        "Grid import (kWh)",
        "Battery import (kWh)",
    ]
    assert lines[1].split(",")[1] == "2026-11-01T01:00:00-07:00"
    assert lines[2].split(",")[1] == "2026-11-01T01:00:00-08:00"
    assert lines[2].split(",")[4] == ""  # circuit 22 had no bucket that hour
    assert round(totals["Bedroom / Outlets [11] (kWh)"], 6) == 0.3


async def test_history_exports_to_a_file_and_a_download_link(
    hass: HomeAssistant, hass_ws_client, tmp_path
) -> None:
    """The action writes one CSV per cloud panel; the websocket command adds a signed link."""
    FakeStream.backends = {}
    hass.config.config_dir = str(tmp_path)
    entry_40 = _entry(SERIAL_40, "MAIN 40")
    entry_16 = _entry(SERIAL_16, "MAIN 16")
    entry_40.add_to_hass(hass)
    entry_16.add_to_hass(hass)

    with (
        patch.object(hub_module, "discover", return_value=[SITE]),
        patch.object(hub_module, "SiteStream", FakeStream),
        patch.object(hub_module, "write_tokens_if_missing"),
    ):
        assert await hass.config_entries.async_setup(entry_40.entry_id)
        await hass.async_block_till_done()

        response = await hass.services.async_call(
            DOMAIN,
            "export_history",
            {
                "start": "2026-11-01 00:00:00",
                "end": "2026-11-02 00:00:00",
                "config_entry_id": entry_16.entry_id,
            },
            blocking=True,
            return_response=True,
        )
        (export,) = response["exports"]
        assert export["panel"] == SERIAL_16
        assert export["rows"] == 2
        assert export["totals_kwh"]["Garage [9] (kWh)"] == 0.5
        written = (tmp_path / "span_history" / export["file"]).read_text().splitlines()
        assert written[0].startswith("Start (UTC),Start (local),End (UTC),Garage [9] (kWh)")
        ((start_ms, end_ms, resolution, time_zone),) = FakeStream.backends[SERIAL_16].history_calls
        assert resolution is Resolution.HOURLY
        assert end_ms - start_ms == 25 * 3_600_000  # the day the clocks went back
        assert FakeStream.backends[SERIAL_40].history_calls == []

        try:
            await hass.services.async_call(
                DOMAIN,
                "export_history",
                {"start": "2026-11-02 00:00:00", "end": "2026-11-01 00:00:00"},
                blocking=True,
                return_response=True,
            )
        except ServiceValidationError as err:
            assert err.translation_key == "history_bad_range"
        else:
            raise AssertionError("a backwards range was exported")

        client = await hass_ws_client(hass)
        await client.send_json(
            {
                "id": 1,
                "type": "span_panel/export_history",
                "start": "2026-11-01T00:00:00-07:00",
                "end": "2026-11-01T06:00:00-07:00",
                "resolution": "quarter_hour",
            }
        )
        reply = await client.receive_json()
        assert reply["success"], reply
        exports = reply["result"]["exports"]
        assert {e["panel"] for e in exports} == {SERIAL_40, SERIAL_16}
        assert all(
            e["url"].startswith("/api/span_panel/history/") and "authSig=" in e["url"]
            for e in exports
        )
