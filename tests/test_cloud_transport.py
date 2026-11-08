"""The SPAN cloud transport: a site's stream feeding the integration's own entities.

The vendored client's network half is replaced by a fake backend that speaks its
callbacks, so these run the real hub, client, snapshot mapping, coordinator and
platforms end to end.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any
from unittest.mock import patch
import uuid

from homeassistant import config_entries
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed

from custom_components.span_panel.cloud import hub as hub_module
from custom_components.span_panel.cloud.snapshot import (
    PanelIdentity,
    build_snapshot,
    field_metadata,
)
from custom_components.span_panel.cloud.span_client.backend import PanelInfo, SiteInfo
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

    assert snapshot.panel_size == 40
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
    # The cloud numbers a MAIN 40's spaces 9..48; the card draws 1..40.
    assert kitchen.tabs == [1]
    assert snapshot.circuits["22"].tabs == [3, 5]


def test_a_main_40s_spaces_shift_by_its_numbering_not_by_what_is_used() -> None:
    """Empty top spaces must not move every breaker."""
    schema = _panel_schema(SERIAL_40, {21: ("Low", (9,)), 22: ("Mid", (30,))}, flows=False)
    snapshot = build_snapshot(PanelIdentity(SERIAL_40, "MAIN 40"), schema, {}, lambda _key: None)

    assert snapshot.circuits["21"].tabs == [1]
    assert snapshot.circuits["22"].tabs == [22]


def test_an_unknown_models_spaces_move_only_if_they_cannot_fit() -> None:
    """Without a known numbering, spaces that fit stay where they are."""
    schema = _panel_schema(SERIAL_16, {21: ("A", (1,)), 22: ("B", (16,))}, flows=False)
    snapshot = build_snapshot(PanelIdentity(SERIAL_16, "MAIN 16"), schema, {}, lambda _key: None)

    assert snapshot.circuits["21"].tabs == [1]
    assert snapshot.circuits["22"].tabs == [16]


def test_fields_the_cloud_does_not_carry_are_declared_unresolved() -> None:
    schema, readings = PANELS[SERIAL_40]
    snapshot = build_snapshot(
        PanelIdentity(SERIAL_40, "MAIN 40"), schema, readings, lambda _k: None
    )
    meta = field_metadata(snapshot)

    assert meta["panel.door_state"].resolved is False
    assert meta["panel.wlan_link"].resolved is False
    assert meta["circuit.priority"].resolved is False
    assert meta["circuit.instant_power_w"].resolved is True
    assert meta["panel.power_flow_grid"].resolved is True
    assert "panel.power_flow_battery" not in meta  # no battery: nothing to say


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
            "gw-40": [sample(21, 152.4), sample(56, 2611.0)],
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
    assert snap_16.circuits["21"].name == "Garage"
    assert snap_16.circuits["21"].instant_power_w == 40.0
    assert snap_16.circuits["21"].relay_state == "OPEN"
    assert snap_16.power_flow_grid is None  # the site's flows went to the lead panel
    await hub.async_stop()
