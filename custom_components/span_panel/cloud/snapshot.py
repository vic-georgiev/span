"""Turn what the SPAN cloud reports into the snapshot the integration reads.

Pure functions over the vendored client's schema and readings, so the mapping
can be tested without a stream. Two rules run through all of it:

* A field the cloud does not carry is never invented. Where the snapshot type
  forces a value (a bool, a state string), the placeholder goes in and the
  field is declared *unresolved* in `field_metadata`, which makes every entity
  reading it unavailable instead of showing a made-up "disconnected" or
  "closed". The Repair the integration raises for unresolved fields is then
  the list of what the cloud path is missing.
* Units are the integration's: W, A, V, Wh. The cloud's energy arrives in kWh.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import re

from span_panel_api import FieldMetadata, SpanCircuitSnapshot, SpanPanelSnapshot

from .span_client.backend import RELAY_PROPERTY, SITE_NODE
from .span_client.models import NodeKind, PanelSchema, PropertySpec

UNKNOWN = "UNKNOWN"

# Panel fields the snapshot type requires but the cloud stream does not carry.
_UNRESOLVED_PANEL_FIELDS = (
    "main_relay_state",
    "dsm_state",
    "current_run_config",
    "door_state",
    "proximity_proven",
    "uptime_s",
    "eth0_link",
    "wlan_link",
    "wwan_link",
    "feedthrough_energy_consumed_wh",
    "feedthrough_energy_produced_wh",
    "main_meter_energy_produced_wh",
    "main_breaker_rating_a",
    "grid_islandable",
)

# Circuit fields likewise. Priority and always-on exist in SPAN's cloud schema
# but are not decoded yet; produced energy is not requested from the history RPC.
_UNRESOLVED_CIRCUIT_FIELDS = (
    "priority",
    "is_sheddable",
    "is_never_backup",
    "always_on",
    "relay_requester",
    "produced_energy_wh",
)

# Cloud site flow -> (snapshot field, sign). The snapshot keeps the panel's own
# wire frame, in which grid is negative while importing (the grid sensor negates
# it back) and site is positive while the house draws. The cloud's `grid` matches
# the main feed's power, positive while importing, hence the flip. Solar and
# battery are left out until their sign is checked against a site that has them:
# a guessed sign would show charging as discharging.
_SITE_FLOW_FIELDS: dict[str, tuple[str, float]] = {
    "grid": ("power_flow_grid", -1.0),
    "home": ("power_flow_site", 1.0),
}


@dataclass(frozen=True)
class PanelIdentity:
    """What the account's topology says about the panel this entry is."""

    serial: str
    model: str  # "MAIN 40", as the cloud names it


def panel_size_for(model: str, spaces: list[int]) -> int:
    """How many spaces the card draws: enough for the numbers the panel uses.

    Spaces keep the numbers SPAN gives them, which are the ones printed on the
    panel and shown in the SPAN app. A MAIN 40 numbers its 40 branch spaces
    9..48, because 1..8 is the main-breaker module that an MLO 48 replaces with
    branch spaces, so its card is 48 tall with that module's rows left empty,
    as on the panel itself. Renumbering to 1..40 would put every breaker on a
    number that appears nowhere else.
    """
    if model in _LAST_SPACE_BY_MODEL:
        return max([_LAST_SPACE_BY_MODEL[model], *spaces])
    match = re.search(r"(\d+)", model)
    rated = int(match.group(1)) if match else 0
    return max([rated, *spaces])


# Models whose numbering does not start at 1, by their highest space number.
# Both start at 9, after the main-breaker module's eight: the SPAN app shows a
# MAIN 16's spaces as 9..24.
_LAST_SPACE_BY_MODEL = {"MAIN 40": 48, "MAIN 16": 24}


def _float(readings: Mapping[str, str], key: str) -> float | None:
    raw = readings.get(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _wh(total_kwh: float | None) -> float | None:
    return None if total_kwh is None else total_kwh * 1000.0


def _nodes(schema: PanelSchema, kind: NodeKind) -> dict[str, list[PropertySpec]]:
    out: dict[str, list[PropertySpec]] = {}
    for spec in schema.properties.values():
        if spec.node_kind is kind:
            out.setdefault(spec.node_id, []).append(spec)
    return out


def circuit_id_for(node_id: str) -> str:
    """Return the integration's circuit id for a cloud node: its instance number."""
    return node_id.removeprefix("circuit-")


def node_for_circuit(circuit_id: str) -> str:
    """Return the cloud node id for a circuit id."""
    return f"circuit-{circuit_id}"


def build_snapshot(
    identity: PanelIdentity,
    schema: PanelSchema,
    readings: Mapping[str, str],
    energy_kwh: Callable[[str], float | None],
) -> SpanPanelSnapshot:
    """One point-in-time view of the panel, in the integration's terms."""
    circuit_nodes = _nodes(schema, NodeKind.CIRCUIT)
    all_spaces = sorted({space for specs in circuit_nodes.values() for space in specs[0].spaces})
    panel_size = panel_size_for(identity.model, all_spaces)

    circuits: dict[str, SpanCircuitSnapshot] = {}
    for node_id, specs in sorted(circuit_nodes.items()):
        first = specs[0]
        props = {spec.property_id for spec in specs}
        circuit_id = circuit_id_for(node_id)
        relay = readings.get(f"{node_id}/{RELAY_PROPERTY}", UNKNOWN)
        circuits[circuit_id] = SpanCircuitSnapshot(
            circuit_id=circuit_id,
            name=first.node_name or f"Circuit {circuit_id}",
            relay_state=relay if relay in ("OPEN", "CLOSED") else UNKNOWN,
            instant_power_w=_float(readings, f"{node_id}/power"),
            produced_energy_wh=None,
            consumed_energy_wh=_wh(energy_kwh(f"{node_id}/power")),
            tabs=sorted(first.spaces),
            priority=UNKNOWN,
            is_user_controllable=RELAY_PROPERTY in props,
            is_sheddable=False,
            is_never_backup=False,
            current_a=_float(readings, f"{node_id}/current"),
            breaker_rating_a=float(first.breaker_amps) if first.breaker_amps else None,
        )

    # The main feed is the one metered instance that is not a branch circuit;
    # the panel's own node is the busbar. Either is "what the panel draws".
    feed_power = next(
        (
            _float(readings, spec.key)
            for spec in schema.properties.values()
            if spec.node_kind is NodeKind.LUGS and spec.property_id == "power"
        ),
        None,
    )
    panel_power = _float(readings, "panel/power")

    def flow(name: str) -> float | None:
        field_sign = _SITE_FLOW_FIELDS[name]
        if f"{SITE_NODE}/{name}" not in schema.properties:
            return None
        value = _float(readings, f"{SITE_NODE}/{name}")
        return None if value is None else field_sign[1] * value

    return SpanPanelSnapshot(
        serial_number=identity.serial,
        firmware_version=schema.software_version or "unknown",
        main_relay_state=UNKNOWN,
        instant_grid_power_w=feed_power if feed_power is not None else panel_power,
        feedthrough_power_w=_float(readings, "panel/feedthrough_power"),
        main_meter_energy_consumed_wh=_wh(energy_kwh(f"{SITE_NODE}/grid")),
        main_meter_energy_produced_wh=None,
        feedthrough_energy_consumed_wh=None,
        feedthrough_energy_produced_wh=None,
        dsm_state=UNKNOWN,
        current_run_config=UNKNOWN,
        door_state=UNKNOWN,
        proximity_proven=None,
        uptime_s=None,
        eth0_link=False,
        wlan_link=False,
        wwan_link=False,
        panel_size=panel_size,
        l1_voltage=_float(readings, "panel/voltage_l1"),
        l2_voltage=_float(readings, "panel/voltage_l2"),
        vendor_name="SPAN",
        model=identity.model,
        circuits=circuits,
        power_flow_grid=flow("grid"),
        power_flow_site=flow("home"),
    )


CLOUD_GAP_PATHS = frozenset(
    ["panel.feedthrough_power_w"]
    + [f"panel.{name}" for name in _UNRESOLVED_PANEL_FIELDS]
    + [f"circuit.{name}" for name in _UNRESOLVED_CIRCUIT_FIELDS]
)
"""Field paths the cloud path declares unresolved: known gaps, not defects."""


def field_metadata(
    snapshot: SpanPanelSnapshot, schema_keys: frozenset[str] = frozenset()
) -> dict[str, FieldMetadata]:
    """Declare what the cloud path produces, and what it only stands in for."""
    meta: dict[str, FieldMetadata] = {
        "panel.instant_grid_power_w": FieldMetadata("W", "float"),
        "panel.l1_voltage": FieldMetadata("V", "float"),
        "panel.l2_voltage": FieldMetadata("V", "float"),
        "panel.main_meter_energy_consumed_wh": FieldMetadata("Wh", "float"),
        "circuit.instant_power_w": FieldMetadata("W", "float"),
        "circuit.current_a": FieldMetadata("A", "float"),
        "circuit.consumed_energy_wh": FieldMetadata("Wh", "float"),
        "circuit.relay_state": FieldMetadata(None, "enum"),
        "circuit.breaker_rating_a": FieldMetadata("A", "float"),
    }
    if "panel/feedthrough_power" in schema_keys:
        meta["panel.feedthrough_power_w"] = FieldMetadata("W", "float")
    else:
        meta["panel.feedthrough_power_w"] = FieldMetadata(None, "string", resolved=False)
    for flow_field, _sign in _SITE_FLOW_FIELDS.values():
        if getattr(snapshot, flow_field) is not None:
            meta[f"panel.{flow_field}"] = FieldMetadata("W", "float")
    for name in _UNRESOLVED_PANEL_FIELDS:
        meta[f"panel.{name}"] = FieldMetadata(None, "string", resolved=False)
    for name in _UNRESOLVED_CIRCUIT_FIELDS:
        meta[f"circuit.{name}"] = FieldMetadata(None, "string", resolved=False)
    return meta
