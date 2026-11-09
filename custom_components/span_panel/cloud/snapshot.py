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

from span_panel_api import FieldMetadata, SpanCircuitSnapshot, SpanPanelSnapshot, SpanPcsSnapshot

from .span_client.backend import (
    CONFIG_BACKUP_PRIORITY,
    CONFIG_BATTERY_LIMIT,
    CONFIG_CELLULAR_LINK,
    CONFIG_ETHERNET_LINK,
    CONFIG_MAIN_BREAKER,
    CONFIG_POWERUP_RANK,
    CONFIG_SERVICE_LIMIT,
    CONFIG_SHED_POLICY,
    CONFIG_WIFI_LINK,
    RELAY_PROPERTY,
    SITE_NODE,
)
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
    "feedthrough_energy_consumed_wh",
    "feedthrough_energy_produced_wh",
    "main_meter_energy_produced_wh",
    "grid_islandable",
)

# Circuit fields likewise. Produced energy is not requested from the history RPC.
_UNRESOLVED_CIRCUIT_FIELDS = (
    "is_never_backup",
    "relay_requester",
    "produced_energy_wh",
)

# PowerUp is SPAN's name for the panel's Power Control System, so it lands where
# the official model keeps one. What the cloud does not say is whether PowerUp is
# limiting right now, or which limit is binding.
_UNRESOLVED_PCS_FIELDS = ("active", "binding_constraint")

# The shed policy SPAN gives the breakers it never turns off: the one set to
# "always on" in the app, and the battery's own breaker.
SHED_POLICY_ALWAYS_ON = 1

# SPAN's backup priority -> the official CircuitPriority member name. The labels
# are the SPAN app's, checked circuit by circuit against both panels.
_PRIORITY_BY_BACKUP = {
    4: "NEVER",  # "Stay on in an outage"
    3: "SOC_THRESHOLD",  # "Stay on until 50% battery"
    2: "OFF_GRID",  # "Not available in an outage"
}

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

# The first space of those models: the eight positions before it are the
# main-breaker module, which holds no branch breakers, so nothing draws them.
FIRST_BREAKER_SPACE = 9


def first_space_for(model: str | None) -> int:
    """Return the panel's lowest breaker space: 9 on a MAIN 40 or 16, else 1."""
    return FIRST_BREAKER_SPACE if model in _LAST_SPACE_BY_MODEL else 1


def _float(readings: Mapping[str, str], key: str) -> float | None:
    raw = readings.get(key)
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _int(readings: Mapping[str, str], key: str) -> int | None:
    value = _float(readings, key)
    return None if value is None else int(value)


def _priority(readings: Mapping[str, str], node_id: str) -> str:
    backup = _int(readings, f"{node_id}/{CONFIG_BACKUP_PRIORITY}")
    return _PRIORITY_BY_BACKUP.get(backup, UNKNOWN) if backup is not None else UNKNOWN


def _pcs(readings: Mapping[str, str]) -> SpanPcsSnapshot | None:
    """PowerUp's limits, or None for a panel whose snapshot names none.

    The enforced limit is taken to be the service limit, which is what binds
    while the grid is up. The battery limit, on the site with the Powerwall, is
    the one that binds while it is not.
    """
    service = _float(readings, f"panel/{CONFIG_SERVICE_LIMIT}")
    if service is None:
        return None
    return SpanPcsSnapshot(
        import_limit_a=service,
        feed_import_limit_a=service,
        off_grid_import_limit_a=_float(readings, f"panel/{CONFIG_BATTERY_LIMIT}"),
    )


def _wh(total_kwh: float | None) -> float | None:
    """Convert to whole Wh: finer than any meter here measures, and coarser than float noise."""
    return None if total_kwh is None else float(round(total_kwh * 1000.0))


def _nodes(schema: PanelSchema, kind: NodeKind) -> dict[str, list[PropertySpec]]:
    out: dict[str, list[PropertySpec]] = {}
    for spec in schema.properties.values():
        if spec.node_kind is kind:
            out.setdefault(spec.node_id, []).append(spec)
    return out


def circuit_id_for(node_id: str) -> str:
    """Return the integration's circuit id for a cloud node: its instance number."""
    return node_id.removeprefix("circuit-")


def _switchable(specs: list[PropertySpec]) -> bool:
    """Whether Home Assistant may switch this breaker.

    It needs a relay the snapshot resolved, and a breaker the panel knows the
    rating of. A breaker without one is not a load SPAN has commissioned: on a
    MAIN 40 the two-pole on spaces 46/48 is the battery's, which the SPAN app
    shows as reserved and offers no switch for. Turning a battery's breaker off
    from a dashboard is not a control to hand out on a guess.
    """
    has_relay = any(spec.property_id == RELAY_PROPERTY for spec in specs)
    return has_relay and any(spec.breaker_amps for spec in specs)


def circuit_is_switchable(schema: PanelSchema, circuit_id: str) -> bool:
    """Answer the same for one circuit, for the control path to check again."""
    node = node_for_circuit(circuit_id)
    return _switchable([spec for spec in schema.properties.values() if spec.node_id == node])


def node_for_circuit(circuit_id: str) -> str:
    """Return the cloud node id for a circuit id."""
    return f"circuit-{circuit_id}"


def main_feed_key(schema: PanelSchema) -> str | None:
    """Return the reading that is the panel's main feed, if the panel reports one.

    The main feed is a metered instance that is not a branch circuit. A panel
    can report a second such instance, its feed-through lugs (41 on a MAIN 40,
    where the main feed is 2), so the lowest-numbered one is taken.
    """
    feeds = sorted(
        (int(spec.node_id.rsplit("-", 1)[-1]), spec.key)
        for spec in schema.properties.values()
        if spec.node_kind is NodeKind.LUGS
        and spec.property_id == "power"
        and spec.node_id.rsplit("-", 1)[-1].isdigit()
    )
    return feeds[0][1] if feeds else None


def power_reading_key(
    schema: PanelSchema, source: str, circuit_id: str | None = None
) -> str | None:
    """Return the reading behind one of the snapshot's power figures.

    `source` is `main_feed` (`instant_grid_power_w`), `feedthrough`
    (`feedthrough_power_w`) or `circuit` (that circuit's `instant_power_w`).
    The readings carry the same values and signs the snapshot does.
    """
    if source == "main_feed":
        return main_feed_key(schema) or "panel/power"
    if source == "feedthrough":
        return "panel/feedthrough_power"
    if source == "circuit" and circuit_id is not None:
        return f"{node_for_circuit(circuit_id)}/power"
    return None


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
        circuit_id = circuit_id_for(node_id)
        relay = readings.get(f"{node_id}/{RELAY_PROPERTY}", UNKNOWN)
        priority = _priority(readings, node_id)
        rank = _int(readings, f"{node_id}/{CONFIG_POWERUP_RANK}")
        always_on = _int(readings, f"{node_id}/{CONFIG_SHED_POLICY}") == SHED_POLICY_ALWAYS_ON
        circuits[circuit_id] = SpanCircuitSnapshot(
            circuit_id=circuit_id,
            name=first.node_name or f"Circuit {circuit_id}",
            relay_state=relay if relay in ("OPEN", "CLOSED") else UNKNOWN,
            instant_power_w=_float(readings, f"{node_id}/power"),
            produced_energy_wh=None,
            consumed_energy_wh=_wh(energy_kwh(f"{node_id}/power")),
            tabs=sorted(first.spaces),
            priority=priority,
            # As in SPAN's own model: an always-on breaker is not the user's to
            # switch, so it gets no switch.
            is_user_controllable=_switchable(specs) and not always_on,
            is_sheddable=priority in ("SOC_THRESHOLD", "OFF_GRID"),
            is_never_backup=False,
            always_on=always_on,
            current_a=_float(readings, f"{node_id}/current"),
            breaker_rating_a=float(first.breaker_amps) if first.breaker_amps else None,
            # A circuit with no rank is one SPAN gave none, not one PowerUp
            # leaves alone; the app lists it last.
            pcs_managed=True if rank is not None else None,
            pcs_priority=rank,
        )

    # The main feed is a metered instance that is not a branch circuit; the
    # panel's own node is the busbar. Either is "what the panel draws". A panel
    # can report a second such instance, its feed-through lugs (41 on a MAIN 40,
    # where the main feed is 2), so the lowest-numbered one is taken.
    feed_key = main_feed_key(schema)
    feed_power = _float(readings, feed_key) if feed_key else None
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
        eth0_link=readings.get(f"panel/{CONFIG_ETHERNET_LINK}") == "1",
        wlan_link=readings.get(f"panel/{CONFIG_WIFI_LINK}") == "1",
        wwan_link=readings.get(f"panel/{CONFIG_CELLULAR_LINK}") == "1",
        panel_size=panel_size,
        l1_voltage=_float(readings, "panel/voltage_l1"),
        l2_voltage=_float(readings, "panel/voltage_l2"),
        vendor_name="SPAN",
        model=identity.model,
        circuits=circuits,
        main_breaker_rating_a=_int(readings, f"panel/{CONFIG_MAIN_BREAKER}"),
        pcs=_pcs(readings),
        power_flow_grid=flow("grid"),
        power_flow_site=flow("home"),
    )


CLOUD_GAP_PATHS = frozenset(
    ["panel.feedthrough_power_w"]
    + [f"panel.{name}" for name in _UNRESOLVED_PANEL_FIELDS]
    + [f"circuit.{name}" for name in _UNRESOLVED_CIRCUIT_FIELDS]
    + [f"pcs.{name}" for name in _UNRESOLVED_PCS_FIELDS]
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
        "circuit.priority": FieldMetadata(None, "enum"),
        "circuit.is_sheddable": FieldMetadata(None, "boolean"),
        "circuit.always_on": FieldMetadata(None, "boolean"),
        "panel.main_breaker_rating_a": FieldMetadata("A", "integer"),
        "pcs.import_limit_a": FieldMetadata("A", "float"),
        "panel.eth0_link": FieldMetadata(None, "boolean"),
        "panel.wlan_link": FieldMetadata(None, "boolean"),
        "panel.wwan_link": FieldMetadata(None, "boolean"),
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
    for name in _UNRESOLVED_PCS_FIELDS:
        meta[f"pcs.{name}"] = FieldMetadata(None, "string", resolved=False)
    return meta
