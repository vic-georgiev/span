"""The house: which panel feeds which, and what the house draws as a whole.

A house with more than one SPAN panel is a tree. A panel either has its own
supply, or is fed by another panel, through that panel's feed-through lugs or
through one of its branch circuits. SPAN's cloud reports each panel on its own
(two MAIN 16/40 panels are two separate sites to it), so the tree is the user's
answer, set on the fed panel's options. The vocabulary (own supply, lugs,
branch) is the one ha-span-ebus asks for, so both describe a house alike.

What the tree settles:

* **What the house draws** is what the root panels draw. A fed panel's power is
  already inside its upstream's main feed, so adding it again counts it twice.
* **A feed check** for each fed panel: what the upstream says goes out through
  the lugs or the branch circuit, against what the fed panel measures coming
  in. The two are separate meters on separate streams, so they agree to within
  a sampling window and a meter's accuracy, not exactly; a lasting gap means a
  wrong answer about the topology, or a decoding error.

The rules are plain functions over snapshots, so they can be tested without
Home Assistant; the glue at the bottom finds the loaded panels.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorStateClass
from homeassistant.const import EntityCategory, UnitOfPower
from homeassistant.core import HomeAssistant, callback
from span_panel_api import SpanPanelSnapshot

from .const import DOMAIN
from .coordinator import SpanPanelCoordinator
from .entity import SpanPanelEntity

CONF_FEED = "feed"

FEED_OWN_SUPPLY = "own_supply"
FEED_LUGS = "lugs"
FEED_BRANCH = "branch"


@dataclass(frozen=True)
class Feed:
    """What feeds one panel. `upstream` is the feeding panel's serial."""

    type: str = FEED_OWN_SUPPLY
    upstream: str | None = None
    circuit: str | None = None  # the upstream's circuit id, for a branch feed

    @property
    def is_fed(self) -> bool:
        """Whether another panel feeds this one."""
        return self.type in (FEED_LUGS, FEED_BRANCH) and self.upstream is not None


OWN_SUPPLY = Feed()


def feed_from_options(options: Mapping[str, Any]) -> Feed:
    """Read the feed stored on an entry's options; anything unreadable is its own supply."""
    raw = options.get(CONF_FEED)
    if not isinstance(raw, Mapping):
        return OWN_SUPPLY
    feed_type = raw.get("type")
    upstream = raw.get("upstream")
    if feed_type not in (FEED_LUGS, FEED_BRANCH) or not isinstance(upstream, str):
        return OWN_SUPPLY
    circuit = raw.get("circuit")
    return Feed(feed_type, upstream, str(circuit) if feed_type == FEED_BRANCH and circuit else None)


def feed_to_options(feed: Feed) -> dict[str, str]:
    """Return the stored form of a feed."""
    stored = {"type": feed.type}
    if feed.upstream is not None:
        stored["upstream"] = feed.upstream
    if feed.circuit is not None:
        stored["circuit"] = feed.circuit
    return stored


def effective_feeds(declared: Mapping[str, Feed], present: Iterable[str]) -> dict[str, Feed]:
    """Return the feeds that hold among the panels present.

    A feed whose upstream is the panel itself, is not present, or leads round a
    cycle is ignored, and the panel is treated as having its own supply: a
    wrong answer must not hide a panel from the house or count it in a loop.
    """
    panels = set(present)
    feeds = {
        serial: (
            feed
            if feed.is_fed and feed.upstream in panels and feed.upstream != serial
            else OWN_SUPPLY
        )
        for serial, feed in ((serial, declared.get(serial, OWN_SUPPLY)) for serial in panels)
    }
    for serial in sorted(feeds):
        seen = {serial}
        upstream = feeds[serial].upstream
        while upstream is not None:
            if upstream in seen:
                feeds[serial] = OWN_SUPPLY
                break
            seen.add(upstream)
            upstream = feeds[upstream].upstream
    return feeds


def roots(feeds: Mapping[str, Feed]) -> list[str]:
    """Panels with their own supply, in a stable order."""
    return sorted(serial for serial, feed in feeds.items() if not feed.is_fed)


def children(feeds: Mapping[str, Feed], serial: str) -> list[str]:
    """Panels fed by `serial`, in a stable order."""
    return sorted(child for child, feed in feeds.items() if feed.upstream == serial)


@dataclass(frozen=True)
class FeedCheck:
    """What the upstream sends to a fed panel, against what the panel measures."""

    expected_w: float
    measured_w: float

    @property
    def difference_w(self) -> float:
        """Upstream's figure less the fed panel's: positive when the upstream says more."""
        return self.expected_w - self.measured_w


def feed_check(
    feed: Feed, upstream: SpanPanelSnapshot | None, fed: SpanPanelSnapshot | None
) -> FeedCheck | None:
    """Compare the two meters on one feed, or None while either is missing."""
    if not feed.is_fed or upstream is None or fed is None:
        return None
    measured = fed.instant_grid_power_w
    if feed.type == FEED_LUGS:
        expected = upstream.feedthrough_power_w
    else:
        circuit = upstream.circuits.get(feed.circuit or "")
        expected = circuit.instant_power_w if circuit is not None else None
    if expected is None or measured is None:
        return None
    return FeedCheck(expected_w=float(expected), measured_w=float(measured))


def house_power(
    feeds: Mapping[str, Feed], snapshots: Mapping[str, SpanPanelSnapshot]
) -> float | None:
    """Return what the house draws: its root panels' main feeds, each fed panel inside."""
    total = 0.0
    for serial in roots(feeds):
        snapshot = snapshots.get(serial)
        if snapshot is None or snapshot.instant_grid_power_w is None:
            return None
        total += snapshot.instant_grid_power_w
    return total


# --- Home Assistant glue --------------------------------------------------------


@dataclass(frozen=True)
class LoadedPanel:
    """One loaded SPAN entry, as the house sees it."""

    serial: str
    entry_id: str
    title: str
    feed: Feed
    snapshot: SpanPanelSnapshot | None


def loaded_panels(hass: HomeAssistant) -> dict[str, LoadedPanel]:
    """Every SPAN entry that is set up, keyed by panel serial."""
    from .runtime import loaded_runtime_data  # pylint: disable=import-outside-toplevel

    panels: dict[str, LoadedPanel] = {}
    for entry in hass.config_entries.async_loaded_entries(DOMAIN):
        runtime = loaded_runtime_data(entry)
        if runtime is None or entry.unique_id is None:
            continue
        panels[entry.unique_id] = LoadedPanel(
            serial=entry.unique_id,
            entry_id=entry.entry_id,
            title=entry.title,
            feed=feed_from_options(entry.options),
            snapshot=runtime.coordinator.data,
        )
    return panels


def _loaded_coordinator(hass: HomeAssistant, serial: str | None) -> SpanPanelCoordinator | None:
    """Return the coordinator of the loaded entry for `serial`, if there is one."""
    from .runtime import loaded_runtime_data  # pylint: disable=import-outside-toplevel

    if serial is None:
        return None
    entry = hass.config_entries.async_entry_for_domain_unique_id(DOMAIN, serial)
    runtime = loaded_runtime_data(entry) if entry is not None else None
    return runtime.coordinator if runtime is not None else None


def house_feeds(panels: Mapping[str, LoadedPanel]) -> dict[str, Feed]:
    """Return the feeds that hold among the loaded panels."""
    return effective_feeds({serial: panel.feed for serial, panel in panels.items()}, panels)


# --- the feed check, as a sensor on the fed panel ----------------------------------


class SpanFeedCheckSensor(SpanPanelEntity, SensorEntity):
    """Upstream's figure for this panel's feed, less this panel's own.

    On the fed panel, beside its main feed. Positive when the upstream says
    more goes out than this panel measures coming in. The two meters stream
    separately, so a few watts either way is timing and accuracy; a gap that
    lasts says the feed setting is wrong, or a reading is decoded wrongly.
    Enabled and recorded so the agreement can be judged over a day, at the
    panel's snapshot pace like any other reading.
    """

    _attr_translation_key = "feed_check"
    _attr_device_class = SensorDeviceClass.POWER
    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_native_unit_of_measurement = UnitOfPower.WATT
    _attr_suggested_display_precision = 0
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _residual_field_paths: ClassVar[tuple[str, ...]] = (
        "panel.instant_grid_power_w",
        "panel.feedthrough_power_w",
    )

    def __init__(
        self, coordinator: SpanPanelCoordinator, snapshot: SpanPanelSnapshot, feed: Feed
    ) -> None:
        """Bind to the fed panel's coordinator and the feed its options name."""
        super().__init__(coordinator)
        self._feed = feed
        self._attr_unique_id = f"{snapshot.serial_number}_feed_check"
        self._attr_device_info = self._build_device_info(coordinator, snapshot)

    _upstream_coordinator: SpanPanelCoordinator | None = None
    _unsub_upstream: Callable[[], None] | None = None

    async def async_added_to_hass(self) -> None:
        """Also follow the upstream panel, so either side moving updates the check."""
        await super().async_added_to_hass()
        self._follow_upstream()
        self.async_on_remove(self._stop_following)

    @callback
    def _handle_coordinator_update(self) -> None:
        self._follow_upstream()
        super()._handle_coordinator_update()

    @callback
    def _follow_upstream(self) -> None:
        """Listen to the upstream's coordinator; again if it reloaded, never twice."""
        upstream = _loaded_coordinator(self.hass, self._feed.upstream)
        if upstream is self._upstream_coordinator:
            return
        self._stop_following()
        self._upstream_coordinator = upstream
        if upstream is not None:
            self._unsub_upstream = upstream.async_add_listener(self.async_write_ha_state)

    @callback
    def _stop_following(self) -> None:
        if self._unsub_upstream is not None:
            self._unsub_upstream()
        self._unsub_upstream = None
        self._upstream_coordinator = None

    def _check(self) -> FeedCheck | None:
        upstream = loaded_panels(self.hass).get(self._feed.upstream or "")
        return feed_check(
            self._feed, upstream.snapshot if upstream else None, self.coordinator.data
        )

    @property
    def native_value(self) -> float | None:
        """Return the difference, or unknown until both panels have reported."""
        check = self._check()
        return None if check is None else round(check.difference_w, 1)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return both sides of the comparison, and where the upstream's figure comes from."""
        check = self._check()
        attributes: dict[str, Any] = {
            "feed": self._feed.type,
            "upstream_serial": self._feed.upstream,
        }
        if self._feed.circuit is not None:
            attributes["upstream_circuit"] = self._feed.circuit
        if check is not None:
            attributes["upstream_w"] = round(check.expected_w, 1)
            attributes["this_panel_w"] = round(check.measured_w, 1)
        return attributes


def create_feed_check_sensors(
    coordinator: SpanPanelCoordinator, snapshot: SpanPanelSnapshot, options: Mapping[str, Any]
) -> list[SpanFeedCheckSensor]:
    """Create the feed check for a panel another panel feeds; none for one that is not."""
    feed = feed_from_options(options)
    return [SpanFeedCheckSensor(coordinator, snapshot, feed)] if feed.is_fed else []
