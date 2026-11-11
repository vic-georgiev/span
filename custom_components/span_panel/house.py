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

from bisect import bisect_left
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
import itertools
from statistics import median
import time
from typing import Any, ClassVar

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorStateClass
from homeassistant.const import EntityCategory, UnitOfPower
from homeassistant.core import HomeAssistant, callback
from homeassistant.util import dt as dt_util
from span_panel_api import SpanPanelSnapshot

from .const import DOMAIN
from .coordinator import SpanPanelCoordinator
from .entity import SpanPanelEntity
from .timeline import CLOCK_SPAN, Timeline

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


# --- the feed check at one instant ------------------------------------------------

# Two samples further apart than this are not bridged: interpolating across a gap
# in the upstream's stream would invent a reading.
MAX_SAMPLE_GAP_S = 5.0
# The average reported beside the instant's difference.
AVERAGE_WINDOW_S = 60.0
# Two readings of one steady load agree to within this: the larger of a few
# watts and a few percent, which a meter's noise and accuracy cover.
STEADY_TOLERANCE_W = 25.0
STEADY_TOLERANCE_RATIO = 0.05

Series = Sequence[tuple[float, float]]


@dataclass(frozen=True)
class AlignedFeedCheck(FeedCheck):
    """A feed check taken at one instant of a clock both panels' samples are on."""

    at: float = 0.0
    average_difference_w: float | None = None
    pairs: int = 0


def _steady(a: float, b: float) -> bool:
    """Return whether two readings agree to within what metering noise explains."""
    return abs(a - b) <= max(STEADY_TOLERANCE_W, STEADY_TOLERANCE_RATIO * max(abs(a), abs(b)))


def _upstream_at(series: Series, when: float) -> float | None:
    """Return the upstream's reading at `when`, or None unless it is steady there.

    `when` must fall between two of the upstream's samples close enough to
    bridge, and those two must agree: a load that switched between them puts
    the instant inside a transition, where which side of the step each panel
    sampled decides the difference, so no comparison is made there.
    """
    times = [sample[0] for sample in series]
    index = bisect_left(times, when)
    if index < len(series) and series[index][0] == when:
        return series[index][1]
    if index == 0 or index == len(series):
        return None
    (t0, v0), (t1, v1) = series[index - 1], series[index]
    if t1 - t0 > MAX_SAMPLE_GAP_S or not _steady(v0, v1):
        return None
    return v0 if when - t0 <= t1 - when else v1


def aligned_feed_check(upstream: Series, fed: Series) -> AlignedFeedCheck | None:
    """Compare the two meters at the same instants of the clock both series are on.

    The two panels stream separately, so their latest readings are from
    different moments, and a load that changes between them shows as a jump
    that is not there. Instead each of the fed panel's samples is matched with
    the upstream's reading at that same frame time, and only where both were
    steady around it: the upstream across its samples either side, the fed
    panel since its previous sample. The newest such pair is the check; the
    pairs of the last minute give its average. None until there is one.
    """
    pairs: list[tuple[float, float, float]] = []
    previous: float | None = None
    for when, measured in fed:
        settled = previous is not None and _steady(previous, measured)
        previous = measured
        if not settled:
            continue
        expected = _upstream_at(upstream, when)
        if expected is not None:
            pairs.append((when, expected, measured))
    if not pairs:
        return None
    at, expected, measured = pairs[-1]
    recent = [e - m for t, e, m in pairs if t >= at - AVERAGE_WINDOW_S]
    return AlignedFeedCheck(
        expected_w=expected,
        measured_w=measured,
        at=at,
        average_difference_w=sum(recent) / len(recent),
        pairs=len(recent),
    )


def sample_interval(series: Series) -> float | None:
    """Return the median time between one series' samples.

    On this machine's clock a figure is stamped when the sample holding it is
    taken, so this is how much older than its stamp a figure can be.
    """
    gaps = [b[0] - a[0] for a, b in itertools.pairwise(series)]
    return median(gaps) if gaps else None


def _steps(series: Series) -> list[tuple[float, float]]:
    """Return each change of level as `(time first seen, size)`."""
    return [
        (t1, v1 - v0)
        for (t0, v0), (t1, v1) in itertools.pairwise(series)
        if t1 - t0 <= MAX_SAMPLE_GAP_S and not _steady(v0, v1)
    ]


@dataclass(frozen=True)
class StepLag:
    """How much later the fed panel shows a load change than the upstream does."""

    lag_s: float
    steps: int


def step_lag(upstream: Series, fed: Series) -> StepLag | None:
    """Measure how far apart the two panels' readings are, from load changes both see.

    A load that switches steps both meters on the feed by the same amount, at
    the same moment. Each step the fed panel shows is matched with the
    upstream's step of the same size nearest it in time, and the time between
    the first samples showing the new level is how far apart the two panels'
    figures arrive on the shared clock: positive when the fed panel is later.
    The median over the window is the lag; None until a step is seen on both.
    """
    upstream_steps = _steps(upstream)
    lags: list[float] = []
    for when, change in _steps(fed):
        matches = [
            at
            for at, size in upstream_steps
            if abs(at - when) <= MAX_SAMPLE_GAP_S and _steady(size, change)
        ]
        if matches:
            lags.append(when - min(matches, key=lambda at: abs(at - when)))
    return StepLag(lag_s=median(lags), steps=len(lags)) if lags else None


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
    # When each side last updated, on this machine's monotonic clock: how far
    # apart the two figures were taken, where no clock aligns them.
    _fed_updated_at: float | None = None
    _upstream_updated_at: float | None = None
    _unrecorded_attributes = frozenset(
        {"readings_apart_s", "sample_interval_s", "step_lag_s", "step_count", "aligned_at"}
    )

    async def async_added_to_hass(self) -> None:
        """Also follow the upstream panel, so either side moving updates the check."""
        await super().async_added_to_hass()
        self._follow_upstream()
        self.async_on_remove(self._stop_following)

    @callback
    def _handle_coordinator_update(self) -> None:
        self._fed_updated_at = time.monotonic()
        self._follow_upstream()
        super()._handle_coordinator_update()

    @callback
    def _handle_upstream_update(self) -> None:
        self._upstream_updated_at = time.monotonic()
        self.async_write_ha_state()

    @callback
    def _follow_upstream(self) -> None:
        """Listen to the upstream's coordinator; again if it reloaded, never twice."""
        upstream = _loaded_coordinator(self.hass, self._feed.upstream)
        if upstream is self._upstream_coordinator:
            return
        self._stop_following()
        self._upstream_coordinator = upstream
        if upstream is not None:
            self._unsub_upstream = upstream.async_add_listener(self._handle_upstream_update)

    @callback
    def _stop_following(self) -> None:
        if self._unsub_upstream is not None:
            self._unsub_upstream()
        self._unsub_upstream = None
        self._upstream_coordinator = None
        self._upstream_updated_at = None

    def _check(self) -> FeedCheck | None:
        aligned = self._aligned_check()
        if aligned is not None:
            return aligned
        upstream = loaded_panels(self.hass).get(self._feed.upstream or "")
        return feed_check(
            self._feed, upstream.snapshot if upstream else None, self.coordinator.data
        )

    def _timelines(self) -> tuple[Timeline, Timeline] | None:
        """Return both sides' recent samples, where they are on one clock."""
        upstream_coordinator = self._upstream_coordinator
        if upstream_coordinator is None:
            return None
        upstream = upstream_coordinator.recent_power(
            _upstream_source(self._feed), self._feed.circuit
        )
        fed = self.coordinator.recent_power("main_feed")
        if upstream.clock != fed.clock or not upstream.samples or not fed.samples:
            return None
        return upstream, fed

    def _aligned_check(self) -> AlignedFeedCheck | None:
        """Return the check at one instant of the clock both panels' samples are on."""
        timelines = self._timelines()
        return aligned_feed_check(timelines[0].samples, timelines[1].samples) if timelines else None

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
        attributes["aligned"] = isinstance(check, AlignedFeedCheck)
        timelines = self._timelines() if isinstance(check, AlignedFeedCheck) else None
        if timelines is not None:
            attributes.update(_sync_attributes(*timelines))
        lag = (
            _house_clock_lag(self.hass) if timelines and timelines[0].clock == CLOCK_SPAN else None
        )
        if lag is not None:
            attributes["house_clock_lag_s"] = round(lag, 2)
        elif (
            timelines is None
            and check is not None
            and self._fed_updated_at
            and self._upstream_updated_at
        ):
            # Unaligned: each panel's latest figure, taken this far apart. Over
            # SPAN API each panel publishes on its own snapshot timer.
            attributes["readings_apart_s"] = round(
                abs(self._fed_updated_at - self._upstream_updated_at), 1
            )
        if isinstance(check, AlignedFeedCheck):
            attributes["aligned_at"] = dt_util.utc_from_timestamp(check.at).isoformat()
            if check.average_difference_w is not None:
                attributes["average_difference_w"] = round(check.average_difference_w, 1)
                attributes["average_pairs"] = check.pairs
        return attributes


def _house_clock_lag(hass: HomeAssistant) -> float | None:
    """Return how far behind now the house is shown, where a clock is keeping it.

    The cloud panels are shown at one shared instant (see `cloud.clock`), and the
    price is this much delay. Its frame times are SPAN's, so the figure carries
    any difference between SPAN's clock and this machine's as well.
    """
    from .cloud.clock import CLOCK_KEY  # pylint: disable=import-outside-toplevel

    clock = hass.data.get(DOMAIN, {}).get(CLOCK_KEY)
    now = getattr(clock, "now", None)
    return time.time() - now if isinstance(now, float) else None


def _sync_attributes(upstream: Timeline, fed: Timeline) -> dict[str, Any]:
    """Say which clock aligned the two panels, how finely, and the lag measured on it."""
    attributes: dict[str, Any] = {"clock": fed.clock}
    intervals = [
        interval
        for interval in (sample_interval(upstream.samples), sample_interval(fed.samples))
        if interval is not None
    ]
    if intervals:
        attributes["sample_interval_s"] = round(max(intervals), 2)
    lag = step_lag(upstream.samples, fed.samples)
    if lag is not None:
        attributes["step_lag_s"] = round(lag.lag_s, 2)
        attributes["step_count"] = lag.steps
    return attributes


def _upstream_source(feed: Feed) -> str:
    return "feedthrough" if feed.type == FEED_LUGS else "circuit"


def create_feed_check_sensors(
    coordinator: SpanPanelCoordinator, snapshot: SpanPanelSnapshot, options: Mapping[str, Any]
) -> list[SpanFeedCheckSensor]:
    """Create the feed check for a panel another panel feeds; none for one that is not."""
    feed = feed_from_options(options)
    return [SpanFeedCheckSensor(coordinator, snapshot, feed)] if feed.is_fed else []
