"""One clock for every panel in the house, so they are shown at the same instant.

Each panel streams on its own, and frames arrive whenever they arrive: the MAIN
40's lugs can be a frame ahead of the MAIN 16 they feed, and a load that just
switched then reads differently on the two for a moment. Showing each panel's
newest reading is showing two different instants side by side.

The clock fixes that the way stream processors do, with a low watermark rather
than a buffer:

* A panel's **watermark** is the newest frame time it has delivered, on SPAN's
  own clock (the time stamped in the frame, not when it reached us).
* The **house clock** is the lowest watermark: the latest instant every panel
  has reported. Each panel is shown as it stood then, its readings taken from
  the short history its feed keeps (the last sample at or before that instant).
* So the panel that is ahead waits for the other, and only for as long as the
  other's next frame takes, a fraction of a second on the cloud stream. Nothing
  is delayed beyond that, and one panel on its own runs at its newest frame.
* A panel more than `MAX_SKEW_S` behind the newest is left out of the clock and
  shown as it is: a stream that has stalled, or dropped to cellular, must not
  freeze the house. It rejoins when it catches up.
* The clock only moves forward, and every panel's snapshot is built at the
  same tick from the same instant, so the house is drawn from one moment.

Readings that are not telemetry (relay state, configuration) have no history
and are taken as they stand.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_call_later

from custom_components.span_panel.const import DOMAIN

CLOCK_KEY = "cloud_house_clock"

# How far behind the newest panel another may fall and still set the clock:
# a few of the stream's frames. Past that it is stalled, not just late.
MAX_SKEW_S = 3.0


class ClockMember(Protocol):
    """What the clock needs from a panel: its watermark, pace, and a way to show an instant."""

    @property
    def watermark(self) -> float | None:
        """Return the newest SPAN frame time this panel has delivered, if any."""

    @property
    def snapshot_interval(self) -> float:
        """Return how often, in seconds, this panel wants its snapshot rebuilt."""

    def dispatch_at(self, at: float | None) -> Awaitable[None]:
        """Build and push the panel's snapshot as it stood at `at`."""


@dataclass(frozen=True)
class ClockReading:
    """Where the clock stands: the shared instant, and the panels setting it."""

    at: float | None
    members: int
    left_out: int


def clock_instant(watermarks: list[float | None]) -> ClockReading:
    """Return the house instant for these watermarks: the lowest, stalled ones aside."""
    known = [mark for mark in watermarks if mark is not None]
    if not known:
        return ClockReading(at=None, members=0, left_out=0)
    newest = max(known)
    keeping = [mark for mark in known if newest - mark <= MAX_SKEW_S]
    return ClockReading(at=min(keeping), members=len(keeping), left_out=len(known) - len(keeping))


class HouseClock:
    """The shared clock: every panel's snapshot is built at one instant, at one tick."""

    def __init__(self, hass: HomeAssistant) -> None:
        """Start with no panels and no instant."""
        self._hass = hass
        self._members: list[ClockMember] = []
        self._now: float | None = None
        self._timer: Callable[[], None] | None = None
        self._ticking = False

    @property
    def now(self) -> float | None:
        """Return the instant the house was last shown at, on SPAN's clock."""
        return self._now

    @callback
    def join(self, member: ClockMember) -> Callable[[], None]:
        """Add a panel; returns how it leaves."""
        self._members.append(member)

        @callback
        def leave() -> None:
            if member in self._members:
                self._members.remove(member)
            if not self._members and self._timer is not None:
                self._timer()
                self._timer = None

        return leave

    def reading(self) -> ClockReading:
        """Return the instant the panels' watermarks allow now, before the next tick."""
        return clock_instant([member.watermark for member in self._members])

    @callback
    def nudge(self) -> None:
        """Tick now, or at the panels' pace, after a panel delivered a frame."""
        if self._timer is not None or self._ticking:
            return
        interval = min((member.snapshot_interval for member in self._members), default=0.0)
        if interval <= 0:
            self._hass.async_create_task(self._tick())
            return
        self._timer = async_call_later(self._hass, interval, self._fire)

    @callback
    def _fire(self, _now: object) -> None:
        self._timer = None
        self._hass.async_create_task(self._tick())

    async def _tick(self) -> None:
        """Move the clock to the instant every panel has reached, and show them all there."""
        self._ticking = True
        try:
            at = self.reading().at
            if at is not None and self._now is not None and at < self._now:
                at = self._now  # never backwards: a rejoining panel catches up instead
            if at is not None:
                self._now = at
            for member in list(self._members):
                await member.dispatch_at(self._now)
        finally:
            self._ticking = False


def house_clock(hass: HomeAssistant) -> HouseClock:
    """Return the one clock for this Home Assistant's cloud panels."""
    store = hass.data.setdefault(DOMAIN, {})
    clock = store.get(CLOCK_KEY)
    if not isinstance(clock, HouseClock):
        clock = HouseClock(hass)
        store[CLOCK_KEY] = clock
    return clock
