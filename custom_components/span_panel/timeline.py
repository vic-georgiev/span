"""A local panel's recent power figures, stamped on Home Assistant's clock.

SPAN's local MQTT stream carries values only: no property says when it was
measured (SPAN API r202639). Two panels on separate brokers therefore share
no clock of SPAN's, and the one clock both pass through is this machine's.
Each sample is stamped when the snapshot holding it is built here, so a
figure is at most one sample interval older than its stamp, and two panels'
samples are on one timeline that close.

The cloud transport keeps its own samples on SPAN's frame clock instead; the
two are never mixed (see `Timeline.clock`).
"""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final

from span_panel_api import SpanPanelSnapshot

CLOCK_HOME_ASSISTANT: Final = "home_assistant"
CLOCK_SPAN: Final = "span"

# How often a local panel's snapshot is sampled. Each sample builds the panel's
# whole snapshot, measured at 0.5-0.8 ms on the reference captures, so a
# quarter second costs a few milliseconds a second per panel and bounds each
# stamp's error at a quarter second. Entities still update at the configured
# snapshot interval; only the timeline sees every sample.
SAMPLE_INTERVAL_S: Final = 0.25

# The feed check averages over a minute; a little more is kept so the oldest
# pair still has a neighbour on either side.
KEEP_S: Final = 75.0


@dataclass(frozen=True)
class Timeline:
    """One power figure's recent samples as `(time, W)`, and whose clock stamped them."""

    clock: str
    samples: Sequence[tuple[float, float]]


def _figures(snapshot: SpanPanelSnapshot) -> dict[str, float]:
    figures: dict[str, float] = {}
    if snapshot.instant_grid_power_w is not None:
        figures["main_feed"] = float(snapshot.instant_grid_power_w)
    if snapshot.feedthrough_power_w is not None:
        figures["feedthrough"] = float(snapshot.feedthrough_power_w)
    for circuit_id, circuit in snapshot.circuits.items():
        if circuit.instant_power_w is not None:
            figures[f"circuit:{circuit_id}"] = float(circuit.instant_power_w)
    return figures


def figure_key(source: str, circuit_id: str | None) -> str:
    """Name one figure: `main_feed`, `feedthrough`, or a circuit's power."""
    return f"circuit:{circuit_id}" if source == "circuit" else source


class PowerTimeline:
    """The last minute or so of a local panel's power figures."""

    def __init__(self) -> None:
        """Start empty."""
        self._samples: deque[tuple[float, dict[str, float]]] = deque()

    def record(self, at: float, snapshot: SpanPanelSnapshot) -> None:
        """Keep the snapshot's power figures, stamped `at` (seconds, this machine's clock)."""
        self._samples.append((at, _figures(snapshot)))
        while self._samples and self._samples[0][0] < at - KEEP_S:
            self._samples.popleft()

    def recent(self, source: str, circuit_id: str | None = None) -> Timeline:
        """Return one figure's samples, oldest first."""
        key = figure_key(source, circuit_id)
        return Timeline(
            CLOCK_HOME_ASSISTANT,
            [(at, figures[key]) for at, figures in self._samples if key in figures],
        )
