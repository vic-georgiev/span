"""The power timeline: local samples stamped on this machine's clock, and the lag between panels."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_time_changed
from span_panel_api import SpanMqttClient, SpanPanelSnapshot

from custom_components.span_panel.coordinator import SpanPanelCoordinator
from custom_components.span_panel.house import sample_interval, step_lag
from custom_components.span_panel.timeline import (
    CLOCK_HOME_ASSISTANT,
    CLOCK_SPAN,
    KEEP_S,
    PowerTimeline,
)

from .factories import SpanCircuitSnapshotFactory, SpanPanelSnapshotFactory


def _snapshot(main_w: float, feedthrough_w: float | None = None) -> SpanPanelSnapshot:
    return replace(
        SpanPanelSnapshotFactory.create(),
        instant_grid_power_w=main_w,
        feedthrough_power_w=feedthrough_w,
        circuits={"7": replace(SpanCircuitSnapshotFactory.create(), instant_power_w=120.0)},
    )


def test_the_timeline_keeps_each_figure_with_its_stamp() -> None:
    timeline = PowerTimeline()
    timeline.record(100.0, _snapshot(1500.0, 400.0))
    timeline.record(100.25, _snapshot(1510.0, None))

    main = timeline.recent("main_feed")
    assert main.clock == CLOCK_HOME_ASSISTANT
    assert main.samples == [(100.0, 1500.0), (100.25, 1510.0)]
    # A figure the panel did not report is a gap, not a zero.
    assert timeline.recent("feedthrough").samples == [(100.0, 400.0)]
    assert timeline.recent("circuit", "7").samples == [(100.0, 120.0), (100.25, 120.0)]


def test_the_timeline_forgets_what_is_older_than_it_keeps() -> None:
    timeline = PowerTimeline()
    timeline.record(0.0, _snapshot(1.0))
    timeline.record(KEEP_S + 1.0, _snapshot(2.0))
    assert timeline.recent("main_feed").samples == [(KEEP_S + 1.0, 2.0)]


# --- the lag between panels, from load changes both see -------------------------


def _series(start: float, step: float, levels: list[float]) -> list[tuple[float, float]]:
    return [(start + i * step, level) for i, level in enumerate(levels)]


def test_a_load_change_both_panels_see_gives_the_lag() -> None:
    # A 1.5 kW load switches on: the 40's lugs show it at 10.0, the 16 at 10.25.
    upstream = _series(9.0, 0.25, [400.0] * 4 + [1900.0] * 4)
    fed = _series(9.25, 0.25, [405.0] * 4 + [1905.0] * 4)
    lag = step_lag(upstream, fed)
    assert lag is not None
    assert lag.lag_s == 0.25
    assert lag.steps == 1


def test_the_lag_is_negative_when_the_fed_panel_shows_it_first() -> None:
    upstream = _series(9.5, 0.25, [400.0] * 4 + [1900.0] * 4)
    fed = _series(9.25, 0.25, [405.0] * 4 + [1905.0] * 4)
    lag = step_lag(upstream, fed)
    assert lag is not None and lag.lag_s == -0.25


def test_steps_of_different_sizes_are_different_loads() -> None:
    # The 40's lugs step by 1.5 kW while the 16 steps by 300 W: not one change.
    upstream = _series(9.0, 0.25, [400.0] * 4 + [1900.0] * 4)
    fed = _series(9.0, 0.25, [405.0] * 4 + [705.0] * 4)
    assert step_lag(upstream, fed) is None


def test_no_lag_while_the_load_is_steady() -> None:
    upstream = _series(0.0, 0.25, [400.0] * 8)
    fed = _series(0.0, 0.25, [405.0] * 8)
    assert step_lag(upstream, fed) is None


def test_the_sample_interval_is_the_median_gap() -> None:
    assert sample_interval([(0.0, 1.0), (0.25, 1.0), (0.5, 1.0), (2.0, 1.0)]) == 0.25
    assert sample_interval([(0.0, 1.0)]) is None


# --- the coordinator: every sample recorded, entities paced ---------------------


def _coordinator(hass: HomeAssistant, publish_interval: float) -> SpanPanelCoordinator:
    return SpanPanelCoordinator(
        hass,
        cast(SpanMqttClient, MagicMock(spec=SpanMqttClient)),
        MockConfigEntry(domain="span_panel", entry_id="entry-timeline", title="SPAN"),
        publish_interval=publish_interval,
    )


async def test_every_sample_is_recorded_and_entities_see_one_per_interval(
    hass: HomeAssistant,
) -> None:
    coordinator = _coordinator(hass, 5.0)
    first, second, third = _snapshot(1000.0), _snapshot(1100.0), _snapshot(1200.0)
    with (
        patch.object(coordinator, "async_set_updated_data") as published,
        patch.object(coordinator, "_run_post_update_tasks", AsyncMock()),
    ):
        for snapshot in (first, second, third):
            await coordinator._on_snapshot_push(snapshot)

        assert [w for _, w in coordinator.recent_power("main_feed").samples] == [
            1000.0,
            1100.0,
            1200.0,
        ]
        # The first goes straight out; the rest wait for the interval, and only
        # the newest of them is published.
        published.assert_called_once_with(first)
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=6))
        await hass.async_block_till_done()
        assert published.call_args_list[-1].args == (third,)
        assert published.call_count == 2
    await coordinator.async_shutdown()


async def test_without_an_interval_every_snapshot_is_published(hass: HomeAssistant) -> None:
    coordinator = _coordinator(hass, 0.0)
    with (
        patch.object(coordinator, "async_set_updated_data") as published,
        patch.object(coordinator, "_run_post_update_tasks", AsyncMock()),
    ):
        await coordinator._on_snapshot_push(_snapshot(1000.0))
        await coordinator._on_snapshot_push(_snapshot(1100.0))
    assert published.call_count == 2


def test_a_transport_with_its_own_samples_keeps_spans_clock(hass: HomeAssistant) -> None:
    client = MagicMock()
    client.recent_power.return_value = [(1.0, 500.0)]
    coordinator = SpanPanelCoordinator(
        hass, cast(SpanMqttClient, client), MockConfigEntry(domain="span_panel")
    )
    recent = coordinator.recent_power("feedthrough")
    assert recent.clock == CLOCK_SPAN
    assert list(recent.samples) == [(1.0, 500.0)]
    client.recent_power.assert_called_once_with("feedthrough", None)
