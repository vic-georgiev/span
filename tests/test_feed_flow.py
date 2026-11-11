"""The wording and guesses behind the "what feeds this panel" question."""

from __future__ import annotations

from custom_components.span_panel.feed_flow import (
    describe_connection,
    format_watts,
    likely_feed,
    panel_label,
)
from custom_components.span_panel.house import FEED_BRANCH, FEED_LUGS, OWN_SUPPLY, Feed, LoadedPanel

from .factories import SpanCircuitSnapshotFactory, SpanPanelSnapshotFactory


def _panel(serial: str, *, feedthrough: float | None, grid: float | None = None) -> LoadedPanel:
    snapshot = SpanPanelSnapshotFactory.create(
        serial_number=serial,
        feedthrough_power_w=feedthrough,
        instant_grid_power_w=grid,
        circuits={
            "7": SpanCircuitSnapshotFactory.create(circuit_id="7", name="Sub-panel", tabs=[30, 32])
        },
    )
    return LoadedPanel(serial, f"entry-{serial}", "Span Panel", OWN_SUPPLY, snapshot)


def test_power_reads_in_watts_below_a_kilowatt_and_kilowatts_from_one() -> None:
    assert format_watts(457.2) == "457 W"
    assert format_watts(-3.4) == "-3 W"
    assert format_watts(1234.0) == "1.23 kW"


def test_two_panels_of_one_name_are_told_apart() -> None:
    label = panel_label(_panel("xg-1", feedthrough=None))
    assert label.startswith("Span Panel (")
    assert "xg-1" in label


def test_the_one_panel_passing_power_through_its_lugs_is_the_likely_feed() -> None:
    assert likely_feed({"a": _panel("a", feedthrough=457.0)}) == Feed(FEED_LUGS, "a")
    # Nothing to go on: no lugs, idle lugs, or more than one candidate.
    assert likely_feed({"a": _panel("a", feedthrough=None)}) == OWN_SUPPLY
    assert likely_feed({"a": _panel("a", feedthrough=0.0)}) == OWN_SUPPLY
    assert (
        likely_feed({"a": _panel("a", feedthrough=1.0), "b": _panel("b", feedthrough=2.0)})
        == OWN_SUPPLY
    )


def test_the_connection_is_described_with_its_feed_check() -> None:
    upstream = _panel("a", feedthrough=457.0)
    own = SpanPanelSnapshotFactory.create(serial_number="b", instant_grid_power_w=455.0)
    text = describe_connection(Feed(FEED_LUGS, "a"), {"a": upstream}, own, answered=True)
    assert "feed-through lugs of Span Panel" in text
    assert "sends 457 W" in text
    assert "measures 455 W" in text
    assert "difference 2 W" in text


def test_a_branch_feed_names_its_breaker() -> None:
    upstream = _panel("a", feedthrough=None)
    text = describe_connection(Feed(FEED_BRANCH, "a", "7"), {"a": upstream}, None, answered=True)
    assert "Sub-panel (30/32)" in text
    assert "waiting for readings" in text


def test_an_unset_or_missing_upstream_says_so() -> None:
    assert "Not set yet" in describe_connection(OWN_SUPPLY, {}, None, answered=False)
    assert "own supply" in describe_connection(OWN_SUPPLY, {}, None, answered=True)
    assert "not set up" in describe_connection(Feed(FEED_LUGS, "gone"), {}, None, answered=True)
