"""The house: what feeds each panel, what the house draws, and the feed check."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from unittest.mock import AsyncMock, patch

from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.common import MockConfigEntry
from span_panel_api import SpanCircuitSnapshot, SpanPanelSnapshot
import voluptuous as vol

from custom_components.span_panel.const import DOMAIN
from custom_components.span_panel.house import (
    CONF_FEED,
    FEED_BRANCH,
    FEED_LUGS,
    OWN_SUPPLY,
    Feed,
    LoadedPanel,
    aligned_feed_check,
    children,
    effective_feeds,
    feed_check,
    feed_from_options,
    feed_to_options,
    house_power,
    roots,
)

MAIN_40 = "XC-TEST-40"
MAIN_16 = "XC-TEST-16"


def _circuit(circuit_id: str, name: str, power: float, tabs: list[int]) -> SpanCircuitSnapshot:
    return SpanCircuitSnapshot(
        circuit_id=circuit_id,
        name=name,
        relay_state="CLOSED",
        instant_power_w=power,
        produced_energy_wh=None,
        consumed_energy_wh=None,
        tabs=tabs,
        priority="UNKNOWN",
        is_user_controllable=True,
        is_sheddable=False,
        is_never_backup=False,
    )


def _panel(
    serial: str, main_feed_w: float, feedthrough_w: float | None = None
) -> SpanPanelSnapshot:
    return SpanPanelSnapshot(
        serial_number=serial,
        firmware_version="test",
        main_relay_state="UNKNOWN",
        instant_grid_power_w=main_feed_w,
        feedthrough_power_w=feedthrough_w,
        main_meter_energy_consumed_wh=None,
        main_meter_energy_produced_wh=None,
        feedthrough_energy_consumed_wh=None,
        feedthrough_energy_produced_wh=None,
        dsm_state="UNKNOWN",
        current_run_config="UNKNOWN",
        door_state="UNKNOWN",
        proximity_proven=None,
        uptime_s=None,
        eth0_link=False,
        wlan_link=False,
        wwan_link=False,
        panel_size=48,
        circuits={"28": _circuit("28", "A/C condenser", 2400.0, [10, 12])},
    )


# --- the rules ------------------------------------------------------------------


def test_a_feed_round_trips_through_the_options() -> None:
    feed = Feed(FEED_BRANCH, MAIN_40, "28")
    assert feed_from_options({CONF_FEED: feed_to_options(feed)}) == feed
    assert feed_from_options({}) == OWN_SUPPLY
    assert feed_from_options({CONF_FEED: {"type": "lugs"}}) == OWN_SUPPLY  # no upstream


def test_the_16_on_the_40s_lugs_makes_one_tree() -> None:
    feeds = effective_feeds({MAIN_16: Feed(FEED_LUGS, MAIN_40)}, [MAIN_40, MAIN_16])
    assert roots(feeds) == [MAIN_40]
    assert children(feeds, MAIN_40) == [MAIN_16]


def test_a_wrong_answer_never_hides_a_panel() -> None:
    gone = effective_feeds({MAIN_16: Feed(FEED_LUGS, "not-here")}, [MAIN_40, MAIN_16])
    assert roots(gone) == [MAIN_16, MAIN_40]
    loop = effective_feeds(
        {MAIN_16: Feed(FEED_LUGS, MAIN_40), MAIN_40: Feed(FEED_LUGS, MAIN_16)}, [MAIN_40, MAIN_16]
    )
    assert roots(loop)  # a cycle is broken, not followed forever
    itself = effective_feeds({MAIN_16: Feed(FEED_LUGS, MAIN_16)}, [MAIN_16])
    assert roots(itself) == [MAIN_16]


def test_the_house_counts_a_fed_panel_once() -> None:
    feeds = effective_feeds({MAIN_16: Feed(FEED_LUGS, MAIN_40)}, [MAIN_40, MAIN_16])
    snapshots = {MAIN_40: _panel(MAIN_40, 3000.0, 400.0), MAIN_16: _panel(MAIN_16, 405.0)}
    assert house_power(feeds, snapshots) == 3000.0  # not 3405
    apart = effective_feeds({}, [MAIN_40, MAIN_16])
    assert house_power(apart, snapshots) == 3405.0


def test_the_feed_check_compares_the_lugs_with_the_fed_panel() -> None:
    check = feed_check(
        Feed(FEED_LUGS, MAIN_40), _panel(MAIN_40, 3000.0, 400.0), _panel(MAIN_16, 405.0)
    )
    assert check is not None
    assert (check.expected_w, check.measured_w, check.difference_w) == (400.0, 405.0, -5.0)
    assert (
        feed_check(Feed(FEED_LUGS, MAIN_40), _panel(MAIN_40, 3000.0), _panel(MAIN_16, 405.0))
        is None
    )


def test_the_feed_check_follows_a_branch_circuit() -> None:
    check = feed_check(
        Feed(FEED_BRANCH, MAIN_40, "28"), _panel(MAIN_40, 3000.0), _panel(MAIN_16, 2390.0)
    )
    assert check is not None
    assert check.difference_w == 10.0
    missing = feed_check(
        Feed(FEED_BRANCH, MAIN_40, "99"), _panel(MAIN_40, 3000.0), _panel(MAIN_16, 1.0)
    )
    assert missing is None


# --- the setting ----------------------------------------------------------------


def _shown_values(schema: vol.Schema) -> dict[str, object]:
    shown: dict[str, object] = {}
    for key in schema.schema:
        description = key.description
        if isinstance(description, Mapping) and "suggested_value" in description:
            shown[str(key)] = description["suggested_value"]
    return shown


def _loaded(snapshot_40: SpanPanelSnapshot) -> dict[str, LoadedPanel]:
    return {
        MAIN_40: LoadedPanel(MAIN_40, "entry-40", "SPAN MAIN 40", OWN_SUPPLY, snapshot_40),
        MAIN_16: LoadedPanel(MAIN_16, "entry-16", "SPAN MAIN 16", OWN_SUPPLY, None),
    }


async def _open_feed_step(hass: HomeAssistant, entry: MockConfigEntry) -> dict:
    result = await hass.config_entries.options.async_init(entry.entry_id)
    return await hass.config_entries.options.async_configure(
        result["flow_id"], user_input=_shown_values(result["data_schema"])
    )


async def test_the_16_is_set_on_the_40s_lugs(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id=MAIN_16, data={}, options={}, entry_id="entry-16"
    )
    entry.add_to_hass(hass)

    with (
        patch("custom_components.span_panel.async_apply_panel_registration", AsyncMock()),
        patch(
            "custom_components.span_panel.config_flow.loaded_panels",
            return_value=_loaded(_panel(MAIN_40, 1.0)),
        ),
    ):
        result = await _open_feed_step(hass, entry)
        assert result["step_id"] == "feed"
        values = [
            option["value"] for option in result["data_schema"].schema[CONF_FEED].config["options"]
        ]
        assert values == ["own_supply", f"lugs:{MAIN_40}", f"branch:{MAIN_40}"]  # never itself
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], user_input={CONF_FEED: f"lugs:{MAIN_40}"}
        )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert feed_from_options(entry.options) == Feed(FEED_LUGS, MAIN_40)


async def test_a_branch_feed_asks_for_the_circuit(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id=MAIN_16, data={}, options={}, entry_id="entry-16"
    )
    entry.add_to_hass(hass)
    upstream = _panel(MAIN_40, 1.0)
    upstream = replace(
        upstream,
        circuits={**upstream.circuits, "21": _circuit("21", "Kitchen / Outlets", 5.0, [9])},
    )

    with (
        patch("custom_components.span_panel.async_apply_panel_registration", AsyncMock()),
        patch(
            "custom_components.span_panel.config_flow.loaded_panels", return_value=_loaded(upstream)
        ),
    ):
        result = await _open_feed_step(hass, entry)
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], user_input={CONF_FEED: f"branch:{MAIN_40}"}
        )
        assert result["step_id"] == "feed_circuit"
        labels = [
            option["label"] for option in result["data_schema"].schema["circuit"].config["options"]
        ]
        assert labels == ["Kitchen / Outlets (9)", "A/C condenser (10/12)"]  # panel order
        result = await hass.config_entries.options.async_configure(
            result["flow_id"], user_input={"circuit": "28"}
        )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert feed_from_options(entry.options) == Feed(FEED_BRANCH, MAIN_40, "28")


async def test_a_lone_panel_is_not_asked(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id=MAIN_40, data={}, options={}, entry_id="entry-40"
    )
    entry.add_to_hass(hass)

    with (
        patch("custom_components.span_panel.async_apply_panel_registration", AsyncMock()),
        patch("custom_components.span_panel.config_flow.loaded_panels", return_value={}),
    ):
        result = await _open_feed_step(hass, entry)

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert CONF_FEED not in entry.options


# --- the check at one instant ---------------------------------------------------


def test_the_check_matches_the_fed_panels_samples_to_the_upstreams_clock() -> None:
    upstream = [(0.0, 200.0), (1.0, 202.0), (2.0, 204.0)]
    fed = [(0.4, 195.0), (0.6, 196.0), (1.7, 199.0)]
    check = aligned_feed_check(upstream, fed)
    assert check is not None
    # Each fed sample meets the upstream's sample nearest it in SPAN's time.
    assert (check.at, check.expected_w, check.measured_w) == (1.7, 204.0, 199.0)
    assert check.difference_w == 5.0
    # 0.6 is nearer the upstream's 1.0 than its 0.0: (202-196 + 204-199) / 2.
    assert check.average_difference_w == 5.5
    assert check.pairs == 2


def test_a_load_switching_between_frames_is_no_jump() -> None:
    """A 2 kW load comes on at t=10.0; the two panels frame it at different times."""
    upstream = [(9.0, 200.0), (9.6, 200.0), (10.6, 2200.0), (11.6, 2200.0)]
    fed = [(8.3, 205.0), (9.3, 205.0), (10.3, 2205.0), (11.3, 2205.0)]
    # Latest against latest: 200 (at 9.6) against 2205 (at 10.3) is a 2 kW gap
    # that is not there. Matched in time, the transition itself is skipped.
    check = aligned_feed_check(upstream, fed)
    assert check is not None
    assert (check.at, check.difference_w) == (11.3, -5.0)
    assert check.average_difference_w == -5.0  # 9.3 and 11.3; never 10.3
    assert check.pairs == 2


def test_no_pair_across_a_gap_in_the_upstreams_stream() -> None:
    upstream = [(0.0, 100.0), (30.0, 100.0)]
    assert aligned_feed_check(upstream, [(14.0, 100.0), (15.0, 100.0)]) is None
    assert aligned_feed_check([], [(1.0, 1.0), (2.0, 1.0)]) is None
