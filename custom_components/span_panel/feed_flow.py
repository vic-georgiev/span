"""The "what feeds this panel" question, asked while adding a panel and in its options.

One set of steps serves both flows. Adding a panel asks it before the entry is
created, with the likely answer already chosen; a panel's options offer it as
its own menu entry, under a sentence saying how the panel is connected now.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.config_entries import ConfigFlowResult
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)
from span_panel_api import SpanPanelSnapshot
import voluptuous as vol

from .house import (
    CONF_FEED,
    FEED_BRANCH,
    FEED_LUGS,
    FEED_OWN_SUPPLY,
    OWN_SUPPLY,
    Feed,
    LoadedPanel,
    feed_check,
    loaded_panels,
)

if TYPE_CHECKING:
    from homeassistant.data_entry_flow import FlowHandler

    _FlowBase = FlowHandler[Any, ConfigFlowResult, Any]
else:
    _FlowBase = object


def panel_label(panel: LoadedPanel) -> str:
    """Name a panel so two of one name tell apart: its title, its model, its serial."""
    model = panel.snapshot.model if panel.snapshot is not None else None
    details = " · ".join(part for part in (model, panel.serial) if part)
    return f"{panel.title} ({details})" if details else panel.title


def circuit_label(name: str, tabs: list[int]) -> str:
    """Label a circuit with its spaces, which tell apart two circuits of one name."""
    spaces = "/".join(str(tab) for tab in sorted(tabs))
    return f"{name} ({spaces})" if spaces else name


def format_watts(watts: float) -> str:
    """Show power below 1 kW in W, from 1 kW in kW with two decimals."""
    if abs(watts) < 1000:
        return f"{watts:.0f} W"
    return f"{watts / 1000:.2f} kW"


def likely_feed(others: dict[str, LoadedPanel]) -> Feed:
    """Guess what feeds a panel being added: the one other panel whose lugs pass power.

    A panel whose feed-through lugs carry power is feeding something. When
    exactly one panel does, that is almost always the panel being added; with
    none or several there is nothing to go on, and the answer stays its own
    supply for the user to change.
    """
    feeding = [
        serial
        for serial, panel in others.items()
        if panel.snapshot is not None and panel.snapshot.feedthrough_power_w not in (None, 0)
    ]
    return Feed(FEED_LUGS, feeding[0]) if len(feeding) == 1 else OWN_SUPPLY


def describe_connection(
    feed: Feed,
    others: dict[str, LoadedPanel],
    own: SpanPanelSnapshot | None,
    *,
    answered: bool,
) -> str:
    """Say in a sentence or two what feeds this panel, and whether the meters agree."""
    if not answered:
        return "Not set yet, so this panel is counted as having its own supply."
    if not feed.is_fed:
        return "This panel has its own supply: the utility, or a panel outside SPAN."
    upstream = others.get(feed.upstream or "")
    if upstream is None:
        return (
            f"This panel is set as fed by panel {feed.upstream}, which is not set up "
            "in Home Assistant. Choose again below."
        )
    name = panel_label(upstream)
    if feed.type == FEED_LUGS:
        sentence = f"This panel is fed from the feed-through lugs of {name}."
    else:
        circuit = upstream.snapshot.circuits.get(feed.circuit or "") if upstream.snapshot else None
        where = circuit_label(circuit.name, circuit.tabs) if circuit else "a branch circuit"
        sentence = f"This panel is fed from {where} on {name}."
    check = feed_check(feed, upstream.snapshot, own)
    if check is None:
        return f"{sentence} Feed check: waiting for readings from both panels."
    return (
        f"{sentence} Feed check: {upstream.title} sends {format_watts(check.expected_w)}, "
        f"this panel measures {format_watts(check.measured_w)} "
        f"(difference {format_watts(check.difference_w)})."
    )


class FeedSteps(_FlowBase):
    """The feed steps, mixed into the config flow and the options flow.

    The host flow says which panel is being asked about, what its answer is so
    far, and what happens once it is given.
    """

    _pending_feed: Feed

    def _feed_serial(self) -> str | None:
        """Return the serial of the panel being asked about."""
        raise NotImplementedError

    def _feed_so_far(self) -> Feed:
        """Return the answer to start from."""
        raise NotImplementedError

    async def _async_feed_answered(self, feed: Feed) -> ConfigFlowResult:
        """Act on the answer."""
        raise NotImplementedError

    def _other_panels(self) -> dict[str, LoadedPanel]:
        """Return the other SPAN panels set up in this Home Assistant, by serial."""
        own = (self._feed_serial() or "").casefold()
        return {
            serial: panel
            for serial, panel in loaded_panels(self.hass).items()
            if serial.casefold() != own
        }

    async def async_step_feed(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Ask what feeds this panel: its own supply, or another panel's lugs or branch."""
        others = self._other_panels()
        current = self._feed_so_far()
        if user_input is not None:
            feed = _feed_from_choice(user_input[CONF_FEED])
            if feed.type == FEED_BRANCH and feed.upstream in others:
                if current.type == FEED_BRANCH and current.upstream == feed.upstream:
                    feed = Feed(FEED_BRANCH, feed.upstream, current.circuit)
                self._pending_feed = feed
                return await self.async_step_feed_circuit()
            return await self._async_feed_answered(feed)

        choices = [SelectOptionDict(value=FEED_OWN_SUPPLY, label="Its own supply")]
        for serial, panel in sorted(others.items(), key=lambda item: panel_label(item[1])):
            name = panel_label(panel)
            choices.append(
                SelectOptionDict(
                    value=f"{FEED_LUGS}:{serial}", label=f"The feed-through lugs of {name}"
                )
            )
            choices.append(
                SelectOptionDict(value=f"{FEED_BRANCH}:{serial}", label=f"A breaker in {name}")
            )
        default = f"{current.type}:{current.upstream}" if current.is_fed else FEED_OWN_SUPPLY
        if default not in {choice["value"] for choice in choices}:
            default = FEED_OWN_SUPPLY
        schema = vol.Schema(
            {
                vol.Required(CONF_FEED, default=default): SelectSelector(
                    SelectSelectorConfig(options=choices, mode=SelectSelectorMode.LIST)
                )
            }
        )
        return self.async_show_form(step_id="feed", data_schema=schema)

    async def async_step_feed_circuit(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Ask which of the upstream panel's breakers feeds this panel."""
        feed = self._pending_feed
        if user_input is not None:
            return await self._async_feed_answered(
                Feed(FEED_BRANCH, feed.upstream, str(user_input["circuit"]))
            )

        upstream = self._other_panels().get(feed.upstream or "")
        circuits = upstream.snapshot.circuits if upstream and upstream.snapshot else {}
        choices = [
            SelectOptionDict(value=circuit_id, label=circuit_label(circuit.name, circuit.tabs))
            for circuit_id, circuit in sorted(
                circuits.items(), key=lambda item: (min(item[1].tabs, default=0), item[0])
            )
        ]
        if not choices:
            return self.async_abort(reason="feed_no_circuits")
        default = feed.circuit if feed.circuit in circuits else choices[0]["value"]
        schema = vol.Schema(
            {
                vol.Required("circuit", default=default): SelectSelector(
                    SelectSelectorConfig(options=choices, mode=SelectSelectorMode.DROPDOWN)
                )
            }
        )
        return self.async_show_form(
            step_id="feed_circuit",
            data_schema=schema,
            description_placeholders={"upstream": panel_label(upstream) if upstream else ""},
        )


def _feed_from_choice(value: str) -> Feed:
    """Read the feed select: `own_supply`, or `lugs:<serial>` / `branch:<serial>`."""
    feed_type, _, upstream = value.partition(":")
    if feed_type in (FEED_LUGS, FEED_BRANCH) and upstream:
        return Feed(feed_type, upstream)
    return OWN_SUPPLY
