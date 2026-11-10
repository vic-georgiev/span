"""A circuit's load-management settings, as the SPAN app's homeowner screens set them.

Two per circuit, both configuration entities:

- **Always on** (a switch): SPAN never sheds the circuit, neither for PowerUp
  nor in an outage. The app's "Always on" toggle.
- **PowerUp rank** (a number): the circuit's place in PowerUp's order, the
  order the app's PowerUp screen lists.

The backup priority is the third setting, and stays the existing priority
select. Each change goes through the control gate like a relay command, and
counts as done only when the panel's own snapshot reports it.

Offered only over SPAN's cloud, the one transport that can write them today.
"""

from __future__ import annotations

from typing import Any, ClassVar

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.components.switch import SwitchEntity
from homeassistant.const import EntityCategory
from homeassistant.exceptions import HomeAssistantError
from span_panel_api import PublishOutcome, SpanCircuitSnapshot, SpanPanelSnapshot

from .cloud.client import SpanCloudClient
from .coordinator import SpanPanelCoordinator
from .entity import SpanPanelEntity
from .helpers import construct_circuit_label

# The highest rank offered. Ranks only order circuits, so any ceiling above the
# number of circuits on a panel is enough; SPAN's own limit is not recovered.
MAX_POWERUP_RANK = 64


def circuit_has_load_settings(circuit: SpanCircuitSnapshot) -> bool:
    """Whether the circuit gets the always-on and PowerUp controls.

    A circuit whose shed policy SPAN reports has a load-management entry to
    write; one without (the battery's own breaker is managed by SPAN alone) has
    nothing these controls could change.
    """
    return circuit.pcs_managed is True or circuit.always_on


def _circuits_with_settings(snapshot: SpanPanelSnapshot) -> list[str]:
    return [cid for cid, c in snapshot.circuits.items() if circuit_has_load_settings(c)]


def always_on_switches(
    coordinator: SpanPanelCoordinator, snapshot: SpanPanelSnapshot
) -> list[SpanAlwaysOnSwitch]:
    """Build an always-on switch for every circuit that has load settings."""
    return [SpanAlwaysOnSwitch(coordinator, cid) for cid in _circuits_with_settings(snapshot)]


def powerup_rank_numbers(
    coordinator: SpanPanelCoordinator, snapshot: SpanPanelSnapshot
) -> list[SpanPowerUpRankNumber]:
    """Build a PowerUp rank number for every circuit that has load settings."""
    return [SpanPowerUpRankNumber(coordinator, cid) for cid in _circuits_with_settings(snapshot)]


class _CircuitLoadSetting(SpanPanelEntity):
    """One circuit's load-management setting."""

    _attr_entity_category = EntityCategory.CONFIG
    # No residual field paths: the field-path gate checks the eBus schemas, and
    # these entities exist only over SPAN's cloud, whose snapshot builds them.
    _setting_key: ClassVar[str]
    _setting_label: ClassVar[str]

    def __init__(self, coordinator: SpanPanelCoordinator, circuit_id: str) -> None:
        super().__init__(coordinator)
        snapshot: SpanPanelSnapshot = coordinator.data
        circuit = snapshot.circuits[circuit_id]
        self.id = circuit_id
        self._attr_unique_id = f"span_{snapshot.serial_number}_{circuit_id}_{self._setting_key}"
        self._attr_device_info = self._build_device_info(coordinator, snapshot)
        self._attr_name = f"{circuit.name or circuit_id} {self._setting_label}"

    def _client(self) -> SpanCloudClient:
        """Return the cloud client; these entities are only created over SPAN's cloud."""
        client = self.coordinator.client
        if not isinstance(client, SpanCloudClient):
            raise HomeAssistantError("These settings can only be changed over SPAN's cloud")
        return client

    def _circuit(self) -> SpanCircuitSnapshot | None:
        snapshot: SpanPanelSnapshot = self.coordinator.data
        return snapshot.circuits.get(self.id)

    async def _apply(self, action: Any, what: str) -> PublishOutcome:
        outcome = await self._async_control(
            action,
            command=f"{what} for {self.entity_id}",
            failed_key="circuit_setting_failed",
            not_delivered_key="circuit_setting_not_delivered",
            placeholders={
                "setting": self._setting_label,
                "circuit": construct_circuit_label(self._circuit(), self.id),
            },
        )
        await self.coordinator.async_request_refresh()
        return outcome

    @property
    def available(self) -> bool:
        """Unavailable while the panel cannot take a change."""
        if not self._transport_available or self.coordinator.panel_offline:
            return False
        return super().available and self._circuit() is not None


class SpanAlwaysOnSwitch(_CircuitLoadSetting, SwitchEntity):
    """The app's Always on: SPAN never sheds this circuit."""

    _setting_key = "always_on"
    _setting_label = "Always on"
    _attr_icon = "mdi:pin"

    @property
    def is_on(self) -> bool | None:
        """Whether the panel reports the circuit as always on."""
        circuit = self._circuit()
        return None if circuit is None else circuit.always_on

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Mark the circuit always on."""
        await self._apply(
            self._client().set_circuit_always_on(self.id, True), "an always-on change"
        )

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Hand the circuit back to SPAN's load management."""
        await self._apply(
            self._client().set_circuit_always_on(self.id, False), "an always-on change"
        )


class SpanPowerUpRankNumber(_CircuitLoadSetting, NumberEntity):
    """The circuit's place in PowerUp's order."""

    _setting_key = "powerup_rank"
    _setting_label = "PowerUp rank"
    _attr_icon = "mdi:sort-numeric-ascending"
    _attr_native_min_value = 1
    _attr_native_max_value = MAX_POWERUP_RANK
    _attr_native_step = 1
    _attr_mode = NumberMode.BOX

    @property
    def native_value(self) -> float | None:
        """The rank the panel reports."""
        circuit = self._circuit()
        return None if circuit is None or circuit.pcs_priority is None else circuit.pcs_priority

    async def async_set_native_value(self, value: float) -> None:
        """Move the circuit to `value` in PowerUp's order."""
        await self._apply(
            self._client().set_circuit_powerup_rank(self.id, int(value)),
            "a PowerUp rank change",
        )
