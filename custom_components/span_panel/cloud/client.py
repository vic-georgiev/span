"""The panel client the coordinator talks to, fed by SPAN's cloud.

Implements the same surface `SpanMqttClient` gives the coordinator and the
entities — snapshot, streaming callbacks, field metadata, relay control and the
control interceptor — over one panel's share of a `CloudSiteHub`.

Connection state is judged by the data, not the socket: the stream reconnects
on its own, and what matters to anyone looking at a breaker is whether the
numbers on screen are current. No reading for `STALE_AFTER_SECONDS` reports the
panel offline, which makes the entities unavailable rather than showing old
values as if they were live.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from functools import partial
import logging
import time
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_interval
from span_panel_api import (
    ControlCommand,
    ControlInterceptor,
    FieldMetadata,
    PublishOutcome,
    PublishState,
    SpanPanelConnectionError,
    SpanPanelError,
    SpanPanelSnapshot,
)

from .clock import house_clock
from .hub import CloudSiteHub, PanelFeed, async_release_feed
from .snapshot import (
    PanelIdentity,
    build_snapshot,
    circuit_is_switchable,
    field_metadata,
    node_for_circuit,
    power_reading_key,
)
from .span_client.backend import RELAY_PROPERTY
from .span_client.cloud_history import Bucket, Resolution

_LOGGER = logging.getLogger(__name__)

# The integration's priority names to the values SPAN's snapshot carries (1/31 config #2.1).
_BACKUP_PRIORITY_BY_NAME = {"NEVER": 4, "SOC_THRESHOLD": 3, "OFF_GRID": 2}

# Over Ethernet the stream pushes a frame or two a second; over the panel's
# cellular backup, about one a minute. Two minutes of silence is past both.
STALE_AFTER_SECONDS = 120.0
_WATCHDOG_SECONDS = 10.0


class SpanCloudClient:
    """One panel, read and controlled through SPAN's cloud."""

    def __init__(
        self,
        hass: HomeAssistant,
        hub: CloudSiteHub,
        feed: PanelFeed,
        *,
        snapshot_interval: float,
    ) -> None:
        """Bind to one panel's feed; nothing is sent until streaming starts."""
        self._hass = hass
        self._hub = hub
        self._feed = feed
        self._identity = PanelIdentity(
            serial=feed.panel.serial or feed.panel.key, model=feed.panel.model
        )
        self._snapshot_interval = snapshot_interval
        self._snapshot_callbacks: list[Callable[[SpanPanelSnapshot], Awaitable[None]]] = []
        self._connection_callbacks: list[Callable[[bool], None]] = []
        self._fatal_error_callbacks: list[Callable[[SpanPanelError], None]] = []
        self._interceptor: ControlInterceptor | None = None
        self._streaming = False
        self._connected: bool | None = None
        self._unsub_watchdog: Callable[[], None] | None = None
        self._closed = False
        self._metadata: dict[str, FieldMetadata] | None = None
        feed.listeners.add(self._on_feed_update)
        # Every cloud panel is shown at the house clock's instant, built at its
        # tick, so two panels never show two different moments (see `clock`).
        self._clock = house_clock(hass)
        self._leave_clock = self._clock.join(self)

    # --- the house clock -----------------------------------------------------

    @property
    def watermark(self) -> float | None:
        """Return the newest SPAN frame time this panel has delivered."""
        return self._feed.watermark if self._feed.schema is not None else None

    @property
    def snapshot_interval(self) -> float:
        """Return how often this panel's snapshot is rebuilt, in seconds."""
        return self._snapshot_interval

    async def dispatch_at(self, at: float | None) -> None:
        """Push this panel's snapshot as it stood at `at`, at the clock's tick."""
        if not self._streaming:
            return
        try:
            snapshot = self._snapshot_at(at)
        except SpanPanelConnectionError:
            return
        for callback_ in list(self._snapshot_callbacks):
            await callback_(snapshot)

    # --- identity the entities read ----------------------------------------

    @property
    def serial_number(self) -> str:
        """Return the panel's serial, as the cloud's topology names it."""
        return self._identity.serial

    @property
    def schema_major(self) -> str | None:
        """No eBus schema generation: the cloud path has its own decoder."""
        return None

    @property
    def field_metadata(self) -> dict[str, FieldMetadata] | None:
        """Return what this transport produces, once the first snapshot is built."""
        return self._metadata

    # --- connection --------------------------------------------------------

    async def connect(self) -> None:
        """Wait for the panel's first description on the stream."""
        try:
            await asyncio.wait_for(self._feed.schema_ready.wait(), timeout=90)
        except TimeoutError as err:
            raise SpanPanelConnectionError(
                "no telemetry from SPAN's cloud within 90 seconds"
            ) from err

    async def close(self) -> None:
        """Stop streaming and let go of the panel's share of the site stream."""
        if self._closed:
            return
        self._closed = True
        self._leave_clock()
        await self.stop_streaming()
        self._feed.listeners.discard(self._on_feed_update)
        await async_release_feed(self._hass, self._hub, self._feed)

    @property
    def is_current(self) -> bool:
        """Return whether a reading has arrived recently enough to trust."""
        last = self._feed.last_reading
        return last is not None and time.monotonic() - last < STALE_AFTER_SECONDS

    # --- snapshot ----------------------------------------------------------

    async def get_snapshot(self) -> SpanPanelSnapshot:
        """Build the panel's snapshot at the house clock's current instant."""
        return self._snapshot_at(self._clock.now)

    def _snapshot_at(self, at: float | None) -> SpanPanelSnapshot:
        """Build the panel's snapshot from its readings as they stood at `at`."""
        feed = self._feed
        if feed.schema is None:
            raise SpanPanelConnectionError("SPAN's cloud has not described this panel yet")
        if feed.last_reading is not None and not self.is_current:
            raise SpanPanelConnectionError(
                f"no reading from SPAN's cloud for over {STALE_AFTER_SECONDS:.0f} seconds"
            )
        snapshot = build_snapshot(
            self._identity, feed.schema, feed.readings_at(at), feed.energy_total
        )
        if self._metadata is None:
            self._metadata = field_metadata(snapshot, frozenset(feed.schema.properties))
        return snapshot

    def recent_power(self, source: str, circuit_id: str | None = None) -> list[tuple[float, float]]:
        """Return the last minute of one power figure as `(SPAN frame time, W)`.

        `source` is `main_feed`, `feedthrough` or `circuit` (with its id). The
        frame time is SPAN's, so two panels on separate streams can be compared
        at the same instant rather than whenever each happened to arrive.
        """
        schema = self._feed.schema
        key = power_reading_key(schema, source, circuit_id) if schema is not None else None
        return self._feed.recent(key) if key is not None else []

    def set_snapshot_interval(self, interval: float) -> None:
        """Change how often pushes are batched into snapshots."""
        self._snapshot_interval = interval

    # --- streaming ---------------------------------------------------------

    def register_snapshot_callback(
        self, callback_: Callable[[SpanPanelSnapshot], Awaitable[None]]
    ) -> Callable[[], None]:
        """Subscribe to pushed snapshots; returns the unsubscribe."""
        self._snapshot_callbacks.append(callback_)
        return lambda: self._remove(self._snapshot_callbacks, callback_)

    def register_connection_callback(self, callback_: Callable[[bool], None]) -> Callable[[], None]:
        """Subscribe to current/stale edges; returns the unsubscribe."""
        self._connection_callbacks.append(callback_)
        return lambda: self._remove(self._connection_callbacks, callback_)

    def register_fatal_error_callback(
        self, callback_: Callable[[SpanPanelError], None]
    ) -> Callable[[], None]:
        """Subscribe to fatal transport errors; returns the unsubscribe."""
        self._fatal_error_callbacks.append(callback_)
        return lambda: self._remove(self._fatal_error_callbacks, callback_)

    def register_schema_change_callback(
        self, callback_: Callable[[str | None, str | None], None]
    ) -> Callable[[], None]:
        """Accept the subscription; the cloud has no schema generations."""
        return lambda: None

    def register_leaf_mismatch_callback(self, callback_: Callable[..., None]) -> Callable[[], None]:
        """Accept the subscription; the cloud path has no TLS leaf to check."""
        return lambda: None

    async def start_streaming(self) -> None:
        """Start pushing snapshots and watching for stale data."""
        self._streaming = True
        self._unsub_watchdog = async_track_time_interval(
            self._hass, self._check_current, timedelta(seconds=_WATCHDOG_SECONDS)
        )

    async def stop_streaming(self) -> None:
        """Stop pushing snapshots."""
        self._streaming = False
        if self._unsub_watchdog is not None:
            self._unsub_watchdog()
            self._unsub_watchdog = None

    @callback
    def _on_feed_update(self) -> None:
        if not self._streaming or self._feed.schema is None:
            return
        self._set_connected(True)
        self._clock.nudge()

    @callback
    def _check_current(self, _now: object = None) -> None:
        if self._feed.last_reading is not None:
            self._set_connected(self.is_current)

    @callback
    def _set_connected(self, connected: bool) -> None:
        if connected == self._connected:
            return
        if self._connected is not None:
            log = _LOGGER.info if connected else _LOGGER.warning
            log(
                "SPAN cloud data for %s is %s",
                self._identity.serial,
                "current again" if connected else f"stale (nothing for {STALE_AFTER_SECONDS:.0f}s)",
            )
        self._connected = connected
        for callback_ in list(self._connection_callbacks):
            callback_(connected)

    # --- control -----------------------------------------------------------

    def set_control_interceptor(self, interceptor: ControlInterceptor | None) -> None:
        """Install the gate every control command passes through."""
        self._interceptor = interceptor

    async def set_circuit_relay(self, circuit_id: str, state: str) -> PublishOutcome:
        """Open or close one circuit's relay through SPAN's cloud."""
        node = node_for_circuit(circuit_id)
        key = f"{node}/{RELAY_PROPERTY}"
        command = ControlCommand(
            device_id=self._identity.serial,
            node_id=node,
            property_id=RELAY_PROPERTY,
            value=state,
            # Not an MQTT topic: where the command goes, for the gate's audit line.
            topic=f"span-cloud/{self._identity.serial}/{key}",
        )
        if self._interceptor is not None:
            try:
                await self._interceptor.before_publish(command)
            except Exception:
                await self._after(
                    command, PublishOutcome(PublishState.FAILED, None, state, detail="vetoed")
                )
                raise
        if self._feed.schema is None or not circuit_is_switchable(self._feed.schema, circuit_id):
            outcome = PublishOutcome(
                PublishState.FAILED, key, state, detail="this breaker is not switchable"
            )
        elif self._feed.readings.get(key) == state:
            outcome = PublishOutcome(PublishState.CONFIRMED, key, state, no_op=True)
        else:
            try:
                await self._hass.async_add_executor_job(self._feed.backend.send_command, key, state)
            except Exception as err:  # noqa: BLE001 — reported as the outcome
                outcome = PublishOutcome(PublishState.FAILED, key, state, detail=str(err))
            else:
                # SendMessages succeeding means SPAN took the command, not that the
                # relay moved; the trait refresh a few seconds later says which.
                outcome = PublishOutcome(PublishState.ACCEPTED, key, state)
        await self._after(command, outcome)
        return outcome

    async def rename_circuit(self, circuit_id: str, name: str) -> str:
        """Rename one circuit on the panel through SPAN's cloud.

        Returns the name as sent. Raises `ValueError` for a name or circuit the
        panel cannot take, and whatever the call raises if SPAN rejects it; the
        caller reports either. Renaming moves nothing electrical, so it does not
        pass the switch gate.
        """
        return await self._hass.async_add_executor_job(
            self._feed.backend.rename_circuit, node_for_circuit(circuit_id), name
        )

    @property
    def serial(self) -> str:
        """The panel's serial number."""
        return self._identity.serial

    async def fetch_history(
        self, start_ms: int, end_ms: int, resolution: Resolution, time_zone: str
    ) -> tuple[dict[str, dict[int, Bucket]], dict[str, tuple[str, tuple[int, ...]]]]:
        """SPAN's metered energy over a range, and the names and spaces of the circuits.

        Raises whatever the call raises; the caller reports it.
        """
        backend = self._feed.backend
        history = await self._hass.async_add_executor_job(
            partial(backend.fetch_history, start_ms, end_ms, resolution, time_zone=time_zone)
        )
        return history, backend.circuit_labels()

    async def set_circuit_priority(self, circuit_id: str, priority: str) -> PublishOutcome:
        """Set the circuit's backup priority (`NEVER`, `SOC_THRESHOLD`, `OFF_GRID`)."""
        value = _BACKUP_PRIORITY_BY_NAME.get(priority)
        if value is None:
            return await self._unsupported(circuit_id, "shed-priority", priority)
        return await self._setting(
            circuit_id, "shed-priority", priority, self._feed.backend.set_backup_priority, value
        )

    async def set_circuit_always_on(self, circuit_id: str, always_on: bool) -> PublishOutcome:
        """Mark the circuit always on, so SPAN never sheds it, or hand it back."""
        return await self._setting(
            circuit_id,
            "always-on",
            "true" if always_on else "false",
            self._feed.backend.set_always_on,
            always_on,
        )

    async def set_circuit_powerup_rank(self, circuit_id: str, rank: int) -> PublishOutcome:
        """Set the circuit's place in PowerUp's order."""
        return await self._setting(
            circuit_id, "powerup-rank", str(rank), self._feed.backend.set_powerup_rank, rank
        )

    async def _setting(
        self,
        circuit_id: str,
        prop: str,
        shown: str,
        apply: Callable[[str, Any], None],
        value: Any,
    ) -> PublishOutcome:
        """Send one load-management setting through the gate, and report what the panel did.

        Unlike a relay command, the backend waits for the panel's own snapshot to
        show the new value, so success here is `CONFIRMED`, not `ACCEPTED`.
        """
        node = node_for_circuit(circuit_id)
        key = f"{node}/{prop}"
        command = ControlCommand(
            device_id=self._identity.serial,
            node_id=node,
            property_id=prop,
            value=shown,
            topic=f"span-cloud/{self._identity.serial}/{key}",
        )
        if self._interceptor is not None:
            try:
                await self._interceptor.before_publish(command)
            except Exception:
                await self._after(
                    command, PublishOutcome(PublishState.FAILED, None, shown, detail="vetoed")
                )
                raise
        try:
            await self._hass.async_add_executor_job(apply, node, value)
        except Exception as err:  # noqa: BLE001 — reported as the outcome
            outcome = PublishOutcome(PublishState.FAILED, key, shown, detail=str(err))
        else:
            outcome = PublishOutcome(PublishState.CONFIRMED, key, shown)
        await self._after(command, outcome)
        return outcome

    async def set_dominant_power_source(self, value: str) -> PublishOutcome:
        """Refuse: not writable over the cloud yet."""
        return await self._unsupported("core", "dominant-power-source", value)

    async def _unsupported(self, node: str, prop: str, value: str) -> PublishOutcome:
        command = ControlCommand(
            device_id=self._identity.serial, node_id=node, property_id=prop, value=value, topic=None
        )
        outcome = PublishOutcome(
            PublishState.FAILED, None, value, detail="not supported over SPAN's cloud yet"
        )
        await self._after(command, outcome)
        return outcome

    async def _after(self, command: ControlCommand, outcome: PublishOutcome) -> None:
        if self._interceptor is None:
            return
        try:
            await self._interceptor.after_publish(command, outcome)
        except Exception:  # noqa: BLE001 — an audit hook must not fail the command
            _LOGGER.exception("control interceptor failed after a cloud command")

    @staticmethod
    def _remove(items: list, item: object) -> None:
        if item in items:
            items.remove(item)
