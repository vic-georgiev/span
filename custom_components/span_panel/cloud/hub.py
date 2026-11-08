"""One SPAN site's cloud stream, shared by the config entries of its panels.

This integration makes one config entry per panel. The cloud makes one stream
per *site*, and two panels on a site share it: circuit instance ids restart per
panel, so the vendored `SiteStream` splits the site's frames between them. A
hub per site holds that stream and hands each entry its panel's share.

The hub starts a feed for every panel on the site, whether or not it has an
entry, because `SiteStream` only starts once every panel it routes has
attached; a panel nobody added is simply read and ignored.

Threads: the vendored backends call back on their own threads. Everything here
crosses to the event loop before touching state, so `PanelFeed` is only ever
read and written on the loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
import logging
from pathlib import Path
import threading
import time
from typing import Any

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_call_later, async_track_time_interval
from homeassistant.helpers.storage import Store

from custom_components.span_panel.const import DOMAIN

from .energy import EnergyLedger
from .span_client import cloud_auth
from .span_client.backend import (
    CloudBackend,
    PanelInfo,
    SiteInfo,
    SiteStream,
    discover_sites,
    find_panel,
    site_device_uuid,
    site_routes,
)
from .span_client.models import EnergySample, PanelSchema, Reading

_LOGGER = logging.getLogger(__name__)

HUBS_KEY = "cloud_hubs"
HUBS_LOCK_KEY = "cloud_hubs_lock"
TOKEN_DIR = ".storage/span_panel_cloud"
ENERGY_STORE_VERSION = 1
ENERGY_POLL_SECONDS = 60
FIRST_ENERGY_POLL_SECONDS = 10
ENERGY_SAVE_DELAY_SECONDS = 30

type FeedListener = Callable[[], None]


class CloudAuthFailed(Exception):
    """SPAN rejected the stored credentials; the user has to sign in again."""


def token_path(hass: HomeAssistant, user_id: str) -> Path:
    """Where an account's tokens live. One file per account: refreshes rewrite it."""
    return Path(hass.config.path(TOKEN_DIR)) / f"{user_id}.json"


def write_tokens_if_missing(path: Path, tokens: dict[str, Any]) -> None:
    """Executor: seed the token store from the entry, unless a fresher one is there.

    The store is rewritten on every refresh, so after the first setup it holds
    newer tokens than the entry does and must not be overwritten with them.
    """
    if path.exists():
        return
    cloud_auth.save_tokens(path, cloud_auth.CloudTokens(**tokens))


def discover(path: Path) -> list[SiteInfo]:
    """Executor: prove the credentials and read the account's sites."""
    try:
        return discover_sites(cloud_auth.access_token_from_store(path))
    except cloud_auth.CloudCredentialsRejected as err:
        raise CloudAuthFailed(str(err)) from err


@dataclass
class PanelFeed:
    """One panel's live state as the stream reports it. Loop-thread only."""

    panel: PanelInfo
    backend: CloudBackend
    schema: PanelSchema | None = None
    readings: dict[str, str] = field(default_factory=dict)
    # Monotonic time of the last reading: what "is this current" is judged by.
    last_reading: float | None = None
    ledger: EnergyLedger = field(default_factory=EnergyLedger)
    listeners: set[FeedListener] = field(default_factory=set)
    schema_ready: asyncio.Event = field(default_factory=asyncio.Event)
    users: int = 0
    store: Store[dict[str, Any]] | None = None

    def energy_total(self, key: str) -> float | None:
        """Return the metered kWh total for a property key."""
        return self.ledger.total(key)

    @callback
    def notify(self) -> None:
        """Tell this feed's clients something changed."""
        for listener in list(self.listeners):
            listener()


class CloudSiteHub:
    """The stream for one site, and a feed per panel on it."""

    def __init__(
        self,
        hass: HomeAssistant,
        site: SiteInfo,
        *,
        tokens: Path,
        device_uuid: str,
        user_id: str | None,
    ) -> None:
        """Plan the site's stream: one backend, or a split per panel."""
        self.hass = hass
        self.site = site
        self.feeds: dict[str, PanelFeed] = {}
        self._buffer: dict[str, dict[str, Reading]] = {}
        self._buffer_lock = threading.Lock()
        self._flush_scheduled = False
        self._energy_unsub: Callable[[], None] | None = None
        self._first_poll_unsub: Callable[[], None] | None = None
        self._energy_failures = 0
        self._auth_failed_listeners: set[Callable[[], None]] = set()

        stream_uuid = site_device_uuid(device_uuid, site.site_id)
        # The lead panel takes the site's own block and flows (grid, home ...),
        # so they reach a panel this integration has a device for.
        routes = site_routes(site, site.lead.key)
        if routes is None:
            backend = CloudBackend(
                tokens,
                stream_uuid,
                user_id=user_id,
                serial=site.lead.serial,
                site_id=site.site_id,
                on_auth_failed=self._on_auth_failed,
            )
            self.feeds[site.lead.key] = PanelFeed(site.lead, backend)
        else:
            stream = SiteStream(
                tokens, stream_uuid, routes=routes, user_id=user_id, serial=site.lead.serial
            )
            for panel in routes.panels:
                backend = stream.panel_backend(
                    panel.key, panel.serial, on_auth_failed=self._on_auth_failed
                )
                self.feeds[panel.key] = PanelFeed(panel, backend)

    # --- lifecycle ---------------------------------------------------------

    async def async_start(self) -> None:
        """Load each panel's energy totals and start the stream."""
        for key, feed in self.feeds.items():
            feed.store = Store(self.hass, ENERGY_STORE_VERSION, f"{DOMAIN}.cloud.{key}.energy")
            feed.ledger.load(await feed.store.async_load())
        for key, feed in self.feeds.items():
            feed.backend.start(
                lambda schema, key=key: self._on_schema(key, schema),
                lambda reading, key=key: self._on_reading(key, reading),
            )
        self._energy_unsub = async_track_time_interval(
            self.hass, self._async_poll_energy, timedelta(seconds=ENERGY_POLL_SECONDS)
        )
        # The first poll soon after start rather than a full interval in, once the
        # frames have named what to ask about, so energy is not blank for a minute.
        self._first_poll_unsub = async_call_later(
            self.hass, FIRST_ENERGY_POLL_SECONDS, self._async_poll_energy
        )

    async def async_stop(self) -> None:
        """Stop the stream and save each panel's energy totals."""
        for unsub in (self._energy_unsub, self._first_poll_unsub):
            if unsub is not None:
                unsub()
        self._energy_unsub = self._first_poll_unsub = None
        for feed in self.feeds.values():
            await self.hass.async_add_executor_job(feed.backend.stop, 2.0)
            if feed.store is not None:
                await feed.store.async_save(feed.ledger.as_dict())

    def feed_for(self, panel_key: str) -> PanelFeed | None:
        """Return the feed for a panel key, if the site has that panel."""
        return next(
            (feed for feed in self.feeds.values() if feed.panel.answers_to(panel_key)), None
        )

    def add_auth_failed_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Call `listener` if SPAN rejects the credentials."""
        self._auth_failed_listeners.add(listener)
        return lambda: self._auth_failed_listeners.discard(listener)

    # --- backend callbacks (backend threads) -------------------------------

    def _on_auth_failed(self, *_args: Any) -> None:
        self.hass.loop.call_soon_threadsafe(self._notify_auth_failed)

    def _on_schema(self, key: str, schema: PanelSchema) -> None:
        self.hass.loop.call_soon_threadsafe(self._apply_schema, key, schema)

    def _on_reading(self, key: str, reading: Reading) -> None:
        with self._buffer_lock:
            self._buffer.setdefault(key, {})[reading.key] = reading
            if not self._flush_scheduled:
                self._flush_scheduled = True
                self.hass.loop.call_soon_threadsafe(self._flush)

    # --- loop thread -------------------------------------------------------

    @callback
    def _notify_auth_failed(self) -> None:
        for listener in list(self._auth_failed_listeners):
            listener()

    @callback
    def _apply_schema(self, key: str, schema: PanelSchema) -> None:
        feed = self.feeds[key]
        feed.schema = schema
        feed.schema_ready.set()
        feed.notify()

    @callback
    def _flush(self) -> None:
        with self._buffer_lock:
            buffered, self._buffer = self._buffer, {}
            self._flush_scheduled = False
        now = time.monotonic()
        for key, readings in buffered.items():
            feed = self.feeds[key]
            for reading in readings.values():
                feed.readings[reading.key] = reading.value
            feed.last_reading = now
            if feed.schema is not None:
                feed.notify()

    async def _async_poll_energy(self, _now: Any = None) -> None:
        """Advance every panel's metered energy from SPAN's history."""
        zone = self.hass.config.time_zone or "UTC"
        for feed in self.feeds.values():
            if not feed.users:
                continue
            try:
                samples: list[EnergySample] = await self.hass.async_add_executor_job(
                    lambda feed=feed: feed.backend.fetch_energy(
                        feed.ledger.covered_through_ms, time_zone=zone
                    )
                )
            except Exception as err:  # noqa: BLE001 — one bad poll must not stop the timer
                self._energy_failures += 1
                log = _LOGGER.warning if self._energy_failures == 1 else _LOGGER.debug
                log("could not read SPAN energy (attempt %d): %s", self._energy_failures, err)
                continue
            self._energy_failures = 0
            if not samples:
                continue
            # Notified even when no total moved: a key's first bucket only sets its
            # mark, and that is the moment its total goes from unknown to 0.
            feed.ledger.apply(samples)
            if feed.store is not None:
                feed.store.async_delay_save(feed.ledger.as_dict, ENERGY_SAVE_DELAY_SECONDS)
            feed.notify()


async def async_acquire_feed(
    hass: HomeAssistant,
    *,
    tokens: dict[str, Any],
    user_id: str,
    device_uuid: str,
    panel_key: str,
) -> tuple[CloudSiteHub, PanelFeed]:
    """Return the hub for this panel's site, starting it if need be, and its feed.

    Serialised so two panels on one site set up at once share one hub rather
    than opening two streams.
    """
    domain_data = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault(HUBS_LOCK_KEY, asyncio.Lock())
    hubs: dict[str, CloudSiteHub] = domain_data.setdefault(HUBS_KEY, {})
    async with lock:
        for hub in hubs.values():
            feed = hub.feed_for(panel_key)
            if feed is not None:
                feed.users += 1
                return hub, feed

        path = token_path(hass, user_id)
        await hass.async_add_executor_job(write_tokens_if_missing, path, tokens)
        sites = await hass.async_add_executor_job(discover, path)
        found = find_panel(sites, panel_key)
        if found is None:
            raise LookupError(f"panel {panel_key} is not on this SPAN account")
        site, _panel = found
        hub = CloudSiteHub(hass, site, tokens=path, device_uuid=device_uuid, user_id=user_id)
        feed = hub.feed_for(panel_key)
        if feed is None:
            raise LookupError(f"panel {panel_key} has no share of its site's stream")
        await hub.async_start()
        hubs[site.site_id] = hub
        feed.users += 1
        return hub, feed


async def async_release_feed(hass: HomeAssistant, hub: CloudSiteHub, feed: PanelFeed) -> None:
    """Let go of a feed; the last one out stops the site's stream."""
    domain_data = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault(HUBS_LOCK_KEY, asyncio.Lock())
    hubs: dict[str, CloudSiteHub] = domain_data.setdefault(HUBS_KEY, {})
    async with lock:
        feed.users = max(0, feed.users - 1)
        if any(f.users for f in hub.feeds.values()):
            return
        hubs.pop(hub.site.site_id, None)
        await hub.async_stop()
