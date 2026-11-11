"""One panel, two connections: local (SPAN API on the panel) and cloud (SPAN's servers).

A panel is one physical thing with up to two independent ways in. Each has its
own config entry and its own settings, and neither may change the other:

- **Local** (`api_version` `v2`): the panel's own MQTT broker and REST API on the
  home network. Its entry's unique id is the panel serial, as upstream sets it.
- **Cloud** (`api_version` `cloud`): the stream and services SPAN's app uses. Its
  entry's unique id is the serial behind `CLOUD_UNIQUE_ID_PREFIX`, so adding the
  same panel locally is never "already configured" and never rewrites the cloud
  entry's data.

Ownership of the panel's device and entities is the one thing they share, and
the rule is simple: while a local entry for the panel is enabled, the local
connection owns them and the cloud entry stands by — it keeps its settings and
its account, sets up no entities and opens no stream. Disable or remove the
local entry and the cloud entry takes the panel again on its next setup.

Serials are matched without regard to case: the panel reports its serial in
lower case over SPAN API, and SPAN's cloud may spell the same serial in capitals.
"""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback

from .const import API_VERSION_CLOUD, CONF_API_VERSION, DOMAIN

CLOUD_UNIQUE_ID_PREFIX = "cloud:"


def serial_key(serial: str | None) -> str | None:
    """Return the form two spellings of one serial share, or None for no serial."""
    if serial is None:
        return None
    key = serial.strip().casefold()
    return key or None


def cloud_unique_id(serial: str) -> str:
    """Return the unique id of a panel's cloud entry."""
    return f"{CLOUD_UNIQUE_ID_PREFIX}{serial}"


def is_cloud_entry(entry: ConfigEntry) -> bool:
    """Whether `entry` is a cloud connection."""
    return entry.data.get(CONF_API_VERSION) == API_VERSION_CLOUD


def entry_serial(entry: ConfigEntry) -> str | None:
    """Return the panel serial an entry stands for, whichever connection it is."""
    unique_id = entry.unique_id
    if not unique_id:
        return None
    if unique_id.startswith(CLOUD_UNIQUE_ID_PREFIX):
        return unique_id[len(CLOUD_UNIQUE_ID_PREFIX) :] or None
    return unique_id


def local_owner(hass: HomeAssistant, serial: str | None) -> ConfigEntry | None:
    """Return the enabled local entry that owns the panel `serial`, if any."""
    key = serial_key(serial)
    if key is None:
        return None
    for entry in hass.config_entries.async_entries(DOMAIN):
        if is_cloud_entry(entry) or entry.disabled_by is not None:
            continue
        if serial_key(entry_serial(entry)) == key:
            return entry
    return None


def cloud_entry_for(hass: HomeAssistant, serial: str | None) -> ConfigEntry | None:
    """Return the cloud entry for the panel `serial`, enabled or not, if any."""
    key = serial_key(serial)
    if key is None:
        return None
    for entry in hass.config_entries.async_entries(DOMAIN):
        if is_cloud_entry(entry) and serial_key(entry_serial(entry)) == key:
            return entry
    return None


@callback
def async_namespace_cloud_entries(hass: HomeAssistant) -> None:
    """Give every cloud entry from before the split its own unique id.

    A cloud entry used to carry the bare serial, which made adding the same
    panel locally abort as already configured or, on the host-update path,
    write the local host into the cloud entry. Disabled entries are renamed
    too: they hold their unique id just the same.
    """
    for entry in hass.config_entries.async_entries(DOMAIN):
        if not is_cloud_entry(entry) or not entry.unique_id:
            continue
        if entry.unique_id.startswith(CLOUD_UNIQUE_ID_PREFIX):
            continue
        hass.config_entries.async_update_entry(entry, unique_id=cloud_unique_id(entry.unique_id))
