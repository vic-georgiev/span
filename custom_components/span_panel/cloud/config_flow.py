"""Config-flow steps for adding panels through SPAN's cloud.

Reached from the user step's "Use SPAN cloud" box. Signs in once, lists the
panels on the account, and makes one entry per panel picked, so the rest of
the integration sees exactly the per-panel entries it is built around. The
password is used only to sign in; what is stored is the token set SPAN issued.
"""

from __future__ import annotations

from dataclasses import asdict
import logging
from typing import TYPE_CHECKING, Any
import uuid

from homeassistant.config_entries import ConfigFlowResult
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)
import voluptuous as vol

from custom_components.span_panel.connections import (
    cloud_unique_id,
    entry_serial,
    is_cloud_entry,
    serial_key,
)
from custom_components.span_panel.const import (
    API_VERSION_CLOUD,
    CONF_API_VERSION,
    CONF_CLOUD_DEVICE_UUID,
    CONF_CLOUD_MODEL,
    CONF_CLOUD_PANEL_KEY,
    CONF_CLOUD_TOKENS,
    CONF_CLOUD_USER_ID,
    CONF_DEVICE_NAME,
    DOMAIN,
    ENABLE_ENERGY_DIP_COMPENSATION,
    USE_CIRCUIT_NUMBERS,
    USE_DEVICE_PREFIX,
)
from custom_components.span_panel.control_gate import DEFAULT_ALLOW_CONTEXTLESS_CONTROL
from custom_components.span_panel.options import (
    ALLOW_CONTEXTLESS_CONTROL,
    ENERGY_DISPLAY_PRECISION,
    POWER_DISPLAY_PRECISION,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigFlow

    from .span_client.backend import PanelInfo, SiteInfo

    _FlowBase = ConfigFlow
else:
    _FlowBase = object

_LOGGER = logging.getLogger(__name__)

CONF_USE_SPAN_CLOUD = "use_span_cloud"
CONF_CLOUD_PANELS = "panels"
CLOUD_IMPORT_SOURCE = "cloud_import"

CLOUD_LOGIN_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_EMAIL): TextSelector(TextSelectorConfig(type=TextSelectorType.EMAIL)),
        vol.Required(CONF_PASSWORD): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
)


def _sign_in(email: str, password: str) -> tuple[dict[str, Any], str, list[SiteInfo]]:
    """Executor: sign in, and read the account's sites with the tokens it gave."""
    from .span_client import cloud_auth  # noqa: PLC0415
    from .span_client.backend import discover_sites  # noqa: PLC0415

    tokens = cloud_auth.authenticate(email, password)
    user_id = cloud_auth.user_id_from_token(tokens.access_token)
    if not user_id:
        raise cloud_auth.CloudAuthError("the SPAN token carries no user id")
    sites = discover_sites(tokens.access_token)
    return asdict(tokens), user_id, sites


def _choices(sites: list[SiteInfo]) -> dict[str, PanelInfo]:
    from .span_client.backend import site_choices  # noqa: PLC0415

    return {panel.key: panel for site in sites for panel in site_choices(site) if panel.serial}


def panel_title(panel: PanelInfo) -> str:
    """Name a panel the way its device will be named."""
    return f"SPAN {panel.model} {panel.serial}"


class CloudFlowSteps(_FlowBase):
    """Mixed into the integration's config flow, which supplies `hass` and the flow API."""

    _cloud_tokens: dict[str, Any] | None = None
    _cloud_user_id: str | None = None
    _cloud_panels: dict[str, PanelInfo] | None = None

    async def async_step_cloud_login(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Sign in to the SPAN account the panels belong to."""
        from .span_client import cloud_auth  # noqa: PLC0415

        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                tokens, user_id, sites = await self.hass.async_add_executor_job(
                    _sign_in, user_input[CONF_EMAIL].strip(), user_input[CONF_PASSWORD]
                )
            except cloud_auth.CloudCredentialsRejected:
                errors["base"] = "invalid_auth"
            except Exception:  # noqa: BLE001 — shown as a form error, logged for the curious
                _LOGGER.exception("SPAN cloud sign-in failed")
                errors["base"] = "cannot_connect"
            else:
                panels = _choices(sites)
                if not panels:
                    return self.async_abort(reason="no_devices_found")
                self._cloud_tokens, self._cloud_user_id, self._cloud_panels = (
                    tokens,
                    user_id,
                    panels,
                )
                return await self.async_step_cloud_panels()

        return self.async_show_form(
            step_id="cloud_login", data_schema=CLOUD_LOGIN_SCHEMA, errors=errors
        )

    async def async_step_cloud_panels(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Pick the panels to add. Each becomes its own entry."""
        assert self._cloud_panels is not None
        # Only another cloud entry makes a panel "configured" here: a local
        # entry for the same panel is a separate connection and leaves the
        # cloud one free to add (see `connections`).
        configured = {
            serial_key(entry_serial(entry))
            for entry in self.hass.config_entries.async_entries(DOMAIN)
            if is_cloud_entry(entry)
        }
        available = {
            key: panel
            for key, panel in self._cloud_panels.items()
            if serial_key(panel.serial) not in configured
        }
        if not available:
            return self.async_abort(reason="already_configured")

        if user_input is None:
            options = [
                SelectOptionDict(value=key, label=panel_title(panel).removeprefix("SPAN "))
                for key, panel in available.items()
            ]
            schema = vol.Schema(
                {
                    vol.Required(CONF_CLOUD_PANELS, default=list(available)): SelectSelector(
                        SelectSelectorConfig(options=options, multiple=True)
                    )
                }
            )
            return self.async_show_form(step_id="cloud_panels", data_schema=schema)

        picked = [key for key in user_input[CONF_CLOUD_PANELS] if key in available]
        if not picked:
            return self.async_abort(reason="no_devices_found")
        # One device UUID for everything this sign-in adds, so panels on one site
        # agree on the stream they share whichever entry starts it.
        device_uuid = str(uuid.uuid4()).upper()
        entries = [self._cloud_entry(available[key], device_uuid) for key in picked]
        for extra in entries[1:]:
            self.hass.async_create_task(
                self.hass.config_entries.flow.async_init(
                    DOMAIN, context={"source": CLOUD_IMPORT_SOURCE}, data=extra
                )
            )
        return await self._async_create_cloud_entry(entries[0])

    async def async_step_cloud_import(self, data: dict[str, Any]) -> ConfigFlowResult:
        """Create the entry for a further panel picked in the same sign-in."""
        return await self._async_create_cloud_entry(data)

    def _cloud_entry(self, panel: PanelInfo, device_uuid: str) -> dict[str, Any]:
        title = panel_title(panel)
        return {
            "title": title,
            "unique_id": cloud_unique_id(panel.serial or panel.key),
            "data": {
                CONF_API_VERSION: API_VERSION_CLOUD,
                CONF_DEVICE_NAME: title,
                CONF_CLOUD_TOKENS: self._cloud_tokens,
                CONF_CLOUD_USER_ID: self._cloud_user_id,
                CONF_CLOUD_DEVICE_UUID: device_uuid,
                CONF_CLOUD_PANEL_KEY: panel.key,
                CONF_CLOUD_MODEL: panel.model,
            },
            "options": {
                USE_DEVICE_PREFIX: True,
                USE_CIRCUIT_NUMBERS: False,
                POWER_DISPLAY_PRECISION: getattr(self, "power_display_precision", 0),
                ENERGY_DISPLAY_PRECISION: getattr(self, "energy_display_precision", 2),
                ENABLE_ENERGY_DIP_COMPENSATION: getattr(self, "_enable_dip_compensation", True),
                ALLOW_CONTEXTLESS_CONTROL: DEFAULT_ALLOW_CONTEXTLESS_CONTROL,
            },
        }

    async def _async_create_cloud_entry(self, spec: dict[str, Any]) -> ConfigFlowResult:
        await self.async_set_unique_id(spec["unique_id"])
        self._abort_if_unique_id_configured()
        return self.async_create_entry(
            title=spec["title"], data=spec["data"], options=spec["options"]
        )
