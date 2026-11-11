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

from homeassistant.config_entries import ConfigFlowResult, ConfigSubentryFlow, SubentryFlowResult
from homeassistant.const import CONF_EMAIL, CONF_PASSWORD
from homeassistant.core import HomeAssistant
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)
import voluptuous as vol

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
from custom_components.span_panel.runtime import loaded_runtime_data

from .link import (
    CONF_RESERVED_BREAKERS,
    CloudPanelMismatch,
    async_open_cloud_panel,
    cloud_account,
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


async def _async_sign_in(
    hass: HomeAssistant, email: str, password: str
) -> tuple[dict[str, Any], str, dict[str, PanelInfo]] | str:
    """Sign in and list the account's panels, or name the form error to show."""
    from .span_client import cloud_auth  # noqa: PLC0415

    try:
        tokens, user_id, sites = await hass.async_add_executor_job(
            _sign_in, email.strip(), password
        )
    except cloud_auth.CloudCredentialsRejected:
        return "invalid_auth"
    except Exception:  # noqa: BLE001 — shown as a form error, logged for the curious
        _LOGGER.exception("SPAN cloud sign-in failed")
        return "cannot_connect"
    return tokens, user_id, _choices(sites)


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
        errors: dict[str, str] = {}
        if user_input is not None:
            signed_in = await _async_sign_in(
                self.hass, user_input[CONF_EMAIL], user_input[CONF_PASSWORD]
            )
            if isinstance(signed_in, str):
                errors["base"] = signed_in
            else:
                tokens, user_id, panels = signed_in
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
        configured = {entry.unique_id for entry in self.hass.config_entries.async_entries(DOMAIN)}
        available = {
            key: panel
            for key, panel in self._cloud_panels.items()
            if panel.serial not in configured
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
            "unique_id": panel.serial,
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


class CloudAccountSubentryFlow(ConfigSubentryFlow):
    """Link a SPAN account to a panel read locally (see `link`).

    Sign in, and the account's panel that is this one is found for you. The two
    sides give one panel different serials, so it is found by what they report
    identically: the breaker spaces its circuits occupy. A panel already linked
    to another entry is not considered. Where several fit, the one naming the
    most circuits alike is taken, and only a tie is asked about. Only the tokens SPAN issues are kept.
    """

    _matches: dict[str, dict[str, Any]]
    _titles: dict[str, str]
    _names_in_common: dict[str, int]

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> SubentryFlowResult:
        """Sign in to the SPAN account this panel belongs to."""
        if cloud_account(self._get_entry()) is not None:
            return self.async_abort(reason="already_linked")
        errors: dict[str, str] = {}
        if user_input is not None:
            signed_in = await _async_sign_in(
                self.hass, user_input[CONF_EMAIL], user_input[CONF_PASSWORD]
            )
            if isinstance(signed_in, str):
                errors["base"] = signed_in
            else:
                return await self._async_find_panel(*signed_in)
        return self.async_show_form(step_id="user", data_schema=CLOUD_LOGIN_SCHEMA, errors=errors)

    def _linked_elsewhere(self) -> set[str]:
        """Return the account panels other entries are already linked to."""
        own = self._get_entry().entry_id
        return {
            account.data[CONF_CLOUD_PANEL_KEY]
            for entry in self.hass.config_entries.async_entries(DOMAIN)
            if entry.entry_id != own and (account := cloud_account(entry)) is not None
        }

    async def _async_find_panel(
        self, tokens: dict[str, Any], user_id: str, panels: dict[str, PanelInfo]
    ) -> SubentryFlowResult:
        """Find which of the account's panels this entry's panel is, and link it."""
        if not panels:
            return self.async_abort(reason="no_panels")
        taken = self._linked_elsewhere()
        candidates = {key: panel for key, panel in panels.items() if key not in taken}
        if not candidates:
            return self.async_abort(reason="all_linked")

        self._matches, self._titles, self._names_in_common = {}, {}, {}
        errors: set[str] = set()
        for key, panel in candidates.items():
            data: dict[str, Any] = {
                CONF_CLOUD_TOKENS: tokens,
                CONF_CLOUD_USER_ID: user_id,
                CONF_CLOUD_DEVICE_UUID: str(uuid.uuid4()).upper(),
                CONF_CLOUD_PANEL_KEY: panel.key,
                CONF_CLOUD_MODEL: panel.model,
            }
            error = await self._async_read_panel(data)
            if error is None:
                self._matches[key] = data
                self._titles[key] = panel_title(panel).removeprefix("SPAN ")
            else:
                errors.add(error)

        if len(self._matches) > 1:
            # One breaker layout on several of the account's panels: the one
            # that also names the most circuits as this panel does is it.
            best = max(self._names_in_common.values())
            closest = [key for key in self._matches if self._names_in_common[key] == best]
            if len(closest) == 1:
                return self._link(closest[0])
        if len(self._matches) == 1:
            return self._link(next(iter(self._matches)))
        if self._matches:
            return await self.async_step_panel()
        # Why nothing fit, most actionable first: the local panel is not
        # running, SPAN could not be reached, or no panel has these breakers.
        for reason in ("panel_not_loaded", "cannot_connect"):
            if reason in errors:
                return self.async_abort(reason=reason)
        return self.async_abort(
            reason="no_matching_panel",
            description_placeholders={"panel": self._get_entry().title},
        )

    def _link(self, key: str) -> SubentryFlowResult:
        return self.async_create_entry(
            title=self._titles[key], data=self._matches[key], unique_id=key
        )

    async def async_step_panel(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Choose among the account's panels that all have this panel's breakers."""
        if user_input is not None:
            return self._link(user_input[CONF_CLOUD_PANEL_KEY])
        options = [SelectOptionDict(value=key, label=title) for key, title in self._titles.items()]
        schema = vol.Schema(
            {
                vol.Required(CONF_CLOUD_PANEL_KEY): SelectSelector(
                    SelectSelectorConfig(options=options)
                )
            }
        )
        return self.async_show_form(
            step_id="panel",
            data_schema=schema,
            description_placeholders={"panel": self._get_entry().title},
        )

    async def _async_read_panel(self, data: dict[str, Any]) -> str | None:
        """Check whether an account panel is this one, and keep what only the cloud says about it.

        Matched by breaker spaces against the panel as SPAN API reads it now.
        The breakers SPAN API leaves out are read
        while the account's stream is open and kept with the link. Returns an
        error key, or None.
        """
        entry = self._get_entry()
        runtime = loaded_runtime_data(entry)
        local = runtime.coordinator.data if runtime is not None else None
        if local is None:
            return "panel_not_loaded"
        try:
            async with async_open_cloud_panel(self.hass, entry, data, local) as linked:
                data[CONF_RESERVED_BREAKERS] = linked.reserved_breakers()
                self._names_in_common[data[CONF_CLOUD_PANEL_KEY]] = linked.names_in_common()
        except CloudPanelMismatch:
            return "wrong_panel"
        except Exception:  # noqa: BLE001 -- any failure to reach the panel is reported the same
            _LOGGER.debug("Could not read the SPAN account's panel", exc_info=True)
            return "cannot_connect"
        return None
