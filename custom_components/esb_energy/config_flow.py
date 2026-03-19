"""
Config flow for ESB Energy integration.
"""

from __future__ import annotations

from pathlib import Path
import shutil
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components.file_upload import process_uploaded_file
from homeassistant.helpers import selector

from .const import (
    CONF_CSV_FILE,
    CONF_FETCH_INTERVAL,
    CONF_MPRN,
    CONF_PASSWORD,
    CONF_USERNAME,
    DOMAIN,
)
from .csv_utils import InvalidCsvFile, extract_mprn, merge_csv_content, validate_csv_header


class ESBEnergyConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for ESB Energy."""

    VERSION = 1

    def __init__(self):
        """Initialize config flow."""
        self._mprn = None
        self._csv_file = None

    async def async_step_user(self, user_input: dict[str, Any] | None = None):
        """Handle the initial step."""
        errors = {}

        if user_input is not None:
            # Validate input
            mprn = user_input.get(CONF_MPRN, "").strip()
            csv_file_input = _normalize_file_input(user_input.get(CONF_CSV_FILE))
            username = user_input.get(CONF_USERNAME, "").strip()
            password = user_input.get(CONF_PASSWORD, "")

            if mprn and (not mprn.isdigit() or len(mprn) != 11):
                errors[CONF_MPRN] = "invalid_mprn"

            if not csv_file_input and ((username and not password) or (password and not username)):
                errors[CONF_USERNAME] = "username_password_required"
                errors[CONF_PASSWORD] = "username_password_required"

            if not csv_file_input and (username or password) and not mprn:
                errors[CONF_MPRN] = "mprn_required_for_login"

            if not csv_file_input and not (username and password):
                errors["base"] = "csv_or_login_required"

            if not errors:
                csv_file = ""
                if csv_file_input:
                    try:
                        csv_file, csv_mprn = await self.hass.async_add_executor_job(
                            save_uploaded_csv_file,
                            self.hass,
                            csv_file_input,
                            _build_upload_path(self.hass, None),
                        )
                    except InvalidCsvFile:
                        errors[CONF_CSV_FILE] = "invalid_csv"
                    except FileNotFoundError:
                        errors[CONF_CSV_FILE] = "file_not_found"
                if not errors:
                    if not csv_file:
                        csv_file = str(_build_upload_path(self.hass, None))
                    if not mprn and csv_file_input:
                        mprn = csv_mprn or ""
                    title_suffix = mprn if mprn else "CSV"
                    return self.async_create_entry(
                        title=f"ESB Energy ({title_suffix})",
                        data={
                            CONF_MPRN: mprn,
                            CONF_CSV_FILE: csv_file,
                            CONF_USERNAME: username,
                            CONF_PASSWORD: password,
                        },
                        options={
                            CONF_FETCH_INTERVAL: int(
                                user_input.get(CONF_FETCH_INTERVAL, 24)
                            ),
                        },
                    )

        # Show the form
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_MPRN): str,
                    vol.Optional(CONF_USERNAME): str,
                    vol.Optional(CONF_PASSWORD): selector.TextSelector(
                        config=selector.TextSelectorConfig(
                            type=selector.TextSelectorType.PASSWORD
                        )
                    ),
                    vol.Optional(CONF_FETCH_INTERVAL, default=24): selector.NumberSelector(
                        config=selector.NumberSelectorConfig(
                            min=6,
                            max=168,
                            step=1,
                            mode=selector.NumberSelectorMode.BOX,
                            unit_of_measurement="h",
                        )
                    ),
                    vol.Optional(CONF_CSV_FILE): selector.FileSelector(
                        config=selector.FileSelectorConfig(accept=".csv")
                    ),
                }
            ),
            errors=errors,
        )

    async def async_step_import(self, user_input):
        """Handle import from configuration.yaml."""
        return await self.async_step_user(user_input)

    @staticmethod
    def async_get_options_flow(config_entry):
        """Get the options flow for this handler."""
        return ESBEnergyOptionsFlow()


class ESBEnergyOptionsFlow(config_entries.OptionsFlowWithReload):
    """Handle options flow for ESB Energy."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None):
        """Manage the options."""
        errors = {}

        if user_input is not None:
            csv_file_input = _normalize_file_input(user_input.get(CONF_CSV_FILE))
            mprn = user_input.get(CONF_MPRN, "").strip()
            username = user_input.get(CONF_USERNAME, "").strip()
            password = user_input.get(CONF_PASSWORD, "")

            if mprn and (not mprn.isdigit() or len(mprn) != 11):
                errors[CONF_MPRN] = "invalid_mprn"

            if not csv_file_input and ((username and not password) or (password and not username)):
                errors[CONF_USERNAME] = "username_password_required"
                errors[CONF_PASSWORD] = "username_password_required"

            if not csv_file_input and (username or password) and not mprn:
                errors[CONF_MPRN] = "mprn_required_for_login"

            if not csv_file_input and not (username and password):
                errors["base"] = "csv_or_login_required"

            if not errors:
                csv_file = ""
                if csv_file_input:
                    try:
                        csv_file, csv_mprn = await self.hass.async_add_executor_job(
                            save_uploaded_csv_file,
                            self.hass,
                            csv_file_input,
                            _build_upload_path(self.hass, self.config_entry.entry_id),
                        )
                    except InvalidCsvFile:
                        errors[CONF_CSV_FILE] = "invalid_csv"
                    except FileNotFoundError:
                        errors[CONF_CSV_FILE] = "file_not_found"
                if not errors:
                    if not csv_file:
                        csv_file = str(
                            _build_upload_path(self.hass, self.config_entry.entry_id)
                        )
                    if not mprn and csv_file_input:
                        mprn = csv_mprn or ""
                    self.hass.config_entries.async_update_entry(
                        self.config_entry,
                        data={
                            **self.config_entry.data,
                            CONF_MPRN: mprn,
                            CONF_USERNAME: username,
                            CONF_PASSWORD: password,
                        },
                    )
                    return self.async_create_entry(
                        title="",
                        data={
                            CONF_CSV_FILE: csv_file,
                            CONF_FETCH_INTERVAL: int(
                                user_input.get(CONF_FETCH_INTERVAL, 24)
                            ),
                        },
                    )

        current_interval = self.config_entry.options.get(
            CONF_FETCH_INTERVAL, self.config_entry.data.get(CONF_FETCH_INTERVAL, 24)
        )
        current_username = self.config_entry.options.get(
            CONF_USERNAME, self.config_entry.data.get(CONF_USERNAME, "")
        )
        current_mprn = self.config_entry.options.get(
            CONF_MPRN, self.config_entry.data.get(CONF_MPRN, "")
        )
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_CSV_FILE): selector.FileSelector(
                        config=selector.FileSelectorConfig(accept=".csv")
                    ),
                    vol.Optional(CONF_MPRN, default=current_mprn): str,
                    vol.Optional(CONF_USERNAME, default=current_username): str,
                    vol.Optional(CONF_PASSWORD): selector.TextSelector(
                        config=selector.TextSelectorConfig(
                            type=selector.TextSelectorType.PASSWORD
                        )
                    ),
                    vol.Optional(CONF_FETCH_INTERVAL, default=current_interval): selector.NumberSelector(
                        config=selector.NumberSelectorConfig(
                            min=6,
                            max=168,
                            step=1,
                            mode=selector.NumberSelectorMode.BOX,
                            unit_of_measurement="h",
                        )
                    ),
                }
            ),
            errors=errors,
        )


def _normalize_file_input(value: Any) -> str:
    """Extract a file id or path from the selector value."""
    if isinstance(value, dict):
        value = value.get("file_id") or value.get("path") or ""
    return (value or "").strip()


def _build_upload_path(hass, entry_id: str | None) -> Path:
    """Build a target path for the uploaded CSV file."""
    base_dir = Path(hass.config.path("esb_energy", "uploads"))
    return base_dir / "esb_energy.csv"


def save_uploaded_csv_file(
    hass, uploaded_file_id: str, target_path: Path
) -> tuple[str, str | None]:
    """Validate the uploaded CSV file and move it to the config directory."""
    if Path(uploaded_file_id).exists():
        content = Path(uploaded_file_id).read_text(encoding="utf-8-sig")
        return str(uploaded_file_id), extract_mprn(content)

    try:
        with process_uploaded_file(hass, uploaded_file_id) as file:
            content = file.read_text(encoding="utf-8-sig")
            validate_csv_header(content)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            merge_csv_content(target_path, content)
            return str(target_path), extract_mprn(content)
    except ValueError as err:
        raise FileNotFoundError("Uploaded file not found") from err
