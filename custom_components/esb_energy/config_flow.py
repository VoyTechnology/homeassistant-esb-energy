"""
Config flow for ESB Energy integration.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import csv
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.components.file_upload import process_uploaded_file
from homeassistant.helpers import selector

from .const import CONF_CSV_FILE, CONF_MPRN, DOMAIN


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

            if mprn and (not mprn.isdigit() or len(mprn) != 11):
                errors[CONF_MPRN] = "invalid_mprn"

            if not csv_file_input:
                errors[CONF_CSV_FILE] = "csv_required"

            if not errors:
                try:
                    csv_file = await self.hass.async_add_executor_job(
                        save_uploaded_csv_file,
                        self.hass,
                        csv_file_input,
                        _build_upload_path(self.hass, None),
                    )
                except InvalidCsvFile:
                    errors[CONF_CSV_FILE] = "invalid_csv"
                except FileNotFoundError:
                    errors[CONF_CSV_FILE] = "file_not_found"
                else:
                    title_suffix = mprn if mprn else "CSV"
                    return self.async_create_entry(
                        title=f"ESB Energy ({title_suffix})",
                        data={
                            CONF_MPRN: mprn,
                            CONF_CSV_FILE: csv_file,
                        },
                    )

        # Show the form
        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_MPRN): str,
                    vol.Required(CONF_CSV_FILE): selector.FileSelector(
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
            if not csv_file_input:
                errors[CONF_CSV_FILE] = "csv_required"
            else:
                try:
                    csv_file = await self.hass.async_add_executor_job(
                        save_uploaded_csv_file,
                        self.hass,
                        csv_file_input,
                        _build_upload_path(self.hass, self.config_entry.entry_id),
                    )
                except InvalidCsvFile:
                    errors[CONF_CSV_FILE] = "invalid_csv"
                except FileNotFoundError:
                    errors[CONF_CSV_FILE] = "file_not_found"
                else:
                    return self.async_create_entry(
                        title="",
                        data={
                            CONF_CSV_FILE: csv_file,
                        },
                    )

        current = self.config_entry.options.get(
            CONF_CSV_FILE, self.config_entry.data.get(CONF_CSV_FILE, "")
        )
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_CSV_FILE, default=current): selector.FileSelector(
                        config=selector.FileSelectorConfig(accept=".csv")
                    ),
                }
            ),
            errors=errors,
        )


class InvalidCsvFile(ValueError):
    """Error to indicate that the uploaded file is not a valid CSV file."""


def _normalize_file_input(value: Any) -> str:
    """Extract a file id or path from the selector value."""
    if isinstance(value, dict):
        value = value.get("file_id") or value.get("path") or ""
    return (value or "").strip()


def _build_upload_path(hass, entry_id: str | None) -> Path:
    """Build a target path for the uploaded CSV file."""
    base_dir = Path(hass.config.path("esb_energy", "uploads"))
    return base_dir / "esb_energy.csv"


def _validate_csv_header(content: str) -> None:
    """Validate the CSV header contains the expected columns."""
    header = content.splitlines()[0] if content else ""
    required = {"MPRN", "Read Value", "Read Date and End Time"}
    columns = {col.strip() for col in header.split(",") if col.strip()}
    if not required.issubset(columns):
        raise InvalidCsvFile("Missing required columns")


def save_uploaded_csv_file(hass, uploaded_file_id: str, target_path: Path) -> str:
    """Validate the uploaded CSV file and move it to the config directory."""
    if Path(uploaded_file_id).exists():
        return str(uploaded_file_id)

    try:
        with process_uploaded_file(hass, uploaded_file_id) as file:
            content = file.read_text(encoding="utf-8-sig")
            _validate_csv_header(content)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            _merge_csv_content(target_path, content)
            return str(target_path)
    except ValueError as err:
        raise FileNotFoundError("Uploaded file not found") from err


def _merge_csv_content(target_path: Path, content: str) -> None:
    """Merge uploaded CSV content into a single consolidated file."""
    rows = _read_csv_rows(content)
    if not rows:
        raise InvalidCsvFile("No CSV data found")

    existing_rows: list[dict[str, str]] = []
    if target_path.exists():
        existing_rows = _read_csv_rows(target_path.read_text(encoding="utf-8-sig"))

    merged = _dedupe_rows(existing_rows + rows)
    merged.sort(
        key=lambda row: (
            row.get("MPRN", ""),
            row.get("Read Type", ""),
            row.get("Read Date and End Time", ""),
        )
    )

    fieldnames = ["MPRN", "Read Value", "Read Type", "Read Date and End Time"]
    with target_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in merged:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def _read_csv_rows(content: str) -> list[dict[str, str]]:
    """Parse CSV content into rows, dropping the serial number column."""
    lines = content.strip().splitlines()
    if len(lines) < 2:
        return []
    reader = csv.DictReader(lines)
    rows: list[dict[str, str]] = []
    for row in reader:
        cleaned = {
            "MPRN": (row.get("MPRN") or "").strip(),
            "Read Value": (row.get("Read Value") or "").strip(),
            "Read Type": (row.get("Read Type") or "").strip(),
            "Read Date and End Time": (row.get("Read Date and End Time") or "").strip(),
        }
        if not cleaned["MPRN"] or not cleaned["Read Date and End Time"]:
            continue
        rows.append(cleaned)
    return rows


def _dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Remove duplicate rows by MPRN + Read Type + timestamp."""
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, str]] = []
    for row in rows:
        key = (row.get("MPRN", ""), row.get("Read Type", ""), row.get("Read Date and End Time", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped
