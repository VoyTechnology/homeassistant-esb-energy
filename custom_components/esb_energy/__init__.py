"""
Home Assistant integration for ESB Energy data.
"""

from pathlib import Path
import logging

from .const import CONF_CSV_FILE, CONF_MPRN, DOMAIN
from .esb_client import ESBClient

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass, config_entry):
    """Set up the ESB Energy integration from a config entry."""
    data = config_entry.data
    options = config_entry.options
    csv_file = options.get(CONF_CSV_FILE, data.get(CONF_CSV_FILE, ""))
    csv_file = _resolve_csv_file(hass, csv_file, config_entry.entry_id)

    _LOGGER.info("ESB Energy using CSV file: %s", csv_file)
    client = ESBClient(csv_path=csv_file)

    # Store client in hass.data for platforms to use
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][config_entry.entry_id] = {
        "client": client,
        "mprn": data.get(CONF_MPRN, ""),
        "csv_file": csv_file,
    }

    # Forward setup to sensor platform
    await hass.config_entries.async_forward_entry_setups(config_entry, ["sensor"])

    return True


def _resolve_csv_file(hass, csv_file: str, entry_id: str) -> str:
    """Resolve a valid CSV path from stored data or uploads directory."""
    if csv_file:
        path = hass.config.path(csv_file)
        if csv_file.startswith("/") or path == csv_file:
            if Path(csv_file).exists():
                return csv_file
        if Path(path).exists():
            return path

    uploads_dir = Path(hass.config.path("esb_energy", "uploads"))
    candidate = uploads_dir / f"{entry_id}.csv"
    if candidate.exists():
        return str(candidate)

    if uploads_dir.exists():
        latest = max(uploads_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime, default=None)
        if latest:
            return str(latest)

    return csv_file


async def async_unload_entry(hass, config_entry):
    """Unload a config entry."""
    # Unload sensor platform
    unload_ok = await hass.config_entries.async_forward_entry_unload(
        config_entry, "sensor"
    )

    if unload_ok:
        hass.data[DOMAIN].pop(config_entry.entry_id, None)

    return unload_ok


async def async_setup(hass, config):
    """Set up integration via configuration.yaml."""
    # Support configuration.yaml setup for backward compatibility
    if DOMAIN in config:
        config_data = config[DOMAIN]
        mprn = config_data.get(CONF_MPRN)
        csv_file = config_data.get(CONF_CSV_FILE)
        if csv_file:
            # Create a config entry from yaml config
            hass.async_create_task(
                hass.config_entries.flow.async_init(
                    DOMAIN,
                    context={"source": "import"},
                    data={
                        CONF_MPRN: mprn or "",
                        CONF_CSV_FILE: csv_file,
                    },
                )
            )
    return True
