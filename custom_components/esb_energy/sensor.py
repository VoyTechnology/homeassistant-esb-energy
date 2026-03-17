"""
Sensor platform for ESB Energy integration.
"""

import logging
from datetime import datetime, timedelta
from typing import Any
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.components.recorder.statistics import async_add_external_statistics
from homeassistant.components.recorder.models.statistics import (
    StatisticData,
    StatisticMetaData,
    StatisticMeanType,
)
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import UnitOfEnergy
from homeassistant.util import dt as dt_util
from homeassistant.util import slugify
from homeassistant.util.unit_conversion import EnergyConverter
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

SCAN_INTERVAL = timedelta(hours=2)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the ESB Energy sensor platform."""
    entry_data = hass.data[DOMAIN][config_entry.entry_id]
    client = entry_data["client"]
    mprn = entry_data.get("mprn", "")
    csv_file = entry_data.get("csv_file", "")
    entry_id = config_entry.entry_id

    async_add_entities([ESBEnergySensor(client, mprn, csv_file, entry_id)], True)


class ESBEnergySensor(SensorEntity):
    """Representation of an ESB Energy sensor."""

    def __init__(self, client, mprn, csv_file, entry_id):
        """Initialize the sensor."""
        self._client = client
        self._mprn = mprn
        self._csv_file = csv_file
        self._entry_id = entry_id
        object_id = f"mprn_{mprn}" if mprn else entry_id
        self._statistic_id = f"{DOMAIN}:{slugify(object_id)}"
        name_suffix = mprn if mprn else "CSV"
        self._attr_name = f"ESB Energy {name_suffix}"
        unique_suffix = mprn if mprn else entry_id
        self._attr_unique_id = f"esb_energy_{unique_suffix}"
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_device_class = SensorDeviceClass.ENERGY
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_available = False
        self._attr_extra_state_attributes = {
            "csv_file": csv_file,
            "statistic_id": self._statistic_id,
        }

    async def async_update(self):
        """Update the sensor."""
        try:
            data = await self._client.get_latest_reading()
            if data:
                self._attr_native_value = data.get("energy")
                self._attr_extra_state_attributes["last_updated"] = data.get(
                    "timestamp"
                )
                self._attr_extra_state_attributes["read_type"] = data.get("read_type")
                metadata = await self._client.get_metadata()
                self._attr_extra_state_attributes["rows"] = metadata.get("rows", 0)
                self._attr_extra_state_attributes["deduplicated_rows"] = metadata.get(
                    "deduplicated_rows", 0
                )
                readings_payload = await self._client.get_readings()
                self._attr_extra_state_attributes["read_mode"] = readings_payload.get(
                    "mode"
                )
                await self._async_import_statistics(readings_payload)
                self._attr_available = True
            else:
                self._attr_available = False
                _LOGGER.warning("No data received from ESB client")
        except Exception as exc:
            _LOGGER.error("Error updating ESB sensor: %s", exc)
            self._attr_available = False

    async def _async_import_statistics(self, payload: dict[str, Any]) -> None:
        """Import historical readings into the recorder statistics."""
        readings = payload.get("readings", [])
        if not readings or self.hass is None:
            return

        tz = dt_util.get_time_zone(self.hass.config.time_zone)
        buckets: dict[datetime, float] = {}
        for reading in readings:
            timestamp = reading.get("datetime")
            if not timestamp:
                continue
            if timestamp.tzinfo is None and tz is not None:
                timestamp = timestamp.replace(tzinfo=tz)
            timestamp = dt_util.as_local(timestamp)
            start = timestamp.replace(minute=0, second=0, microsecond=0)
            buckets[start] = buckets.get(start, 0.0) + float(
                reading.get("energy", 0.0)
            )

        if not buckets:
            return

        statistics: list[StatisticData] = []
        running_sum = 0.0
        for start in sorted(buckets.keys()):
            running_sum += buckets[start]
            statistics.append(
                StatisticData(start=start, state=buckets[start], sum=running_sum)
            )

        metadata = StatisticMetaData(
            mean_type=StatisticMeanType.NONE,
            has_sum=True,
            name=f"ESB Energy {self._mprn or self._entry_id} Consumption",
            source=DOMAIN,
            statistic_id=self._statistic_id,
            unit_class=EnergyConverter.UNIT_CLASS,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        )

        async_add_external_statistics(self.hass, metadata, statistics)
