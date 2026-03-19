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
    downloader = entry_data.get("downloader")
    entry_id = config_entry.entry_id

    entities = [
        ESBEnergySensor(client, mprn, csv_file, entry_id, "import"),
        ESBEnergySensor(client, mprn, csv_file, entry_id, "export"),
    ]
    if downloader is not None:
        entities.append(ESBLastFetchSensor(downloader, mprn, entry_id))
    async_add_entities(entities, True)


class ESBEnergySensor(SensorEntity):
    """Representation of an ESB Energy sensor."""

    def __init__(self, client, mprn, csv_file, entry_id, direction: str):
        """Initialize the sensor."""
        self._client = client
        self._mprn = mprn
        self._csv_file = csv_file
        self._entry_id = entry_id
        self._direction = direction
        object_id = f"mprn_{mprn}_{direction}" if mprn else f"{entry_id}_{direction}"
        object_id = f"{mprn}_{direction}" if mprn else f"{entry_id}_{direction}"
        self._statistic_id = f"{DOMAIN}:{slugify(object_id)}"
        name_suffix = mprn if mprn else "CSV"
        direction_label = "Import" if direction == "import" else "Export"
        self._attr_name = f"ESB Energy {direction_label} {name_suffix}"
        unique_suffix = mprn if mprn else entry_id
        self._attr_unique_id = f"esb_energy_{unique_suffix}_{direction}"
        self._attr_native_unit_of_measurement = "kWh"
        self._attr_device_class = SensorDeviceClass.ENERGY
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_available = False
        self._attr_extra_state_attributes = {
            "csv_file": csv_file,
            "statistic_id": self._statistic_id,
            "direction": direction,
        }
        self._last_value = None
        self._reset_detected = False

    async def async_update(self):
        """Update the sensor."""
        try:
            data = await self._client.get_latest_reading(self._direction)
            if data:
                energy = data.get("energy")
                if energy is not None and self._last_value is not None:
                    if energy < self._last_value:
                        self._reset_detected = True
                        _LOGGER.warning(
                            "Detected decreasing ESB total (%.3f -> %.3f); "
                            "keeping previous value.",
                            self._last_value,
                            energy,
                        )
                        energy = self._last_value
                    else:
                        self._reset_detected = False
                self._attr_native_value = energy
                if energy is not None:
                    self._last_value = energy
                self._attr_extra_state_attributes["last_updated"] = data.get(
                    "timestamp"
                )
                self._attr_extra_state_attributes["read_type"] = data.get("read_type")
                if data.get("total_energy") is not None:
                    self._attr_extra_state_attributes["total_energy"] = data.get(
                        "total_energy"
                    )
                if data.get("interval_energy") is not None:
                    self._attr_extra_state_attributes["interval_energy"] = data.get(
                        "interval_energy"
                    )
                metadata = await self._client.get_metadata(self._direction)
                self._attr_extra_state_attributes["rows"] = metadata.get("rows", 0)
                self._attr_extra_state_attributes["deduplicated_rows"] = metadata.get(
                    "deduplicated_rows", 0
                )
                readings_payload = await self._client.get_readings(self._direction)
                self._attr_extra_state_attributes["read_mode"] = readings_payload.get(
                    "mode"
                )
                if readings_payload.get("mode") == "interval_with_snapshot":
                    self._attr_extra_state_attributes["snapshot_value"] = readings_payload.get(
                        "baseline"
                    )
                    self._attr_extra_state_attributes[
                        "snapshot_timestamp"
                    ] = readings_payload.get("baseline_timestamp")
                self._attr_extra_state_attributes[
                    "reset_detected"
                ] = self._reset_detected
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
            usage = float(reading.get("energy", 0.0))
            if usage < 0:
                usage = 0.0
            buckets[start] = buckets.get(start, 0.0) + usage

        if not buckets:
            return

        statistics: list[StatisticData] = []
        running_sum = 0.0
        for start in sorted(buckets.keys()):
            running_sum += buckets[start]
            statistics.append(
                StatisticData(start=start, state=buckets[start], sum=running_sum)
            )

        direction_label = "Import" if self._direction == "import" else "Export"
        metadata = StatisticMetaData(
            mean_type=StatisticMeanType.NONE,
            has_sum=True,
            name=f"ESB Energy {self._mprn or self._entry_id} {direction_label}",
            source=DOMAIN,
            statistic_id=self._statistic_id,
            unit_class=EnergyConverter.UNIT_CLASS,
            unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        )

        async_add_external_statistics(self.hass, metadata, statistics)


class ESBLastFetchSensor(SensorEntity):
    """Sensor for the last successful data fetch timestamp."""

    def __init__(self, downloader, mprn, entry_id):
        self._downloader = downloader
        name_suffix = mprn if mprn else entry_id
        self._attr_name = f"ESB Energy {name_suffix} Last Fetch"
        self._attr_unique_id = f"esb_energy_last_fetch_{name_suffix}"
        self._attr_device_class = SensorDeviceClass.TIMESTAMP
        self._attr_icon = "mdi:cloud-download"
        self._attr_available = True

    async def async_update(self):
        last_fetch = self._downloader.last_fetch
        if last_fetch:
            self._attr_native_value = dt_util.as_local(last_fetch)
        else:
            self._attr_native_value = None
