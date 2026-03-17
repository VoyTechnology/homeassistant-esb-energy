"""
ESB Smart Meter CSV Client for Home Assistant integration.
Reads ESB CSV exports and provides the latest reading.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import datetime
from pathlib import Path
import re
from typing import Any, Dict, Optional


_LOGGER = logging.getLogger(__name__)


class ESBClient:
    """Client for reading ESB Smart Meter CSV exports."""

    def __init__(self, csv_path: str):
        """Initialize client with CSV path."""
        self.csv_path = csv_path

    async def get_latest_reading(self) -> Optional[Dict[str, Any]]:
        """Get the latest energy reading from the CSV file."""
        try:
            csv_data = await self._read_csv()
            if not csv_data:
                return None
            parsed = self._parse_readings(csv_data)
            if not parsed["latest"]:
                return None
            return parsed["latest"]
        except Exception as exc:
            _LOGGER.error("Error reading ESB CSV data: %s", exc)
            return None

    async def get_metadata(self) -> Dict[str, Any]:
        """Return metadata about the current CSV file."""
        csv_data = await self._read_csv()
        if not csv_data:
            return {"rows": 0, "deduplicated_rows": 0}
        parsed = self._parse_readings(csv_data)
        return {
            "rows": parsed["rows"],
            "deduplicated_rows": parsed["deduplicated_rows"],
        }

    async def get_readings(self) -> Dict[str, Any]:
        """Return parsed readings and mode information."""
        csv_data = await self._read_csv()
        if not csv_data:
            return {"mode": "unknown", "readings": [], "read_type": None}
        parsed = self._parse_readings(csv_data)
        return {
            "mode": parsed["mode"],
            "readings": parsed["readings"],
            "read_type": parsed["read_type"],
        }

    async def _read_csv(self) -> Optional[str]:
        """Read CSV contents from disk."""
        if not self.csv_path:
            _LOGGER.error("CSV file path is not configured")
            return None
        path = Path(self.csv_path)
        if not path.exists():
            _LOGGER.error("CSV file not found: %s", self.csv_path)
            return None
        try:
            return await asyncio.to_thread(path.read_text, encoding="utf-8-sig")
        except Exception as exc:
            _LOGGER.error("Failed to read CSV file %s: %s", self.csv_path, exc)
            return None

    def _parse_readings(self, csv_data: str) -> Dict[str, Any]:
        """Parse CSV data, deduplicate readings, and select the latest entry."""
        if not csv_data or not csv_data.startswith("MPRN"):
            _LOGGER.error("Invalid CSV data format")
            return {
                "rows": 0,
                "deduplicated_rows": 0,
                "latest": None,
                "readings": [],
                "mode": "unknown",
                "read_type": None,
            }

        lines = csv_data.strip().split("\n")
        if len(lines) < 2:
            return {
                "rows": 0,
                "deduplicated_rows": 0,
                "latest": None,
                "readings": [],
                "mode": "unknown",
                "read_type": None,
            }

        reader = csv.DictReader(lines)
        rows = 0
        register_rows: dict[datetime, Dict[str, Any]] = {}
        interval_rows: dict[datetime, Dict[str, Any]] = {}
        register_type = None
        interval_type = None

        for row in reader:
            rows += 1
            timestamp_raw = row.get("Read Date and End Time", "").strip()
            if not timestamp_raw:
                continue
            try:
                timestamp = datetime.strptime(timestamp_raw, "%d-%m-%Y %H:%M")
            except ValueError:
                _LOGGER.debug("Skipping row with invalid timestamp: %s", timestamp_raw)
                continue

            read_type = (row.get("Read Type") or "").strip()
            value_raw = row.get("Read Value", "").strip()
            try:
                value = float(value_raw)
            except (TypeError, ValueError):
                value = 0.0

            mode, unit = _classify_read_type(read_type)
            if mode == "register":
                register_type = register_type or read_type
                register_rows[timestamp] = {
                    "value": value,
                    "timestamp": timestamp_raw,
                    "read_type": read_type,
                    "unit": unit,
                }
            elif mode == "interval":
                interval_type = interval_type or read_type
                interval_rows[timestamp] = {
                    "value": value,
                    "timestamp": timestamp_raw,
                    "read_type": read_type,
                    "unit": unit,
                }

        if register_rows:
            parsed = _parse_register_rows(register_rows)
            parsed["rows"] = rows
            parsed["read_type"] = register_type
            return parsed

        if interval_rows:
            parsed = _parse_interval_rows(interval_rows)
            parsed["rows"] = rows
            parsed["read_type"] = interval_type
            return parsed

        return {
            "rows": rows,
            "deduplicated_rows": 0,
            "latest": None,
            "readings": [],
            "mode": "unknown",
            "read_type": None,
        }


def _classify_read_type(read_type: str) -> tuple[str | None, str | None]:
    """Return (mode, unit) based on the Read Type field."""
    unit = None
    match = re.search(r"\(([^)]+)\)", read_type)
    if match:
        unit = match.group(1).strip()
    read_type_lower = read_type.lower()

    if "register" in read_type_lower and "kwh" in read_type_lower:
        return "register", unit or "kWh"
    if "interval" in read_type_lower and "kw" in read_type_lower:
        return "interval", unit or "kW"
    if "interval" in read_type_lower and "kwh" in read_type_lower:
        return "interval", unit or "kWh"

    return None, unit


def _parse_register_rows(rows: dict[datetime, Dict[str, Any]]) -> Dict[str, Any]:
    """Parse register rows (cumulative kWh)."""
    ordered = sorted(rows.items(), key=lambda item: item[0])
    if not ordered:
        return {
            "rows": 0,
            "deduplicated_rows": 0,
            "latest": None,
            "readings": [],
            "mode": "register",
            "read_type": None,
        }

    readings: list[Dict[str, Any]] = []
    prev_value = None
    for timestamp, data in ordered:
        if prev_value is None:
            prev_value = data["value"]
            continue
        usage = max(0.0, data["value"] - prev_value)
        prev_value = data["value"]
        readings.append(
            {
                "datetime": timestamp,
                "energy": usage,
                "timestamp": data["timestamp"],
            }
        )

    latest_timestamp, latest_data = ordered[-1]
    latest = {
        "energy": latest_data["value"],
        "timestamp": latest_data["timestamp"],
        "unit": "kWh",
        "read_type": latest_data["read_type"],
    }

    return {
        "rows": len(rows),
        "deduplicated_rows": len(rows),
        "latest": latest,
        "readings": readings,
        "mode": "register",
        "read_type": latest_data["read_type"],
    }


def _parse_interval_rows(rows: dict[datetime, Dict[str, Any]]) -> Dict[str, Any]:
    """Parse interval rows (kW or kWh per interval)."""
    ordered = sorted(rows.items(), key=lambda item: item[0])
    if not ordered:
        return {
            "rows": 0,
            "deduplicated_rows": 0,
            "latest": None,
            "readings": [],
            "mode": "interval",
            "read_type": None,
        }

    interval_hours = _infer_interval_hours([t for t, _ in ordered])
    readings: list[Dict[str, Any]] = []
    total = 0.0

    for timestamp, data in ordered:
        value = data["value"]
        unit = (data.get("unit") or "").lower()
        if unit == "kwh":
            usage = value
        else:
            usage = value * interval_hours
        total += usage
        readings.append(
            {
                "datetime": timestamp,
                "energy": usage,
                "timestamp": data["timestamp"],
            }
        )

    latest_timestamp, latest_data = ordered[-1]
    latest = {
        "energy": total,
        "timestamp": latest_data["timestamp"],
        "unit": "kWh",
        "read_type": latest_data["read_type"],
    }

    return {
        "rows": len(rows),
        "deduplicated_rows": len(rows),
        "latest": latest,
        "readings": readings,
        "mode": "interval",
        "read_type": latest_data["read_type"],
    }


def _infer_interval_hours(timestamps: list[datetime]) -> float:
    """Infer the most common interval in hours, defaulting to 0.5."""
    if len(timestamps) < 2:
        return 0.5
    diffs = {}
    for prev, curr in zip(timestamps, timestamps[1:]):
        delta = (curr - prev).total_seconds()
        if delta <= 0:
            continue
        diffs[delta] = diffs.get(delta, 0) + 1
    if not diffs:
        return 0.5
    most_common = max(diffs.items(), key=lambda item: item[1])[0]
    return most_common / 3600.0
