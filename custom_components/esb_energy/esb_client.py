"""
ESB Smart Meter CSV Client for Home Assistant integration.
Reads ESB CSV exports and provides the latest reading.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Any, Dict, Optional


_LOGGER = logging.getLogger(__name__)


class ESBClient:
    """Client for reading ESB Smart Meter CSV exports."""

    def __init__(self, csv_path: str):
        """Initialize client with CSV path."""
        self.csv_path = csv_path

    async def get_latest_reading(self, direction: str = "import") -> Optional[Dict[str, Any]]:
        """Get the latest energy reading from the CSV file."""
        try:
            csv_data = await self._read_csv()
            if not csv_data:
                return None
            parsed = self._parse_readings(csv_data)
            dataset = parsed["datasets"].get(direction)
            if not dataset or not dataset["latest"]:
                return None
            return dataset["latest"]
        except Exception as exc:
            _LOGGER.error("Error reading ESB CSV data: %s", exc)
            return None

    async def get_metadata(self, direction: str | None = None) -> Dict[str, Any]:
        """Return metadata about the current CSV file."""
        csv_data = await self._read_csv()
        if not csv_data:
            return {"rows": 0, "deduplicated_rows": 0}
        parsed = self._parse_readings(csv_data)
        if direction:
            dataset = parsed["datasets"].get(direction)
            if not dataset:
                return {"rows": 0, "deduplicated_rows": 0}
            return {
                "rows": dataset["rows"],
                "deduplicated_rows": dataset["deduplicated_rows"],
            }
        return {
            "rows": parsed["rows"],
            "deduplicated_rows": parsed["deduplicated_rows"],
        }

    async def get_readings(self, direction: str = "import") -> Dict[str, Any]:
        """Return parsed readings and mode information."""
        csv_data = await self._read_csv()
        if not csv_data:
            return {"mode": "unknown", "readings": [], "read_type": None}
        parsed = self._parse_readings(csv_data)
        dataset = parsed["datasets"].get(direction)
        if not dataset:
            return {"mode": "unknown", "readings": [], "read_type": None}
        return {
            "mode": dataset["mode"],
            "readings": dataset["readings"],
            "read_type": dataset["read_type"],
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
                "datasets": {},
            }

        lines = csv_data.strip().split("\n")
        if len(lines) < 2:
            return {
                "rows": 0,
                "deduplicated_rows": 0,
                "datasets": {},
            }

        reader = csv.DictReader(lines)
        rows = 0
        datasets: dict[str, dict[str, Any]] = {}
        register_rows: dict[str, dict[datetime, Dict[str, Any]]] = {
            "import": {},
            "export": {},
        }
        interval_rows: dict[str, dict[datetime, Dict[str, Any]]] = {
            "import": {},
            "export": {},
        }
        register_type: dict[str, str | None] = {"import": None, "export": None}
        interval_type: dict[str, str | None] = {"import": None, "export": None}

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
            direction = "export" if "export" in read_type.lower() else "import"
            if mode == "register":
                register_type[direction] = register_type[direction] or read_type
                register_rows[direction][timestamp] = {
                    "value": value,
                    "timestamp": timestamp_raw,
                    "read_type": read_type,
                    "unit": unit,
                }
            elif mode == "interval":
                interval_type[direction] = interval_type[direction] or read_type
                interval_rows[direction][timestamp] = {
                    "value": value,
                    "timestamp": timestamp_raw,
                    "read_type": read_type,
                    "unit": unit,
                }

        total_deduped = 0
        for direction in ("import", "export"):
            if register_rows[direction] and interval_rows[direction]:
                parsed = _parse_interval_with_snapshot(
                    register_rows[direction], interval_rows[direction]
                )
                parsed["read_type"] = interval_type[direction] or register_type[direction]
                datasets[direction] = parsed
            elif register_rows[direction]:
                parsed = _parse_register_rows(register_rows[direction])
                parsed["read_type"] = register_type[direction]
                datasets[direction] = parsed
            elif interval_rows[direction]:
                parsed = _parse_interval_rows(interval_rows[direction])
                parsed["read_type"] = interval_type[direction]
                datasets[direction] = parsed
            if direction in datasets:
                total_deduped += datasets[direction]["deduplicated_rows"]

        return {
            "rows": rows,
            "deduplicated_rows": total_deduped,
            "datasets": datasets,
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
        usage_timestamp = timestamp - timedelta(days=1)
        readings.append(
            {
                "datetime": usage_timestamp,
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
    latest_usage = readings[-1]["energy"] if readings else 0.0
    latest = {
        "energy": latest_usage,
        "total_energy": total,
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


def _parse_interval_with_snapshot(
    register_rows: dict[datetime, Dict[str, Any]],
    interval_rows: dict[datetime, Dict[str, Any]],
) -> Dict[str, Any]:
    """Combine interval readings with the latest register snapshot."""
    ordered_registers = sorted(register_rows.items(), key=lambda item: item[0])
    ordered_intervals = sorted(interval_rows.items(), key=lambda item: item[0])
    if not ordered_registers:
        return _parse_interval_rows(interval_rows)

    snapshot_timestamp, snapshot_data = ordered_registers[-1]
    snapshot_value = snapshot_data["value"]
    interval_after_snapshot = [
        (ts, data) for ts, data in ordered_intervals if ts > snapshot_timestamp
    ]
    readings: list[Dict[str, Any]] = []
    baseline_value = ordered_registers[0][1]["value"]

    prev_value = ordered_registers[0][1]["value"]
    for timestamp, data in ordered_registers[1:]:
        usage = max(0.0, data["value"] - prev_value)
        prev_value = data["value"]
        usage_timestamp = timestamp - timedelta(days=1)
        readings.append(
            {
                "datetime": usage_timestamp,
                "energy": usage,
                "timestamp": data["timestamp"],
            }
        )

    total_delta = 0.0
    interval_hours = _infer_interval_hours([t for t, _ in interval_after_snapshot])
    for timestamp, data in interval_after_snapshot:
        value = data["value"]
        unit = (data.get("unit") or "").lower()
        if unit == "kwh":
            usage = value
        else:
            usage = value * interval_hours
        total_delta += usage
        readings.append(
            {
                "datetime": timestamp,
                "energy": usage,
                "timestamp": data["timestamp"],
            }
        )

    if interval_after_snapshot:
        latest_data = interval_after_snapshot[-1][1]
    else:
        latest_data = snapshot_data
    latest_usage = readings[-1]["energy"] if readings else 0.0
    latest = {
        "energy": snapshot_value + total_delta,
        "interval_energy": latest_usage,
        "timestamp": latest_data["timestamp"],
        "unit": "kWh",
        "read_type": latest_data["read_type"],
    }

    return {
        "rows": len(register_rows) + len(interval_rows),
        "deduplicated_rows": len(register_rows) + len(interval_rows),
        "latest": latest,
        "readings": readings,
        "mode": "interval_with_snapshot",
        "read_type": latest_data["read_type"],
        "baseline": baseline_value,
        "baseline_timestamp": ordered_registers[0][1]["timestamp"],
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
