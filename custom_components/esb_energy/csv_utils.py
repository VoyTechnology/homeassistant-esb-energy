"""CSV utilities for ESB Energy integration."""

from __future__ import annotations

import csv
from pathlib import Path


class InvalidCsvFile(ValueError):
    """Error to indicate that the file is not a valid ESB CSV file."""


def validate_csv_header(content: str) -> None:
    """Validate the CSV header contains the expected columns."""
    header = content.splitlines()[0] if content else ""
    required = {"MPRN", "Read Value", "Read Date and End Time"}
    columns = {col.strip() for col in header.split(",") if col.strip()}
    if not required.issubset(columns):
        raise InvalidCsvFile("Missing required columns")


def merge_csv_content(target_path: Path, content: str) -> None:
    """Merge CSV content into a single consolidated file."""
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


def extract_mprn(content: str) -> str | None:
    """Extract the first MPRN value from CSV content."""
    rows = _read_csv_rows(content)
    if not rows:
        return None
    mprn = rows[0].get("MPRN")
    return mprn or None


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
        key = (
            row.get("MPRN", ""),
            row.get("Read Type", ""),
            row.get("Read Date and End Time", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped
