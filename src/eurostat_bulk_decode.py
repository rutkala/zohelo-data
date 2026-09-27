"""Decode one retained Eurostat full-distribution TSV into source-shaped Bronze.

Landing remains the exact native ``.tsv.gz`` object.  This module is a downstream,
replayable transformation: it streams the retained file, preserves every native
dimension code, period, value lexeme and status flag, and writes an independently
verifiable Parquet relation without changing the Landing object.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import gzip
from hashlib import sha256
import json
import math
from pathlib import Path
import re
import sys
from typing import Any

import duckdb


_DATASET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.$-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DIMENSION_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_DRIVE_FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")
_BATCH_ROWS = 10_000

BRONZE_COLUMNS = (
    ("dataset_id", "VARCHAR"),
    ("period_key", "VARCHAR"),
    ("freq_code", "VARCHAR"),
    ("unit_code", "VARCHAR"),
    ("geo_code", "VARCHAR"),
    ("dimension_key_json", "VARCHAR"),
    ("dimension_key_sha256", "VARCHAR"),
    ("value_text", "VARCHAR"),
    ("value_numeric", "DOUBLE"),
    ("status_code", "VARCHAR"),
    ("is_missing", "BOOLEAN"),
    ("missing_reason", "VARCHAR"),
    ("source_version", "VARCHAR"),
    ("retrieved_at_utc", "TIMESTAMPTZ"),
    ("raw_sha256", "VARCHAR"),
    ("raw_file_id", "VARCHAR"),
    ("source_row_number", "BIGINT"),
    ("source_period_position", "BIGINT"),
)


class EurostatBulkDecodeError(ValueError):
    """A retained Eurostat distribution cannot satisfy the Bronze contract."""


def decode_full_distribution(
    source: Path,
    output: Path,
    *,
    dataset_id: str,
    retrieved_at_utc: str,
    raw_sha256: str,
    raw_file_id: str,
    source_version: str | None = None,
) -> dict[str, Any]:
    """Stream one exact Eurostat TSV.GZ distribution into a fresh Parquet file.

    Empty and ``:`` cells are both retained as missing but remain distinguishable
    through ``missing_reason``.  Numeric source text is kept alongside the
    analytical double so downstream consumers never depend on a lossy conversion.
    """
    source = Path(source)
    output = Path(output)
    _validate_identity(
        source=source,
        output=output,
        dataset_id=dataset_id,
        retrieved_at_utc=retrieved_at_utc,
        raw_sha256=raw_sha256,
        raw_file_id=raw_file_id,
        source_version=source_version,
    )
    if _file_sha256(source) != raw_sha256:
        raise EurostatBulkDecodeError("Eurostat distribution SHA-256 does not match its receipt")

    output.parent.mkdir(parents=True, exist_ok=True)
    database = output.parent / f".{output.name}.duckdb"
    if database.exists():
        raise EurostatBulkDecodeError("Eurostat decoder working database already exists")

    measurements = {
        "dataset_id": dataset_id,
        "source_rows": 0,
        "observation_cells": 0,
        "missing_cells": 0,
        "empty_cells": 0,
        "colon_cells": 0,
        "flagged_cells": 0,
        "periods": 0,
    }
    completed = False
    try:
        with duckdb.connect(str(database)) as connection:
            connection.execute("SET memory_limit = '512MB'")
            definitions = ", ".join(
                f'"{name}" {kind}' for name, kind in BRONZE_COLUMNS
            )
            connection.execute(f"CREATE TABLE bronze ({definitions})")
            with gzip.open(
                source, "rt", encoding="utf-8-sig", errors="strict", newline=""
            ) as stream:
                reader = csv.reader(stream, delimiter="\t")
                try:
                    header = next(reader)
                except StopIteration:
                    raise EurostatBulkDecodeError("Eurostat TSV is empty") from None
                dimension_names, periods = _parse_header(header)
                measurements["periods"] = len(periods)
                batch: list[tuple[Any, ...]] = []
                placeholders = ",".join("?" for _ in BRONZE_COLUMNS)
                for line_number, row in enumerate(reader, start=2):
                    if len(row) != len(header):
                        raise EurostatBulkDecodeError(
                            f"Eurostat TSV row {line_number} has {len(row)} fields; "
                            f"expected {len(header)}"
                        )
                    dimension_codes = [part.strip() for part in row[0].split(",")]
                    if len(dimension_codes) != len(dimension_names) or any(
                        not code for code in dimension_codes
                    ):
                        raise EurostatBulkDecodeError(
                            f"Eurostat TSV row {line_number} has an invalid series key"
                        )
                    series_dimensions = dict(zip(dimension_names, dimension_codes))
                    measurements["source_rows"] += 1
                    for period_position, (period, cell) in enumerate(
                        zip(periods, row[1:]), start=1
                    ):
                        dimensions = {**series_dimensions, "time": period}
                        key_json = json.dumps(
                            dimensions,
                            ensure_ascii=False,
                            separators=(",", ":"),
                            sort_keys=True,
                        )
                        parsed = _parse_cell(cell, line_number, period)
                        if parsed[3]:
                            measurements["missing_cells"] += 1
                            measurements[f"{parsed[4]}_cells"] += 1
                        if parsed[2] is not None:
                            measurements["flagged_cells"] += 1
                        batch.append(
                            (
                                dataset_id,
                                period,
                                series_dimensions.get("freq"),
                                series_dimensions.get("unit"),
                                series_dimensions.get("geo"),
                                key_json,
                                sha256(key_json.encode("utf-8")).hexdigest(),
                                parsed[0],
                                parsed[1],
                                parsed[2],
                                parsed[3],
                                parsed[4],
                                source_version,
                                retrieved_at_utc,
                                raw_sha256,
                                raw_file_id,
                                line_number,
                                period_position,
                            )
                        )
                        measurements["observation_cells"] += 1
                        if len(batch) >= _BATCH_ROWS:
                            connection.executemany(
                                f"INSERT INTO bronze VALUES ({placeholders})", batch
                            )
                            batch.clear()
                if batch:
                    connection.executemany(
                        f"INSERT INTO bronze VALUES ({placeholders})", batch
                    )

            duplicate = connection.execute(
                """
                SELECT dimension_key_sha256, count(*)
                FROM bronze
                GROUP BY dimension_key_sha256
                HAVING count(*) > 1
                LIMIT 1
                """
            ).fetchone()
            if duplicate is not None:
                raise EurostatBulkDecodeError(
                    "Eurostat TSV repeats a complete dimensional observation key"
                )
            escaped = str(output).replace("'", "''")
            connection.execute(
                f"COPY bronze TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        if not output.is_file():
            raise EurostatBulkDecodeError("Eurostat Bronze output was not created")
        measurements["output_bytes"] = output.stat().st_size
        measurements["output_sha256"] = _file_sha256(output)
        completed = True
    except EurostatBulkDecodeError:
        raise
    except (OSError, EOFError, UnicodeError, csv.Error, duckdb.Error) as exc:
        raise EurostatBulkDecodeError(f"invalid Eurostat full distribution: {exc}") from exc
    finally:
        database.unlink(missing_ok=True)
        if not completed and output.exists():
            output.unlink()

    return measurements


def _parse_header(header: list[str]) -> tuple[list[str], list[str]]:
    if not header or "\\TIME_PERIOD" not in header[0].upper():
        raise EurostatBulkDecodeError(
            "Eurostat TSV header has no series key/TIME_PERIOD column"
        )
    series_header, marker = header[0].split("\\", 1)
    if marker.strip().upper() != "TIME_PERIOD":
        raise EurostatBulkDecodeError("Eurostat TSV has an unsupported observation axis")
    dimensions = [value.strip().lower() for value in series_header.split(",")]
    if (
        not dimensions
        or any(not _DIMENSION_RE.fullmatch(value) for value in dimensions)
        or len(set(dimensions)) != len(dimensions)
        or "time" in dimensions
    ):
        raise EurostatBulkDecodeError("Eurostat TSV has invalid series dimensions")
    periods = [value.strip() for value in header[1:]]
    if not periods or any(not value for value in periods) or len(set(periods)) != len(periods):
        raise EurostatBulkDecodeError("Eurostat TSV has invalid or duplicate periods")
    return dimensions, periods


def _parse_cell(
    raw: str, line_number: int, period: str
) -> tuple[str | None, float | None, str | None, bool, str | None]:
    stripped = raw.strip()
    if not stripped:
        return "", None, None, True, "empty"
    parts = stripped.split(maxsplit=1)
    value_text = parts[0]
    status = parts[1].strip() if len(parts) == 2 and parts[1].strip() else None
    if value_text == ":":
        return ":", None, status, True, "colon"
    try:
        numeric = Decimal(value_text)
    except InvalidOperation:
        raise EurostatBulkDecodeError(
            f"Eurostat TSV row {line_number} period {period!r} has a non-numeric value"
        ) from None
    if not numeric.is_finite():
        raise EurostatBulkDecodeError(
            f"Eurostat TSV row {line_number} period {period!r} has a non-finite value"
        )
    numeric_float = float(numeric)
    if not math.isfinite(numeric_float):
        raise EurostatBulkDecodeError(
            f"Eurostat TSV row {line_number} period {period!r} exceeds numeric range"
        )
    return value_text, numeric_float, status, False, None


def _validate_identity(
    *,
    source: Path,
    output: Path,
    dataset_id: str,
    retrieved_at_utc: str,
    raw_sha256: str,
    raw_file_id: str,
    source_version: str | None,
) -> None:
    if not source.is_file():
        raise EurostatBulkDecodeError("Eurostat distribution path is not a regular file")
    if output.exists():
        raise EurostatBulkDecodeError("Eurostat Bronze output must be a fresh path")
    if not isinstance(dataset_id, str) or not _DATASET_RE.fullmatch(dataset_id):
        raise EurostatBulkDecodeError("Eurostat dataset identity is invalid")
    if not isinstance(raw_sha256, str) or not _SHA256_RE.fullmatch(raw_sha256):
        raise EurostatBulkDecodeError("Eurostat distribution SHA-256 is invalid")
    if not isinstance(raw_file_id, str) or not _DRIVE_FILE_ID_RE.fullmatch(raw_file_id):
        raise EurostatBulkDecodeError("Eurostat Drive file identity is invalid")
    if source_version is not None and (
        not isinstance(source_version, str)
        or not source_version
        or len(source_version) > 1024
    ):
        raise EurostatBulkDecodeError("Eurostat source version is invalid")
    try:
        parsed = datetime.fromisoformat(retrieved_at_utc.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise EurostatBulkDecodeError("Eurostat retrieval timestamp is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise EurostatBulkDecodeError("Eurostat retrieval timestamp must be UTC")


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    """Run the local, non-publishing decoder for one receipt-bound distribution."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--retrieved-at-utc", required=True)
    parser.add_argument("--raw-sha256", required=True)
    parser.add_argument("--raw-file-id", required=True)
    parser.add_argument("--source-version")
    args = parser.parse_args(argv)
    try:
        report = decode_full_distribution(
            args.source,
            args.output,
            dataset_id=args.dataset_id,
            retrieved_at_utc=args.retrieved_at_utc,
            raw_sha256=args.raw_sha256,
            raw_file_id=args.raw_file_id,
            source_version=args.source_version,
        )
    except EurostatBulkDecodeError as exc:
        print(
            json.dumps(
                {
                    "status": "eurostat_full_distribution_decode_failed",
                    "error_type": type(exc).__name__,
                    "detail": str(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {"status": "eurostat_full_distribution_decoded", **report},
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the library tests
    raise SystemExit(main())
