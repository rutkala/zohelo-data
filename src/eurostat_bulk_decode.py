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
from typing import Any, Mapping
from urllib.parse import urlsplit

import duckdb
import pandas as pd


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MD5_RE = re.compile(r"^[0-9a-f]{32}$")
_DIMENSION_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
_DRIVE_FILE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,1024}$")
_BATCH_ROWS = 10_000

BRONZE_COLUMNS = (
    ("dataset_id", "VARCHAR"),
    ("distribution_id", "VARCHAR"),
    ("partition_id", "VARCHAR"),
    ("partition_selection_json", "VARCHAR"),
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
    ("receipt_sha256", "VARCHAR"),
    ("receipt_file_id", "VARCHAR"),
    ("receipt_size_bytes", "BIGINT"),
    ("source_row_number", "BIGINT"),
    ("source_period_position", "BIGINT"),
)
_BRONZE_COLUMN_NAMES = tuple(name for name, _kind in BRONZE_COLUMNS)


class EurostatBulkDecodeError(ValueError):
    """A retained Eurostat distribution cannot satisfy the Bronze contract."""


def decode_full_distribution(
    source: Path,
    receipt: Path,
    output: Path,
    *,
    receipt_descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    """Stream one exact Eurostat TSV.GZ distribution into a fresh Parquet file.

    Empty and ``:`` cells are both retained as missing but remain distinguishable
    through ``missing_reason``.  Numeric source text is kept alongside the
    analytical double so downstream consumers never depend on a lossy conversion.
    """
    source = Path(source)
    receipt = Path(receipt)
    output = Path(output)
    identity = _validated_receipt_identity(
        source=source, receipt=receipt, output=output,
        descriptor=receipt_descriptor,
    )
    dataset_id = identity["dataset_id"]
    if _file_sha256(source) != identity["raw_sha256"]:
        raise EurostatBulkDecodeError("Eurostat distribution SHA-256 does not match its receipt")

    output.parent.mkdir(parents=True, exist_ok=True)
    database = output.parent / f".{output.name}.duckdb"
    if database.exists():
        raise EurostatBulkDecodeError("Eurostat decoder working database already exists")

    measurements = {
        "dataset_id": dataset_id,
        "distribution_id": identity["distribution_id"],
        "partition_id": identity["partition_id"],
        "empty_partition": identity["empty_partition"],
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
            # Exact duplicate-key validation is a large hash aggregation. The hosted
            # runner has bounded memory, so give DuckDB room to spill without multiplying
            # per-thread state or retaining insertion-order metadata.
            connection.execute("SET memory_limit = '4GB'")
            connection.execute("SET threads = 1")
            connection.execute("SET preserve_insertion_order = false")
            definitions = ", ".join(
                f'"{name}" {kind}' for name, kind in BRONZE_COLUMNS
            )
            connection.execute(f"CREATE TABLE bronze ({definitions})")
            if not identity["empty_partition"]:
                _load_observations(connection, source, identity, measurements)

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


def _load_observations(
    connection: duckdb.DuckDBPyConnection,
    source: Path,
    identity: Mapping[str, Any],
    measurements: dict[str, Any],
) -> None:
    """Decode the native TSV body after receipt and raw identity are accepted."""
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
                        identity["dataset_id"],
                        identity["distribution_id"],
                        identity["partition_id"],
                        identity["partition_selection_json"],
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
                        identity["source_version"],
                        identity["retrieved_at_utc"],
                        identity["raw_sha256"],
                        identity["raw_file_id"],
                        identity["receipt_sha256"],
                        identity["receipt_file_id"],
                        identity["receipt_size_bytes"],
                        line_number,
                        period_position,
                    )
                )
                measurements["observation_cells"] += 1
                if len(batch) >= _BATCH_ROWS:
                    _append_batch(connection, batch)
        if batch:
            _append_batch(connection, batch)


def _append_batch(
    connection: duckdb.DuckDBPyConnection,
    batch: list[tuple[Any, ...]],
) -> None:
    """Append one typed frame instead of binding every row through executemany."""
    frame = pd.DataFrame.from_records(
        batch,
        columns=_BRONZE_COLUMN_NAMES,
        coerce_float=False,
    )
    connection.append("bronze", frame)
    batch.clear()


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


def _validated_receipt_identity(
    *,
    source: Path,
    receipt: Path,
    output: Path,
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    if not source.is_file():
        raise EurostatBulkDecodeError("Eurostat distribution path is not a regular file")
    if not receipt.is_file():
        raise EurostatBulkDecodeError("Eurostat receipt path is not a regular file")
    if output.exists():
        raise EurostatBulkDecodeError("Eurostat Bronze output must be a fresh path")
    if not isinstance(descriptor, Mapping) or set(descriptor) != {
        "id", "sha256", "size_bytes"
    }:
        raise EurostatBulkDecodeError("Eurostat receipt descriptor has invalid fields")
    receipt_file_id = descriptor.get("id")
    receipt_sha256 = descriptor.get("sha256")
    receipt_size_bytes = descriptor.get("size_bytes")
    if not isinstance(receipt_file_id, str) or not _DRIVE_FILE_ID_RE.fullmatch(receipt_file_id):
        raise EurostatBulkDecodeError("Eurostat receipt Drive identity is invalid")
    if not isinstance(receipt_sha256, str) or not _SHA256_RE.fullmatch(receipt_sha256):
        raise EurostatBulkDecodeError("Eurostat receipt SHA-256 is invalid")
    if type(receipt_size_bytes) is not int or receipt_size_bytes <= 0:
        raise EurostatBulkDecodeError("Eurostat receipt size is invalid")
    if receipt.stat().st_size != receipt_size_bytes or _file_sha256(receipt) != receipt_sha256:
        raise EurostatBulkDecodeError("Eurostat receipt bytes do not match the immutable descriptor")
    try:
        accepted = json.loads(receipt.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EurostatBulkDecodeError(f"Eurostat receipt is not valid UTF-8 JSON: {exc}") from exc
    if (
        not isinstance(accepted, dict)
        or accepted.get("schema_version") != 1
        or accepted.get("source_id") != "eurostat"
        or accepted.get("accepted") is not True
        or accepted.get("kind") != "full_distribution"
    ):
        raise EurostatBulkDecodeError("receipt is not an accepted Eurostat full distribution")
    distribution = accepted.get("distribution")
    raw = accepted.get("raw")
    inspection = accepted.get("inspection")
    if not isinstance(distribution, dict) or not isinstance(raw, dict):
        raise EurostatBulkDecodeError("Eurostat receipt is missing distribution or raw metadata")
    if not isinstance(inspection, dict) or inspection.get("status") != "complete":
        raise EurostatBulkDecodeError("Eurostat receipt inspection is not complete")
    distribution_id = distribution.get("dataset_id")
    if not _bounded_text(distribution_id, 2048):
        raise EurostatBulkDecodeError("Eurostat dataset identity is invalid")
    original_dataset_id = distribution.get("original_dataset_id")
    partition_id = distribution.get("partition_id")
    partition_selection = distribution.get("partition_selection")
    partitioned = original_dataset_id is not None or partition_id is not None
    if partitioned:
        if (
            not _bounded_text(original_dataset_id, 2048)
            or not _bounded_text(partition_id, 2048)
            or not isinstance(partition_selection, dict)
        ):
            raise EurostatBulkDecodeError("Eurostat partition identity is invalid")
        dataset_id = original_dataset_id
        partition_selection_json = json.dumps(
            partition_selection,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    else:
        dataset_id = distribution_id
        partition_id = None
        if partition_selection is not None:
            raise EurostatBulkDecodeError(
                "unpartitioned Eurostat receipt has a partition selection"
            )
        partition_selection_json = None
    empty_partition = inspection.get("empty_partition") is True
    if empty_partition and not partitioned:
        raise EurostatBulkDecodeError(
            "only an accepted constrained Eurostat partition may be empty"
        )
    if distribution.get("kind") != "eurostat_tsv_gzip":
        raise EurostatBulkDecodeError("Eurostat receipt is not a TSV.GZ data distribution")
    url = distribution.get("url")
    if not isinstance(url, str) or urlsplit(url).scheme != "https" or not urlsplit(url).hostname:
        raise EurostatBulkDecodeError("Eurostat receipt distribution URL is invalid")
    if not isinstance(distribution.get("params"), dict):
        raise EurostatBulkDecodeError("Eurostat receipt distribution parameters are invalid")
    source_version = distribution.get("version")
    if source_version is not None and not _bounded_text(source_version, 1024):
        raise EurostatBulkDecodeError("Eurostat source version is invalid")
    raw_sha256 = raw.get("sha256")
    raw_file_id = raw.get("id")
    raw_name = raw.get("name")
    raw_md5 = raw.get("md5")
    raw_size = raw.get("size_bytes")
    if not isinstance(raw_sha256, str) or not _SHA256_RE.fullmatch(raw_sha256):
        raise EurostatBulkDecodeError("Eurostat distribution SHA-256 is invalid")
    if not isinstance(raw_file_id, str) or not _DRIVE_FILE_ID_RE.fullmatch(raw_file_id):
        raise EurostatBulkDecodeError("Eurostat Drive file identity is invalid")
    if (
        raw_name != f"raw-{raw_sha256}.bin"
        or not isinstance(raw_md5, str)
        or not _MD5_RE.fullmatch(raw_md5)
        or type(raw_size) is not int
        or raw_size <= 0
        or source.stat().st_size != raw_size
    ):
        raise EurostatBulkDecodeError("Eurostat raw descriptor is invalid")
    retrieved_at_utc = accepted.get("retrieved_at_utc")
    try:
        parsed = datetime.fromisoformat(retrieved_at_utc.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise EurostatBulkDecodeError("Eurostat retrieval timestamp is invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise EurostatBulkDecodeError("Eurostat retrieval timestamp must be UTC")
    return {
        "dataset_id": dataset_id,
        "distribution_id": distribution_id,
        "partition_id": partition_id,
        "partition_selection_json": partition_selection_json,
        "empty_partition": empty_partition,
        "source_version": source_version,
        "retrieved_at_utc": retrieved_at_utc,
        "raw_sha256": raw_sha256,
        "raw_file_id": raw_file_id,
        "receipt_sha256": receipt_sha256,
        "receipt_file_id": receipt_file_id,
        "receipt_size_bytes": receipt_size_bytes,
    }


def _bounded_text(value: Any, limit: int) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= limit
        and not any(ord(character) < 32 for character in value)
    )


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
    parser.add_argument("receipt", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--receipt-file-id", required=True)
    parser.add_argument("--receipt-sha256", required=True)
    parser.add_argument("--receipt-size-bytes", required=True, type=int)
    args = parser.parse_args(argv)
    try:
        report = decode_full_distribution(
            args.source,
            args.receipt,
            args.output,
            receipt_descriptor={
                "id": args.receipt_file_id,
                "sha256": args.receipt_sha256,
                "size_bytes": args.receipt_size_bytes,
            },
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
