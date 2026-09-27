"""Decode accepted Eurostat JSON-stat Landing envelopes into structural rows."""
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Iterable

import duckdb

from ingestion.sources.eurostat import decode_jsonstat


OBSERVATION_COLUMNS = {
    "dataset_id": "VARCHAR",
    "dataset_label": "VARCHAR",
    "geo_code": "VARCHAR",
    "freq_code": "VARCHAR",
    "unit_code": "VARCHAR",
    "period_key": "VARCHAR",
    "dimension_key_json": "VARCHAR",
    "dimension_key_sha256": "VARCHAR",
    "value_json": "VARCHAR",
    "value_numeric": "DOUBLE",
    "status_code": "VARCHAR",
    "is_missing": "BOOLEAN",
    "source_updated_at": "TIMESTAMPTZ",
    "retrieved_at_utc": "TIMESTAMPTZ",
    "response_sha256": "VARCHAR",
    "task_id": "VARCHAR",
    "lane": "VARCHAR",
}


def _canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _numeric(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def decode_landing_files(paths: Iterable[Path], output: Path) -> dict[str, int]:
    """Decode every retained JSON-stat response, including explicit missing cells."""
    path_list = [str(Path(path)) for path in paths]
    if not path_list:
        raise ValueError("Eurostat Landing snapshot has no Parquet fragments")
    response_count = 0
    cell_count = 0
    output.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as connection:
        definitions = ", ".join(f'"{name}" {kind}' for name, kind in OBSERVATION_COLUMNS.items())
        connection.execute(f"CREATE TABLE decoded ({definitions})")
        source = connection.read_parquet(path_list, union_by_name=True)
        envelopes = source.filter("source_id = 'eurostat' AND task_kind = 'jsonstat'").project(
            "task_id, lane, retrieved_at_utc, raw_sha256, metadata_json, payload_utf8"
        ).fetchall()
        placeholders = ",".join("?" for _ in OBSERVATION_COLUMNS)
        for task_id, lane, retrieved_at, response_sha, metadata_raw, payload in envelopes:
            metadata = json.loads(metadata_raw)
            dataset_id = metadata.get("dataset")
            geo_code = metadata.get("geo")
            if not isinstance(dataset_id, str) or not dataset_id or not isinstance(geo_code, str) or not geo_code:
                raise ValueError("Eurostat accepted response omits dataset or geography evidence")
            document = json.loads(payload)
            label = document.get("label")
            updated = document.get("updated")
            response_rows = []
            for cell in decode_jsonstat(payload.encode("utf-8")):
                dimensions = cell["dimensions"]
                if dimensions.get("geo") != geo_code or not isinstance(dimensions.get("time"), str):
                    raise ValueError("Decoded Eurostat cell escaped its accepted geography/time key")
                key_json = _canonical(dimensions)
                value = cell["value"]
                response_rows.append((
                    dataset_id, label if isinstance(label, str) else dataset_id, geo_code,
                    dimensions.get("freq"), dimensions.get("unit"), dimensions["time"],
                    key_json, sha256(key_json.encode("utf-8")).hexdigest(), _canonical(value),
                    _numeric(value), cell["status"], cell["is_missing"], updated,
                    retrieved_at, response_sha, task_id, lane,
                ))
            if response_rows:
                connection.executemany(f"INSERT INTO decoded VALUES ({placeholders})", response_rows)
                cell_count += len(response_rows)
            response_count += 1
        if not cell_count:
            raise ValueError("Eurostat Landing snapshot has no decoded JSON-stat cells")
        escaped = str(output).replace("'", "''")
        connection.execute(f"COPY decoded TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    return {"decoded_response_count": response_count, "decoded_cell_count": cell_count}
