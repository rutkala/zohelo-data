#!/usr/bin/env python3
"""Independently restore and query a complete retained DBW publication.

Reads Drive only. No publisher, write session, or developer cache is constructed.
This accepts the dated retained inventory, not complete provider coverage/lineage.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys

import duckdb
from googleapiclient.http import MediaIoBaseDownload

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
import audit_retained_dbw_bronze as audit
from prepare_retained_dbw_release import reviewed_report

SOURCE = "gus_dbw_retained_bronze"
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
TABLES = {
    "observations": "br_dbw_observations",
    "dictionaries": "br_dbw_dictionaries",
    "metadata": "br_dbw_metadata",
    "taxonomy": "br_dbw_indicators",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, *, positive=False):
    require(type(value) is int and value >= (1 if positive else 0), "Invalid integer")
    return value


def descriptor(value, maximum):
    require(isinstance(value, dict), "Invalid file descriptor")
    require(isinstance(value.get("id"), str) and
            re.fullmatch(r"[A-Za-z0-9_-]+", value["id"]), "Invalid Drive file ID")
    require(isinstance(value.get("name"), str) and bool(value["name"]), "Missing file name")
    require(integer(value.get("size"), positive=True) <= maximum, "File exceeds bound")
    require(isinstance(value.get("sha256"), str) and
            re.fullmatch(r"[0-9a-f]{64}", value["sha256"]), "Invalid file SHA-256")
    return value


def download(storage, value, path, maximum=MAX_FILE_BYTES):
    """Stream bounded bytes; check metadata and exact payload SHA-256."""
    value = descriptor(value, maximum)
    files = storage.drive_service.files()
    meta = files.get(fileId=value["id"], fields="id,name,size,trashed").execute(num_retries=4)
    require(not meta.get("trashed") and meta.get("name") == value["name"] and
            int(meta.get("size", -1)) == value["size"], "File metadata changed")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            downloader = MediaIoBaseDownload(stream, files.get_media(fileId=value["id"]),
                                            chunksize=MAX_FILE_BYTES)
            done = False
            while not done:
                _, done = downloader.next_chunk(num_retries=4)
                require(stream.tell() <= value["size"], "Downloaded file exceeds declared size")
        require(path.stat().st_size == value["size"], "Downloaded size differs")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        require(digest.hexdigest() == value["sha256"], "Downloaded SHA-256 differs")
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def pointer_bytes(storage, folder):
    matches = [f for f in audit._list_files(storage, folder)
               if f["name"] == "current-landing.json"]
    require(len(matches) == 1, "DBW query pointer is absent or ambiguous")
    item = matches[0]
    require(0 < int(item.get("size", -1)) <= 65536, "Pointer size is invalid")
    files = storage.drive_service.files()
    fields = "id,size,version,modifiedTime,md5Checksum,trashed"
    before = files.get(fileId=item["id"], fields=fields).execute(num_retries=4)
    raw = files.get_media(fileId=item["id"]).execute(num_retries=4)
    after = files.get(fileId=item["id"], fields=fields).execute(num_retries=4)
    require(before == after and not after.get("trashed"), "Pointer changed during read")
    require(len(raw) == int(after["size"]) <= 65536, "Pointer length differs")
    require(hashlib.md5(raw).hexdigest() == after.get("md5Checksum"), "Pointer checksum differs")
    return raw


def validate_contract(pointer, manifest, index, baseline, expected_snapshot, expected_code_sha):
    require(pointer.get("format_version") == 1 and pointer.get("source_id") == SOURCE and
            pointer.get("snapshot_id") == expected_snapshot, "Unexpected publication pointer")
    expected = {
        "format_version": 1, "kind": "retained_bronze_snapshot", "source_id": SOURCE,
        "snapshot_id": expected_snapshot, "code_sha": expected_code_sha,
        "status": "validated", "layer": "02_bronze",
        "coverage_status": "incomplete_retained_inventory",
        "lineage_status": "unresolved_native_to_bronze",
        "inventory_sha256": baseline["inventory_sha256"],
        "audit_run_id": baseline["run_id"],
        "indicator_count": 1550, "published_indicator_count": 1550,
        "pending_indicator_count": 0,
        "tests": {"passed": True, "rows_and_schemas_preserved": True},
    }
    require(all(manifest.get(k) == v for k, v in expected.items()), "Manifest contract mismatch")
    from retained_dbw_publication import REVIEWED_AUDIT_REPORT_SHA256
    require(manifest.get("audit_report_sha256") == REVIEWED_AUDIT_REPORT_SHA256,
            "Publication has a different reviewed audit")
    datasets = manifest.get("datasets", [])
    require(isinstance(datasets, list) and len(datasets) == 4, "Four Bronze datasets required")
    require({d.get("name") for d in datasets} == set(TABLES), "Bronze dataset membership differs")
    for dataset in datasets:
        name = dataset["name"]
        require(dataset.get("table_name") == TABLES[name], "Unexpected Bronze table name")
        require(dataset.get("columns") == baseline["schemas"][name], "Bronze columns differ")
        require(dataset.get("row_count") == baseline["measured_parquet_rows"][name],
                "Bronze row count differs from reviewed snapshot")
        require(isinstance(dataset.get("files"), list), "Dataset files are absent")
        require((len(dataset["files"]) == 0) == (name == "observations"),
                "Observation selections or fixed files are invalid")
    require(manifest.get("observation_schema") == baseline["schemas"]["observations"],
            "Observation schema differs")
    index_expected = {"format_version": 1, "kind": "retained_bronze_indicator_index",
                      **{k: expected[k] for k in ("source_id", "inventory_sha256", "indicator_count",
                                                 "published_indicator_count", "pending_indicator_count")}}
    require(all(index.get(k) == v for k, v in index_expected.items()), "Indicator index mismatch")
    indicators = index.get("indicators")
    require(isinstance(indicators, list) and len(indicators) == 1550, "Indicator inventory incomplete")
    ids = set()
    seen_files = {manifest["indicator_index"]["id"], pointer["manifest_file_id"]}
    seen_names = set()
    def register(part):
        descriptor(part, MAX_FILE_BYTES)
        require(part["id"] not in seen_files and part["name"] not in seen_names,
                "Publication reuses a fragment")
        seen_files.add(part["id"])
        seen_names.add(part["name"])
        return integer(part.get("row_count"), positive=True)
    for dataset in datasets:
        if dataset["name"] != "observations":
            require(sum(register(f) for f in dataset["files"]) == dataset["row_count"],
                    "Fixed dataset fragment totals differ")
    for item in indicators:
        identity = integer(item.get("indicator_id"), positive=True)
        require(identity not in ids and item.get("status") == "published", "Invalid indicator state")
        ids.add(identity)
        parts = item.get("parts")
        require(isinstance(parts, list) and bool(parts), "Published indicator lacks parts")
        rows = 0
        for number, part in enumerate(parts, 1):
            require(part.get("part") == number, "Indicator part sequence differs")
            rows += register(part)
        require(rows == integer(item.get("row_count"), positive=True), "Indicator row total differs")
    require(sum(item["row_count"] for item in indicators) == baseline["measured_parquet_rows"]["observations"],
            "Published observation total differs from retained inventory")
    return datasets, indicators


def check_parquet(connection, path, columns, expected_rows, indicator_id=None):
    description = connection.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    require([(r[0], r[1]) for r in description] == [(c["name"], c["type"]) for c in columns],
            "Restored Parquet schema differs")
    if indicator_id is None:
        rows = connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        require(rows == expected_rows, "Restored Parquet row count differs")
    else:
        actual = connection.execute(
            "SELECT count(*), count(indicator_id), min(indicator_id), max(indicator_id) "
            "FROM read_parquet(?)", [str(path)]).fetchone()
        require(actual == (expected_rows, expected_rows, indicator_id, indicator_id),
                "Restored indicator identity or row count differs")


def verify(storage, output, expected_snapshot, expected_code_sha):
    require(not output.exists(), "Use a fresh consumer directory")
    output.mkdir(parents=True)
    _, baseline = reviewed_report()
    control = storage.resolve_zone("control", create=False)
    campaigns = audit._folder(storage, control, "source_campaigns")
    source = audit._folder(storage, campaigns, SOURCE)
    original_pointer = pointer_bytes(storage, source)
    pointer = json.loads(original_pointer)
    manifest_desc = {"id": pointer["manifest_file_id"], "name": pointer["manifest_file_name"],
                     "size": pointer["manifest_size_bytes"], "sha256": pointer["manifest_sha256"]}
    manifest_path = download(storage, manifest_desc, output / "manifest.json", MAX_MANIFEST_BYTES)
    manifest = json.loads(manifest_path.read_bytes())
    index_path = download(storage, manifest["indicator_index"], output / "indicator-index.json")
    index = json.loads(index_path.read_bytes())
    datasets, indicators = validate_contract(pointer, manifest, index, baseline,
                                             expected_snapshot, expected_code_sha)
    checked = 0
    total_bytes = 0
    with duckdb.connect() as connection:
        connection.execute("SET memory_limit='1GB'")
        for dataset in datasets:
            work = ([(None, f) for f in dataset["files"]] if dataset["name"] != "observations"
                    else [(item["indicator_id"], f) for item in indicators for f in item["parts"]])
            for identity, part in work:
                path = download(storage, part, output / f"part-{checked}.parquet")
                check_parquet(connection, path, dataset["columns"], part["row_count"], identity)
                total_bytes += part["size"]
                checked += 1
                path.unlink()
                if checked % 100 == 0:
                    print(json.dumps({"verified_fragments": checked, "verified_bytes": total_bytes}), flush=True)
    require(pointer_bytes(storage, source) == original_pointer, "Current pointer changed during verification")
    selected = min(indicators, key=lambda i: sum(f["size"] for f in i["parts"]))
    parts = selected["parts"]
    table = f"br_dbw_observations__indicator_{selected['indicator_id']}"
    if sum(f["size"] for f in parts) > 64 * 1024 * 1024:
        parts = [parts[0]]
        table += "__part_1"
    evidence = {
        "format_version": 1, "status": "verified", "read_only": True,
        "source_id": SOURCE, "snapshot_id": expected_snapshot, "code_sha": expected_code_sha,
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "verified_fragment_count": checked, "verified_bytes": total_bytes,
        "published_indicator_count": len(indicators), "pending_indicator_count": 0,
        "measured_parquet_rows": baseline["measured_parquet_rows"],
        "coverage_status": manifest["coverage_status"], "lineage_status": manifest["lineage_status"],
        "browser_probe": {"table_name": table, "indicator_id": selected["indicator_id"],
                          "expected_rows": sum(f["row_count"] for f in parts),
                          "file_ids": [f["id"] for f in parts]},
        "portal_sql_verified": False,
    }
    audit._atomic_json(output / "consumer-evidence.json", evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--drive-root-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-snapshot", required=True)
    parser.add_argument("--expected-code-sha", required=True)
    args = parser.parse_args()
    storage = audit.StorageManager(allow_interactive_auth=False, root_id=args.drive_root_id)
    print(json.dumps(verify(storage, args.output, args.expected_snapshot, args.expected_code_sha), sort_keys=True))


if __name__ == "__main__":
    main()
