#!/usr/bin/env python3
"""Restore one verified DBW Bronze snapshot from Drive for dbt consumption."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import uuid
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from storage_manager import StorageManager


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _folder(storage: StorageManager, parent_id: str, name: str) -> str:
    query = (
        f"name='{_escape(name)}' and "
        "mimeType='application/vnd.google-apps.folder' and "
        f"'{_escape(parent_id)}' in parents and trashed=false"
    )
    files = storage.drive_service.files().list(
        q=query, spaces="drive", fields="files(id,name,mimeType,trashed)"
    ).execute(num_retries=4).get("files", [])
    matches = [item for item in files if item.get("name") == name and not item.get("trashed")]
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one DBW folder {name!r}; found {len(matches)}.")
    return matches[0]["id"]


def _list_files(storage: StorageManager, parent_id: str) -> list[dict[str, Any]]:
    query = f"'{_escape(parent_id)}' in parents and trashed=false"
    result: list[dict[str, Any]] = []
    token = None
    while True:
        kwargs: dict[str, Any] = {
            "q": query,
            "spaces": "drive",
            "pageSize": 1000,
            "fields": "nextPageToken,files(id,name,size,md5Checksum,appProperties,trashed)",
        }
        if token:
            kwargs["pageToken"] = token
        page = storage.drive_service.files().list(**kwargs).execute(num_retries=4)
        result.extend(page.get("files", []))
        token = page.get("nextPageToken")
        if not token:
            return result


def _unique(files: list[dict[str, Any]], name: str) -> dict[str, Any]:
    matches = [item for item in files if item.get("name") == name and not item.get("trashed")]
    if len(matches) != 1:
        raise RuntimeError(f"Expected exactly one verified DBW object {name!r}; found {len(matches)}.")
    return matches[0]


def _download_verified(storage: StorageManager, item: dict[str, Any], path: Path) -> bytes:
    props = item.get("appProperties") or {}
    expected_sha = props.get("sha256", "")
    expected_md5 = item.get("md5Checksum", "")
    try:
        expected_size = int(item.get("size", -1))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Invalid Drive size for {item.get('name')!r}.") from exc
    if (
        re.fullmatch(r"[0-9a-f]{64}", expected_sha) is None
        or re.fullmatch(r"[0-9a-f]{32}", expected_md5) is None
        or expected_size <= 0
    ):
        raise RuntimeError(f"Drive object lacks integrity metadata: {item.get('name')!r}.")
    raw = storage.drive_service.files().get_media(fileId=item["id"]).execute(num_retries=4)
    if (
        len(raw) != expected_size
        or hashlib.sha256(raw).hexdigest() != expected_sha
        or hashlib.md5(raw).hexdigest() != expected_md5
    ):
        raise RuntimeError(f"Drive object failed byte verification: {item.get('name')!r}.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def _completion(raw: bytes, release_id: str) -> dict[str, Any]:
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("DBW Bronze completion marker is not valid UTF-8 JSON.") from exc
    required = {
        "schema_version": 1,
        "record_type": "gus_dbw_bronze_completion",
        "source_id": "gus_dbw",
        "status": "complete_native_snapshot",
        "release_id": release_id,
    }
    if any(document.get(key) != value for key, value in required.items()):
        raise RuntimeError("DBW Bronze completion marker does not match the selected release.")
    completed = document.get("completed_indicators")
    if (
        not isinstance(completed, int)
        or completed <= 0
        or document.get("observation_partitions") != completed
        or document.get("dictionary_partitions") != completed
        or re.fullmatch(r"[0-9a-f]{64}", document.get("observation_inventory_sha256", "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", document.get("dictionary_inventory_sha256", "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", document.get("observation_content_inventory_sha256", "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", document.get("dictionary_content_inventory_sha256", "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", document.get("taxonomy_sha256", "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", document.get("metadata_sha256", "")) is None
        or re.fullmatch(r"[0-9a-f]{64}", document.get("consolidated_dictionary_sha256", "")) is None
    ):
        raise RuntimeError("DBW Bronze completion marker does not reconcile all partitions.")
    return document


def _tree_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def _inventory_sha256(names: set[str]) -> str:
    return hashlib.sha256("\n".join(sorted(names)).encode("utf-8")).hexdigest()


def _content_inventory_sha256(items: list[dict[str, Any]]) -> str:
    entries: list[str] = []
    for item in items:
        name = item.get("name")
        digest = (item.get("appProperties") or {}).get("sha256")
        if not isinstance(name, str) or re.fullmatch(r"[0-9a-f]{64}", digest or "") is None:
            raise RuntimeError("Remote DBW partition lacks a verified content identity.")
        entries.append(f"{name}\0{digest}")
    return hashlib.sha256("\n".join(sorted(entries)).encode("utf-8")).hexdigest()


def restore_dbw_release(
    storage: StorageManager, release_id: str, data_root: Path
) -> dict[str, Any]:
    if re.fullmatch(r"[0-9a-f]{64}", release_id) is None:
        raise ValueError("DBW Bronze release ID must be a lowercase SHA-256 value.")

    bronze = storage.resolve_zone("bronze", create=False)
    dbw = _folder(storage, bronze, "gus_dbw")
    releases = _folder(storage, dbw, "releases")
    release = _folder(storage, releases, release_id)
    folders = {
        name: _folder(storage, release, name)
        for name in ("observations", "dictionaries", "taxonomy", "metadata", "_control")
    }
    inventories = {name: _list_files(storage, folder_id) for name, folder_id in folders.items()}

    marker_name = f"bronze-complete-v1-{release_id}.json"
    marker_item = _unique(inventories["_control"], marker_name)
    marker_raw = _download_verified(storage, marker_item, data_root / ".dbw-marker-check")
    (data_root / ".dbw-marker-check").unlink(missing_ok=True)
    marker = _completion(marker_raw, release_id)
    completed = marker["completed_indicators"]

    observations = [
        item for item in inventories["observations"]
        if re.fullmatch(r"part_\d+\.parquet", item.get("name", ""))
    ]
    dictionary_parts = [
        item for item in inventories["dictionaries"]
        if re.fullmatch(r"dict_\d+\.parquet", item.get("name", ""))
    ]
    if len(observations) != completed or len({item["name"] for item in observations}) != completed:
        raise RuntimeError("Remote DBW observation partitions do not reconcile completion.")
    if len(dictionary_parts) != completed or len({item["name"] for item in dictionary_parts}) != completed:
        raise RuntimeError("Remote DBW dictionary partitions do not reconcile completion.")
    if marker.get("observation_inventory_sha256") != _inventory_sha256(
        {item["name"] for item in observations}
    ):
        raise RuntimeError("Remote DBW observation inventory hash does not reconcile completion.")
    if marker.get("dictionary_inventory_sha256") != _inventory_sha256(
        {item["name"] for item in dictionary_parts}
    ):
        raise RuntimeError("Remote DBW dictionary inventory hash does not reconcile completion.")
    if marker.get("observation_content_inventory_sha256") != _content_inventory_sha256(
        observations
    ):
        raise RuntimeError("Remote DBW observation content inventory does not reconcile completion.")
    if marker.get("dictionary_content_inventory_sha256") != _content_inventory_sha256(
        dictionary_parts
    ):
        raise RuntimeError("Remote DBW dictionary content inventory does not reconcile completion.")

    fixed_items = {
        "taxonomy_sha256": _unique(inventories["taxonomy"], "br_dbw_indicators.parquet"),
        "metadata_sha256": _unique(inventories["metadata"], "br_dbw_metadata.parquet"),
        "consolidated_dictionary_sha256": _unique(
            inventories["dictionaries"], "br_dbw_dictionaries.parquet"
        ),
    }
    for marker_field, item in fixed_items.items():
        if marker.get(marker_field) != (item.get("appProperties") or {}).get("sha256"):
            raise RuntimeError(f"Remote DBW {marker_field} does not reconcile completion.")

    target = data_root / "02_bronze" / "gus_dbw" / "releases" / release_id
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.parent / f".restore-{release_id}-{uuid.uuid4()}"
    staging.mkdir(parents=False)
    try:
        for item in observations:
            _download_verified(storage, item, staging / "observations" / item["name"])
        for item in dictionary_parts:
            _download_verified(storage, item, staging / "dictionaries" / item["name"])
        fixed = (
            ("taxonomy", "br_dbw_indicators.parquet", "taxonomy_sha256"),
            ("metadata", "br_dbw_metadata.parquet", "metadata_sha256"),
            ("dictionaries", "br_dbw_dictionaries.parquet", "consolidated_dictionary_sha256"),
        )
        for folder, name, marker_field in fixed:
            _download_verified(storage, fixed_items[marker_field], staging / folder / name)
        (staging / "_control").mkdir(parents=True, exist_ok=True)
        (staging / "_control" / marker_name).write_bytes(marker_raw)

        if target.exists():
            if _tree_sha256(target) != _tree_sha256(staging):
                raise RuntimeError(
                    f"Existing local DBW release differs; verified staging was preserved at {staging}."
                )
            shutil.rmtree(staging)
        else:
            os.replace(staging, target)
    except Exception:
        if staging.exists() and not any(staging.iterdir()):
            staging.rmdir()
        raise
    return {
        "status": "restored_complete_native_snapshot",
        "release_id": release_id,
        "completed_indicators": completed,
        "local_path": str(target),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-id", default=os.environ.get("ZOHELO_DBW_BRONZE_RELEASE_ID"))
    parser.add_argument(
        "--data-root", type=Path,
        default=Path(os.environ.get("ZOHELO_DATA_ROOT", "/tmp/zohelo_data")),
    )
    args = parser.parse_args()
    if not args.release_id:
        parser.error("--release-id or ZOHELO_DBW_BRONZE_RELEASE_ID is required")
    storage = StorageManager(allow_interactive_auth=False)
    result = restore_dbw_release(storage, args.release_id, args.data_root.resolve())
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
