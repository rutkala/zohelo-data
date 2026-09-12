"""Plan and checkpoint the next GUS BDL subgroup for Web bulk extraction."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

import duckdb
from googleapiclient.http import MediaInMemoryUpload

from drive_release_store import DriveReleaseStore
from release_protocol import ReleaseProtocolError, read_current_release_manifest
from storage_manager import StorageManager

_SUBJECT_RE = re.compile(r"^[KGP][0-9]+$")
_SUBGROUP_RE = re.compile(r"^P[0-9]+$")
_BDL_PLATFORM_FOLDER = "bdl-platform"
_PROCESSED_STATUSES = {
    "landed",
    "below_bulk_threshold",
    "no_bulk_control",
    "web_bulk_unsupported",
}


def _number(identifier: str) -> int:
    return int(identifier[1:])


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _list_named(storage: StorageManager, parent_id: str, *, mime_type: str | None = None) -> list[dict[str, Any]]:
    query = f"'{_escape(parent_id)}' in parents and trashed=false"
    if mime_type:
        query += f" and mimeType='{_escape(mime_type)}'"
    results: list[dict[str, Any]] = []
    token = None
    while True:
        args: dict[str, Any] = {"q": query, "spaces": "drive", "fields": "nextPageToken, files(id,name,mimeType,appProperties,trashed)"}
        if token:
            args["pageToken"] = token
        response = storage.drive_service.files().list(**args).execute(num_retries=4)
        results.extend(response.get("files", []))
        token = response.get("nextPageToken")
        if not token:
            return results


def _bulk_roots(storage: StorageManager) -> tuple[str, str]:
    session = storage.begin_write_session()
    landing = storage.resolve_zone("landing", create=False)
    bulk = storage.get_or_create_nested_folder(["gus_bdl", "web_bulk"], root_id=landing, write_session=session)
    control = storage.get_or_create_nested_folder(["_control"], root_id=bulk, write_session=session)
    return bulk, control


def _require_production_context() -> None:
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("BDL bulk planning must run in the main-branch Actions workflow")


def _durable_status(storage: StorageManager, bulk_id: str, control_id: str) -> tuple[set[str], set[str]]:
    markers = _list_named(storage, control_id)
    statuses = {}
    for item in markers:
        name = item.get("name", "")
        subgroup_id = name.removesuffix(".json") if name.endswith(".json") else ""
        properties = item.get("appProperties") or {}
        status = properties.get("status")
        if (
            _SUBGROUP_RE.fullmatch(subgroup_id)
            and properties.get("source_id") == "gus_bdl"
            and properties.get("transport") == "web_bulk"
            and properties.get("subgroup_id") == subgroup_id
            and status in _PROCESSED_STATUSES
        ):
            statuses[subgroup_id] = status
    landed = {subgroup_id for subgroup_id, status in statuses.items() if status == "landed"}
    processed = set(statuses)
    return landed, processed


def _subject_catalogue(storage: StorageManager, root_id: str) -> tuple[str, list[dict[str, Any]]]:
    """Read only the pinned subject dimension from the current BDL release."""
    release_root = storage.get_or_create_nested_folder([_BDL_PLATFORM_FOLDER], root_id=root_id)
    store = DriveReleaseStore(storage, release_root)
    manifest = read_current_release_manifest(store, release_root)
    if manifest.get("release_scope") != "bdl_platform":
        raise ReleaseProtocolError("current BDL release has an unexpected scope")
    matches = [
        dataset for dataset in manifest.get("datasets", [])
        if isinstance(dataset, dict) and dataset.get("dataset_id") == "dim_bdl_subject"
    ]
    if len(matches) != 1:
        raise ReleaseProtocolError("current BDL release must contain one dim_bdl_subject dataset")
    files = matches[0].get("files")
    if not isinstance(files, list) or len(files) != 1 or not isinstance(files[0], dict):
        raise ReleaseProtocolError("dim_bdl_subject must contain one release file")
    descriptor = files[0]
    file_id = descriptor.get("id")
    expected_size = descriptor.get("size")
    expected_sha = descriptor.get("sha256")
    if (
        not isinstance(file_id, str)
        or not file_id
        or not isinstance(expected_size, int)
        or isinstance(expected_size, bool)
        or expected_size <= 0
        or not isinstance(expected_sha, str)
        or not re.fullmatch(r"[0-9a-f]{64}", expected_sha)
    ):
        raise ReleaseProtocolError("dim_bdl_subject release descriptor is invalid")
    raw = store.read(file_id)
    if len(raw) != expected_size or sha256(raw).hexdigest() != expected_sha:
        raise ReleaseProtocolError("dim_bdl_subject release file failed checksum validation")
    with tempfile.NamedTemporaryFile(suffix=".parquet") as handle:
        handle.write(raw)
        handle.flush()
        with duckdb.connect() as connection:
            rows = connection.execute(
                """
                select
                    cast(subject_key as varchar),
                    cast(parent_subject_id as varchar),
                    cast(subject_name as varchar),
                    cast(has_variables as boolean)
                from read_parquet(?)
                """,
                [handle.name],
            ).fetchall()
    return manifest["release_id"], [
        {
            "subject_id": row[0],
            "parent_subject_id": row[1],
            "subject_name": row[2],
            "has_variables": row[3],
        }
        for row in rows
    ]


def _catalogue_candidates(subjects: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    """Build stable Web URLs and expose every malformed subgroup as a blocker."""
    by_id: dict[str, dict[str, Any]] = {}
    duplicate_ids: set[str] = set()
    for subject in subjects:
        identifier = subject.get("subject_id")
        if not isinstance(identifier, str) or not _SUBJECT_RE.fullmatch(identifier):
            continue
        if identifier in by_id:
            duplicate_ids.add(identifier)
        by_id[identifier] = subject

    candidates: list[dict[str, Any]] = []
    invalid: set[str] = set(duplicate_ids)
    for identifier in sorted(
        (subject_id for subject_id in by_id if _SUBGROUP_RE.fullmatch(subject_id)),
        key=lambda subject_id: (_number(subject_id), subject_id),
    ):
        if identifier in duplicate_ids:
            continue
        path: list[str] = []
        seen: set[str] = set()
        current: str | None = identifier
        while current:
            if current in seen or current not in by_id or current in duplicate_ids:
                invalid.add(identifier)
                break
            seen.add(current)
            path.append(current)
            parent = by_id[current].get("parent_subject_id")
            if parent in (None, ""):
                current = None
            elif not isinstance(parent, str) or not _SUBJECT_RE.fullmatch(parent):
                invalid.add(identifier)
                break
            else:
                current = parent
        if identifier in invalid:
            continue
        path.reverse()
        categories = [part for part in path if part.startswith("K")]
        groups = [part for part in path if part.startswith("G")]
        subgroups = [part for part in path if part.startswith("P")]
        if (
            len(categories) != 1
            or len(groups) != 1
            or subgroups != [identifier]
            or path.index(categories[0]) > path.index(groups[0])
        ):
            invalid.add(identifier)
            continue
        category_id, group_id = categories[0], groups[0]
        subject = by_id[identifier]
        candidates.append({
            "subgroup_id": identifier,
            "subgroup_name": subject.get("subject_name"),
            "category_id": category_id,
            "group_id": group_id,
            "url": f"https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/{_number(category_id)}/{_number(group_id)}/{_number(identifier)}",
            "path": path,
            "has_variables": subject.get("has_variables"),
        })
    return candidates, sorted(invalid, key=lambda value: (_number(value), value))


def plan() -> dict[str, Any]:
    _require_production_context()
    storage = StorageManager(allow_interactive_auth=False)
    root_id = storage.resolve_root(create=False)
    bulk_id, control_id = _bulk_roots(storage)
    landed, processed = _durable_status(storage, bulk_id, control_id)
    release_id, subjects = _subject_catalogue(storage, root_id)
    candidates, invalid = _catalogue_candidates(subjects)
    remaining = [item for item in candidates if item["subgroup_id"] not in processed]
    candidate = remaining[0] if remaining else None
    status = "candidate" if candidate else ("catalogue_invalid" if invalid else "complete")
    return {
        "status": status,
        "catalogue_release_id": release_id,
        "catalogue_requests": 0,
        "catalogue_subgroups": len(candidates) + len(invalid),
        "catalogue_valid_subgroups": len(candidates),
        "catalogue_invalid_subgroups": len(invalid),
        "invalid_subgroup_examples": invalid[:20],
        "landed_subgroups": len(landed),
        "web_checked_nonbulk_subgroups": len(processed - landed),
        "remaining_subgroups": len(remaining),
        "candidate": candidate,
        "bulk_root_id": bulk_id,
        "control_root_id": control_id,
    }


def mark(subgroup_id: str, status: str, detail: str | None = None) -> dict[str, Any]:
    _require_production_context()
    if not _SUBGROUP_RE.fullmatch(subgroup_id):
        raise ValueError("BDL subgroup must use P<digits> identity")
    allowed = _PROCESSED_STATUSES - {"landed"}
    if status not in allowed:
        raise ValueError(f"Unsupported durable BDL bulk marker status: {status}")
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    _, control_id = _bulk_roots(storage)
    payload = {"format_version": 1, "source_id": "gus_bdl", "transport": "web_bulk", "subgroup_id": subgroup_id, "status": status, "detail": detail, "checked_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    name = f"{subgroup_id}.json"
    query = f"name='{_escape(name)}' and '{_escape(control_id)}' in parents and trashed=false"
    found = storage.drive_service.files().list(q=query, spaces="drive", fields="files(id,name)").execute(num_retries=4).get("files", [])
    media = MediaInMemoryUpload(raw, mimetype="application/json", resumable=False)
    if len(found) > 1:
        raise RuntimeError("Ambiguous BDL bulk status marker")
    properties = {
        "source_id": "gus_bdl",
        "transport": "web_bulk",
        "subgroup_id": subgroup_id,
        "status": status,
    }
    if found:
        response = storage.drive_service.files().update(fileId=found[0]["id"], body={"appProperties": properties}, media_body=media, fields="id,name,size,appProperties").execute(num_retries=4)
    else:
        response = storage.drive_service.files().create(body={"name": name, "parents": [control_id], "appProperties": properties}, media_body=media, fields="id,name,size,appProperties").execute(num_retries=4)
    if (response.get("appProperties") or {}) != properties:
        raise RuntimeError("BDL bulk status marker metadata did not verify")
    return {"status": "marked", "marker": payload, "file_id": response["id"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    plan_parser = sub.add_parser("plan")
    plan_parser.add_argument("--output", type=Path)
    mark_parser = sub.add_parser("mark")
    mark_parser.add_argument("--subgroup", required=True)
    mark_parser.add_argument("--status", required=True)
    mark_parser.add_argument("--detail")
    args = parser.parse_args()
    result = plan() if args.command == "plan" else mark(args.subgroup, args.status, args.detail)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered, flush=True)
    if args.command == "plan" and args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
