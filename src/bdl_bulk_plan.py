"""Plan and checkpoint the next GUS BDL subgroup for Web bulk extraction."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from typing import Any

from googleapiclient.http import MediaInMemoryUpload
import requests

from storage_manager import StorageManager

_API = "https://bdl.stat.gov.pl/api/v1/subjects"
_SUBJECT_RE = re.compile(r"^[KGP][0-9]+$")
_SUBGROUP_RE = re.compile(r"^P[0-9]+$")


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
        args: dict[str, Any] = {"q": query, "spaces": "drive", "fields": "nextPageToken, files(id,name,mimeType,trashed)"}
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


def _durable_status(storage: StorageManager, bulk_id: str, control_id: str) -> tuple[set[str], set[str]]:
    folders = _list_named(storage, bulk_id, mime_type=storage.FOLDER_MIME_TYPE)
    landed = {item["name"] for item in folders if _SUBGROUP_RE.fullmatch(item.get("name", ""))}
    markers = _list_named(storage, control_id)
    processed = {item["name"].removesuffix(".json") for item in markers if item.get("name", "").endswith(".json") and _SUBGROUP_RE.fullmatch(item["name"].removesuffix(".json"))}
    return landed, processed


def _fetch_page(session: requests.Session, parent_id: str | None, page: int) -> dict[str, Any]:
    params: dict[str, Any] = {"lang": "pl", "page": page, "page-size": 100, "sort": "Id"}
    if parent_id:
        params["parent-id"] = parent_id
    response = session.get(_API, params=params, timeout=30, allow_redirects=False)
    if response.status_code != 200:
        raise RuntimeError(f"BDL subject catalogue returned HTTP {response.status_code}")
    payload = response.json()
    if not isinstance(payload, dict) or not isinstance(payload.get("results"), list):
        raise RuntimeError("BDL subject catalogue returned an invalid response")
    return payload


def _subjects(session: requests.Session, parent_id: str | None = None) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page = 0
    while True:
        payload = _fetch_page(session, parent_id, page)
        results = payload["results"]
        for item in results:
            if not isinstance(item, dict) or not _SUBJECT_RE.fullmatch(str(item.get("id", ""))):
                raise RuntimeError("BDL subject catalogue contains an invalid subject")
            items.append(item)
        total = payload.get("totalRecords")
        page_size = payload.get("pageSize", 100)
        if not isinstance(total, int) or total < 0:
            raise RuntimeError("BDL subject catalogue totalRecords is invalid")
        if len(items) >= total or not results:
            break
        if not isinstance(page_size, int) or page_size <= 0:
            raise RuntimeError("BDL subject catalogue pageSize is invalid")
        page += 1
        if page > 100:
            raise RuntimeError("BDL subject catalogue pagination exceeded safety bound")
    unique = {str(item["id"]): item for item in items}
    return sorted(unique.values(), key=lambda item: (_number(str(item["id"])), str(item["id"])))


def _next_subgroup(session: requests.Session, excluded: set[str]) -> dict[str, Any] | None:
    roots = _subjects(session)
    def walk(item: dict[str, Any], ancestors: tuple[str, ...]) -> dict[str, Any] | None:
        identifier = str(item["id"])
        path = (*ancestors, identifier)
        if identifier.startswith("P") and identifier not in excluded:
            k_id = next((part for part in path if part.startswith("K")), None)
            g_id = next((part for part in path if part.startswith("G")), None)
            if k_id and g_id:
                return {"subgroup_id": identifier, "subgroup_name": item.get("name"), "category_id": k_id, "group_id": g_id, "url": f"https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/{_number(k_id)}/{_number(g_id)}/{_number(identifier)}", "path": list(path), "has_variables": item.get("hasVariables")}
        if not item.get("children"):
            return None
        for child in _subjects(session, identifier):
            found = walk(child, path)
            if found:
                return found
        return None
    for root in roots:
        found = walk(root, ())
        if found:
            return found
    return None


def plan() -> dict[str, Any]:
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    bulk_id, control_id = _bulk_roots(storage)
    landed, processed = _durable_status(storage, bulk_id, control_id)
    headers = {}
    api_key = os.environ.get("GUS_BDL_API_KEY", "")
    if api_key:
        if api_key.strip() != api_key or any(ord(ch) < 0x21 or ord(ch) > 0x7E for ch in api_key):
            raise ValueError("Configured BDL API credential is invalid")
        headers["X-ClientId"] = api_key
    with requests.Session() as session:
        session.headers.update(headers)
        candidate = _next_subgroup(session, landed | processed)
    return {"status": "candidate" if candidate else "complete", "landed_subgroups": len(landed), "web_checked_nonbulk_subgroups": len(processed), "candidate": candidate, "bulk_root_id": bulk_id, "control_root_id": control_id}


def mark(subgroup_id: str, status: str, detail: str | None = None) -> dict[str, Any]:
    if not _SUBGROUP_RE.fullmatch(subgroup_id):
        raise ValueError("BDL subgroup must use P<digits> identity")
    allowed = {"below_bulk_threshold", "no_bulk_control", "web_bulk_unsupported"}
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
    if found:
        response = storage.drive_service.files().update(fileId=found[0]["id"], media_body=media, fields="id,name,size").execute(num_retries=4)
    else:
        response = storage.drive_service.files().create(body={"name": name, "parents": [control_id]}, media_body=media, fields="id,name,size").execute(num_retries=4)
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
