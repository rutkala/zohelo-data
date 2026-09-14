"""Plan and apply a fail-closed production reset of GUS BDL-owned Drive state.

The operation preserves shared platform containers and all non-BDL sources. It empties
BDL Landing, campaign-control, canonical release and archived-wrapper containers, and
removes only the BDL medallion navigation folders. Production apply is restricted to
main-branch GitHub Actions and requires an exact plan SHA-256.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
from typing import Any

from storage_manager import StorageManager

FOLDER_MIME = "application/vnd.google-apps.folder"
LAYERS = ("02_bronze", "03_silver", "04_gold")
MAX_PAGES = 1000


class BDLResetError(RuntimeError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _list_children(storage: StorageManager, parent_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    token = None
    seen: set[str] = set()
    for _ in range(MAX_PAGES):
        response = storage.drive_service.files().list(
            q=f"'{_quote(parent_id)}' in parents and trashed=false",
            spaces="drive",
            pageSize=100,
            pageToken=token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="nextPageToken,incompleteSearch,files(id,name,mimeType,parents,trashed)",
        ).execute(num_retries=4)
        if not isinstance(response, dict) or response.get("incompleteSearch") not in (None, False):
            raise BDLResetError("Drive returned an incomplete or malformed listing")
        page = response.get("files")
        if not isinstance(page, list):
            raise BDLResetError("Drive listing omitted files")
        for item in page:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("id"), str)
                or not isinstance(item.get("name"), str)
                or item.get("parents") != [parent_id]
                or item.get("trashed") is not False
            ):
                raise BDLResetError("Drive listing contained malformed or escaped metadata")
            items.append(item)
        next_token = response.get("nextPageToken")
        if next_token in (None, ""):
            return sorted(items, key=lambda item: (item["name"], item["id"]))
        if not isinstance(next_token, str) or next_token in seen:
            raise BDLResetError("Drive returned an invalid/repeated page token")
        seen.add(next_token)
        token = next_token
    raise BDLResetError("Drive listing exceeded page bound")


def _unique_folder(storage: StorageManager, parent_id: str, name: str, *, required: bool = True) -> dict[str, Any] | None:
    matches = [item for item in _list_children(storage, parent_id) if item["name"] == name]
    if len(matches) > 1:
        raise BDLResetError(f"Ambiguous Drive path: duplicate folder {name!r}")
    if not matches:
        if required:
            raise BDLResetError(f"Required Drive folder is missing: {name!r}")
        return None
    item = matches[0]
    if item.get("mimeType") != FOLDER_MIME:
        raise BDLResetError(f"Expected folder {name!r}, found MIME {item.get('mimeType')!r}")
    return item


def _path(storage: StorageManager, root_id: str, names: list[str], *, required: bool = True) -> dict[str, Any] | None:
    current_id = root_id
    current = None
    for index, name in enumerate(names):
        current = _unique_folder(storage, current_id, name, required=required if index == len(names) - 1 else True)
        if current is None:
            return None
        current_id = current["id"]
    return current


def _target(path: str, folder: dict[str, Any], action: str, storage: StorageManager) -> dict[str, Any]:
    if action not in {"empty_children", "delete_folder"}:
        raise ValueError(action)
    children = _list_children(storage, folder["id"])
    return {
        "path": path,
        "action": action,
        "folder": {"id": folder["id"], "name": folder["name"], "mimeType": folder["mimeType"]},
        "children": [
            {"id": item["id"], "name": item["name"], "mimeType": item.get("mimeType")}
            for item in children
        ],
    }


def build_plan(storage: StorageManager) -> dict[str, Any]:
    root_id = storage.resolve_root(create=False)
    landing_id = storage.resolve_zone("landing", create=False)
    landing_bdl = _unique_folder(storage, landing_id, "gus_bdl")
    control_bdl = _path(storage, root_id, ["06_control", "source_campaigns", "gus_bdl"])
    release_bdl = _path(storage, root_id, ["releases", "bdl"])

    targets: list[dict[str, Any]] = []
    # Navigation is non-authoritative and should disappear before release targets.
    for layer in LAYERS:
        nav = _path(storage, root_id, [layer, "current", "bdl"], required=False)
        if nav is not None:
            targets.append(_target(f"{layer}/current/bdl", nav, "delete_folder", storage))
    targets.extend(
        [
            _target("01_landing/gus_bdl", landing_bdl, "empty_children", storage),
            _target("06_control/source_campaigns/gus_bdl", control_bdl, "empty_children", storage),
            _target("releases/bdl", release_bdl, "empty_children", storage),
        ]
    )
    archive = _path(storage, root_id, ["05_archive", "bdl-platform"], required=False)
    if archive is not None:
        targets.append(_target("05_archive/bdl-platform", archive, "empty_children", storage))

    paths = [target["path"] for target in targets]
    if len(paths) != len(set(paths)):
        raise BDLResetError("Reset plan contains duplicate target paths")
    return {
        "schema_version": 1,
        "source_id": "gus_bdl",
        "root_name": storage.root_name,
        "targets": targets,
    }


def _delete(storage: StorageManager, item_id: str) -> None:
    storage.drive_service.files().delete(
        fileId=item_id,
        supportsAllDrives=True,
    ).execute(num_retries=4)


def _verify_empty(storage: StorageManager, target: dict[str, Any], root_id: str) -> None:
    if target["action"] == "empty_children":
        if _list_children(storage, target["folder"]["id"]):
            raise BDLResetError(f"Reset verification failed; {target['path']} is not empty")
        return
    parts = target["path"].split("/")
    parent = _path(storage, root_id, parts[:-1]) if len(parts) > 1 else None
    if parent is None:
        raise BDLResetError(f"Reset verification parent disappeared for {target['path']}")
    if any(item["name"] == parts[-1] for item in _list_children(storage, parent["id"])):
        raise BDLResetError(f"Reset verification failed; {target['path']} still exists")


def apply_plan(storage: StorageManager, plan: dict[str, Any]) -> dict[str, Any]:
    current = build_plan(storage)
    if _canonical(current) != _canonical(plan):
        raise BDLResetError("Drive state drifted since reset plan; refusing destructive apply")
    root_id = storage.resolve_root(create=False)
    deleted = 0
    by_path: dict[str, int] = {}
    for target in plan["targets"]:
        count = 0
        if target["action"] == "delete_folder":
            _delete(storage, target["folder"]["id"])
            count = 1
        else:
            for child in target["children"]:
                _delete(storage, child["id"])
                count += 1
        deleted += count
        by_path[target["path"]] = count
    for target in plan["targets"]:
        _verify_empty(storage, target, root_id)
    return {
        "schema_version": 1,
        "source_id": "gus_bdl",
        "status": "bdl_reset_verified",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "deleted_direct_items": deleted,
        "deleted_by_path": by_path,
    }


def _production_storage() -> StorageManager:
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("Production BDL reset must run in main-branch GitHub Actions")
    os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    storage.authorize_writes()
    return storage


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    plan_parser = sub.add_parser("plan")
    plan_parser.add_argument("--output", type=Path, required=True)
    apply_parser = sub.add_parser("apply")
    apply_parser.add_argument("--plan", type=Path, required=True)
    apply_parser.add_argument("--expected-plan-sha", required=True)
    apply_parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()

    storage = _production_storage()
    if args.command == "plan":
        plan = build_plan(storage)
        data = _canonical(plan)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(data + b"\n")
        digest = sha256(data + b"\n").hexdigest()
        print(json.dumps({
            "status": "bdl_reset_planned",
            "plan_sha256": digest,
            "target_paths": [target["path"] for target in plan["targets"]],
            "direct_items": sum(
                1 if target["action"] == "delete_folder" else len(target["children"])
                for target in plan["targets"]
            ),
        }, sort_keys=True))
        return 0

    raw = args.plan.read_bytes()
    actual_sha = sha256(raw).hexdigest()
    if actual_sha != args.expected_plan_sha:
        raise BDLResetError("Reset plan SHA-256 does not match the authorized plan")
    plan = json.loads(raw.decode("utf-8"))
    if not isinstance(plan, dict) or plan.get("schema_version") != 1 or plan.get("source_id") != "gus_bdl":
        raise BDLResetError("Reset plan identity/version is invalid")
    receipt = apply_plan(storage, plan)
    receipt["plan_sha256"] = actual_sha
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
