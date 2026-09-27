"""Reset only the GUS BDL Web-bulk Landing namespace for a clean bootstrap."""
from __future__ import annotations

import json
import os

from storage_manager import StorageManager

FOLDER_MIME = "application/vnd.google-apps.folder"


def _quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _list_children(storage: StorageManager, parent_id: str) -> list[dict]:
    result: list[dict] = []
    token = None
    while True:
        response = storage.drive_service.files().list(
            q=f"'{_quote(parent_id)}' in parents and trashed=false",
            spaces="drive",
            pageSize=100,
            pageToken=token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="nextPageToken,incompleteSearch,files(id,name,mimeType,parents,trashed)",
        ).execute(num_retries=4)
        if response.get("incompleteSearch") not in (None, False):
            raise RuntimeError("Incomplete Drive listing while resetting BDL Web bulk")
        items = response.get("files")
        if not isinstance(items, list):
            raise RuntimeError("Malformed Drive listing while resetting BDL Web bulk")
        for item in items:
            if item.get("parents") != [parent_id] or item.get("trashed") is not False:
                raise RuntimeError("Drive item escaped expected BDL Web bulk parent")
            result.append(item)
        token = response.get("nextPageToken")
        if not token:
            return result


def _unique_child_folder(storage: StorageManager, parent_id: str, name: str) -> dict | None:
    matches = [item for item in _list_children(storage, parent_id) if item.get("name") == name]
    if len(matches) > 1:
        raise RuntimeError(f"Ambiguous Drive path while resetting BDL Web bulk: {name}")
    if not matches:
        return None
    item = matches[0]
    if item.get("mimeType") != FOLDER_MIME:
        raise RuntimeError(f"Expected BDL Web bulk folder {name}, found non-folder")
    return item


def main() -> int:
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise PermissionError("BDL Web bulk reset must run in main-branch GitHub Actions")
    os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    storage.authorize_writes()
    landing = storage.resolve_zone("landing", create=False)
    bdl = _unique_child_folder(storage, landing, "gus_bdl")
    if bdl is None:
        print(json.dumps({"status": "bdl_web_bulk_reset", "deleted_direct_items": 0, "reason": "no_gus_bdl_landing"}))
        return 0
    web_bulk = _unique_child_folder(storage, bdl["id"], "web_bulk")
    if web_bulk is None:
        print(json.dumps({"status": "bdl_web_bulk_reset", "deleted_direct_items": 0, "reason": "no_web_bulk"}))
        return 0
    children = _list_children(storage, web_bulk["id"])
    for item in children:
        storage.drive_service.files().delete(fileId=item["id"], supportsAllDrives=True).execute(num_retries=4)
    remaining = _list_children(storage, web_bulk["id"])
    if remaining:
        raise RuntimeError("BDL Web bulk reset verification failed")
    print(json.dumps({"status": "bdl_web_bulk_reset", "deleted_direct_items": len(children)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
