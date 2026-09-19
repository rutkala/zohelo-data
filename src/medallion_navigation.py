"""Medallion layer navigation for 02_bronze, 03_silver, and 04_gold.

Maintains non-authoritative Drive shortcuts and a Drive-native navigation index
derived from verified current release manifests, covering single-file and
multi-file datasets without copying Parquet bytes.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut"
CURRENT_NAV_DIR = "current"
ALL_MEDALLION_LAYERS = ("02_bronze", "03_silver", "04_gold")


class NavigationError(RuntimeError):
    """Base exception for navigation errors."""


class StaleNavigationError(NavigationError):
    """Raised when navigation links or index do not match the current release."""


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")


def _get_drive_service(store_or_storage: Any) -> Any:
    return getattr(store_or_storage, "drive_service", None) or getattr(
        getattr(store_or_storage, "storage", None), "drive_service", None
    )


def _paged_items(store_or_storage: Any, *, query: str, fields: str) -> list[dict[str, Any]]:
    drive_service = _get_drive_service(store_or_storage)
    if drive_service is None:
        return []
    result: list[dict[str, Any]] = []
    token = None
    seen_tokens: set[str] = set()
    for _ in range(100):
        response = drive_service.files().list(
            q=query,
            fields=f"nextPageToken,incompleteSearch,files({fields})",
            pageSize=100,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=token,
        ).execute()
        if not isinstance(response, dict):
            raise NavigationError("Drive navigation listing returned a malformed response")
        if response.get("incompleteSearch") not in (None, False):
            raise NavigationError("Drive navigation listing was incomplete")
        items = response.get("files")
        if not isinstance(items, list):
            raise NavigationError("Drive navigation listing omitted its files array")
        if any(not isinstance(item, dict) or not item.get("id") for item in items):
            raise NavigationError("Drive navigation listing returned malformed file metadata")
        result.extend(items)
        next_token = response.get("nextPageToken")
        if next_token in (None, ""):
            return result
        if not isinstance(next_token, str) or next_token in seen_tokens:
            raise NavigationError("Drive navigation listing returned a malformed or repeated page token")
        seen_tokens.add(next_token)
        token = next_token
    raise NavigationError("Drive navigation listing exceeded the page limit")


def _find_item(
    store_or_storage: Any,
    name: str,
    parent_id: str,
    *,
    mime_type: str | None = None,
) -> list[str]:
    drive_service = _get_drive_service(store_or_storage)
    if drive_service is not None:
        escaped_name = name.replace("\\", "\\\\").replace("'", "\\'")
        query = f"name='{escaped_name}' and '{parent_id}' in parents and trashed=false"
        items = _paged_items(
            store_or_storage,
            query=query,
            fields="id,name,mimeType,parents,shortcutDetails",
        )
        if mime_type is not None:
            items = [item for item in items if item.get("mimeType") == mime_type]
        return [item["id"] for item in items]
    if hasattr(store_or_storage, "find"):
        return list(store_or_storage.find(name, parent_id))
    if hasattr(store_or_storage, "_list_exact_folders"):
        items = store_or_storage._list_exact_folders(name, parent_id=parent_id)
        return [item["id"] for item in items]
    raise TypeError(f"Unsupported store: {type(store_or_storage)}")


def _list_children(store_or_storage: Any, parent_id: str) -> list[dict[str, Any]]:
    if _get_drive_service(store_or_storage) is None:
        return []
    return _paged_items(
        store_or_storage,
        query=f"'{parent_id}' in parents and trashed=false",
        fields="id,name,mimeType,parents,shortcutDetails",
    )


def _unique_child(
    store_or_storage: Any,
    parent_id: str,
    name: str,
    mime_type: str,
    *,
    required: bool,
) -> dict[str, Any] | None:
    matches = [item for item in _list_children(store_or_storage, parent_id) if item.get("name") == name]
    if len(matches) > 1:
        raise NavigationError(f"Drive item '{name}' is duplicated under parent {parent_id}")
    if not matches:
        if required:
            raise NavigationError(f"Drive item '{name}' is missing under parent {parent_id}")
        return None
    item = matches[0]
    if item.get("mimeType") != mime_type:
        raise NavigationError(
            f"Drive item '{name}' under parent {parent_id} has MIME {item.get('mimeType')!r}, "
            f"expected {mime_type!r}"
        )
    return item


def _mkdir(store_or_storage: Any, name: str, parent_id: str) -> str:
    if hasattr(store_or_storage, "get_or_create_nested_folder"):
        return store_or_storage.get_or_create_nested_folder([name], root_id=parent_id)
    if hasattr(store_or_storage, "mkdir"):
        return store_or_storage.mkdir(name, parent_id)
    raise TypeError(f"Unsupported store: {type(store_or_storage)}")


def _create_shortcut(store_or_storage: Any, name: str, target_id: str, parent_id: str) -> str:
    """Create a Drive shortcut pointing to target_id."""
    if hasattr(store_or_storage, "create_shortcut"):
        return store_or_storage.create_shortcut(name, target_id, parent_id)
    drive_service = _get_drive_service(store_or_storage)
    if drive_service is not None:
        files = drive_service.files()
        body = {
            "name": name,
            "mimeType": SHORTCUT_MIME_TYPE,
            "shortcutDetails": {"targetId": target_id},
            "parents": [parent_id],
        }
        shortcut = files.create(
            body=body,
            fields="id,name,mimeType,shortcutDetails",
            supportsAllDrives=True,
        ).execute()
        return shortcut["id"]
    raise TypeError(f"Unsupported store for shortcut creation: {type(store_or_storage)}")


def _get_target_id(store_or_storage: Any, shortcut_id: str) -> str | None:
    drive_service = _get_drive_service(store_or_storage)
    if drive_service:
        try:
            meta = drive_service.files().get(
                fileId=shortcut_id, fields="id,shortcutDetails,mimeType", supportsAllDrives=True
            ).execute()
            if meta.get("mimeType") != SHORTCUT_MIME_TYPE:
                return None
            return (meta.get("shortcutDetails") or {}).get("targetId")
        except Exception:
            return None
    return None


def _delete_item(store_or_storage: Any, item_id: str) -> None:
    drive_service = _get_drive_service(store_or_storage)
    if drive_service:
        try:
            drive_service.files().delete(fileId=item_id, supportsAllDrives=True).execute()
            return
        except Exception as exc:
            logger.warning(f"Error deleting Drive item {item_id}: {exc}")
            raise NavigationError(f"Failed to delete item {item_id}: {exc}") from exc
    if hasattr(store_or_storage, "delete"):
        store_or_storage.delete(item_id)


def _sync_shortcut(store_or_storage: Any, name: str, target_id: str, parent_id: str) -> str:
    existing_items = _list_children(store_or_storage, parent_id)
    matches = [item for item in existing_items if item.get("name") == name]
    if matches:
        for match in matches:
            if match.get("mimeType") != SHORTCUT_MIME_TYPE:
                raise NavigationError(
                    f"Existing item '{name}' in parent {parent_id} is a {match.get('mimeType')}, not a shortcut. Refusing to delete real file."
                )
            target = (match.get("shortcutDetails") or {}).get("targetId")
            if target == target_id and len(matches) == 1:
                return match["id"]
        # If target changed or multiple shortcuts exist, remove old shortcuts
        for match in matches:
            if match.get("mimeType") == SHORTCUT_MIME_TYPE:
                _delete_item(store_or_storage, match["id"])
    return _create_shortcut(store_or_storage, name, target_id, parent_id)


def _create_or_replace_file(store_or_storage: Any, name: str, data: bytes, parent_id: str) -> str:
    """Create or replace a file in parent_id."""
    drive_service = _get_drive_service(store_or_storage)
    if drive_service is not None:
        import io
        from googleapiclient.http import MediaIoBaseUpload
        files = drive_service.files()
        existing = _find_item(store_or_storage, name, parent_id)
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype="application/json", resumable=False)
        if existing:
            updated = files.update(
                fileId=existing[0],
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()
            return updated["id"]
        created = files.create(
            body={"name": name, "parents": [parent_id], "mimeType": "application/json"},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        return created["id"]
    if hasattr(store_or_storage, "create"):
        existing = _find_item(store_or_storage, name, parent_id)
        if existing and hasattr(store_or_storage, "replace"):
            try:
                store_or_storage.replace(existing[0], data)
                return existing[0]
            except Exception:
                pass
        return store_or_storage.create(name, data, parent_id)
    raise TypeError(f"Unsupported store: {type(store_or_storage)}")


def _read_file_bytes(store_or_storage: Any, file_id: str) -> bytes:
    if hasattr(store_or_storage, "read"):
        return store_or_storage.read(file_id)
    drive_service = _get_drive_service(store_or_storage)
    if drive_service is not None:
        return drive_service.files().get_media(
            fileId=file_id, supportsAllDrives=True
        ).execute()
    raise TypeError(f"Unsupported store: {type(store_or_storage)}")


def _manifest_layers(source_id: str, manifest: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(manifest, dict) or not isinstance(manifest.get("release_id"), str):
        raise NavigationError("Navigation requires a manifest with a release ID")
    if manifest.get("status") != "validated" or manifest.get("tests") != {"passed": True}:
        raise NavigationError("Navigation requires a validated manifest with passing tests")
    datasets = manifest.get("datasets")
    if not isinstance(datasets, list):
        raise NavigationError("Navigation manifest datasets must be a list")
    by_layer: dict[str, list[dict[str, Any]]] = {layer: [] for layer in ALL_MEDALLION_LAYERS}
    seen: set[tuple[str, str]] = set()
    for dataset in datasets:
        if not isinstance(dataset, dict):
            raise NavigationError("Navigation manifest contains malformed dataset metadata")
        layer = dataset.get("layer")
        if layer not in by_layer:
            continue
        table_name = dataset.get("table_name")
        files = dataset.get("files")
        if not isinstance(table_name, str) or not table_name or not isinstance(files, list) or not files:
            raise NavigationError("Navigation dataset requires a table name and at least one file")
        key = (layer, table_name)
        if key in seen:
            raise NavigationError(f"Duplicate navigation table key {layer}/{table_name}")
        seen.add(key)
        names: set[str] = set()
        for index, item in enumerate(files):
            if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]:
                raise NavigationError(f"Navigation table {layer}/{table_name} has malformed file metadata")
            name = item.get("name", f"{table_name}--part-{index}.parquet")
            if not isinstance(name, str) or not name or name in names:
                raise NavigationError(f"Navigation table {layer}/{table_name} has duplicate part names")
            names.add(name)
        by_layer[layer].append(dataset)
    return by_layer


def _read_index(store_or_storage: Any, item: dict[str, Any] | None) -> dict[str, Any] | None:
    if item is None:
        return None
    try:
        value = json.loads(_read_file_bytes(store_or_storage, item["id"]).decode("utf-8"))
    except Exception as exc:
        raise NavigationError("Navigation index is corrupt") from exc
    if not isinstance(value, dict):
        raise NavigationError("Navigation index is not a JSON object")
    return value


def _index_owned_ids(index_doc: dict[str, Any] | None) -> set[str]:
    if not index_doc:
        return set()
    owned = {
        value for value in index_doc.get("previous_managed_ids", [])
        if isinstance(value, str) and value
    }
    tables = index_doc.get("tables", {})
    if isinstance(tables, dict):
        for table in tables.values():
            if not isinstance(table, dict):
                continue
            container_id = table.get("container_id")
            if isinstance(container_id, str) and container_id:
                owned.add(container_id)
            shortcuts = table.get("shortcuts", [])
            if isinstance(shortcuts, list):
                for shortcut in shortcuts:
                    if isinstance(shortcut, dict):
                        shortcut_id = shortcut.get("shortcut_id")
                        if isinstance(shortcut_id, str) and shortcut_id:
                            owned.add(shortcut_id)
    return owned


def _desired_tables(datasets: list[dict[str, Any]]) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for dataset in datasets:
        table_name = dataset["table_name"]
        files = dataset["files"]
        shortcuts = []
        for index, item in enumerate(files):
            name = (
                f"{table_name}.parquet"
                if len(files) == 1
                else item.get("name", f"{table_name}--part-{index}.parquet")
            )
            shortcuts.append({
                "name": name,
                "target_id": item["id"],
                "size": item.get("size", 0),
                "sha256": item.get("sha256", ""),
            })
        tables[table_name] = {
            "is_multi_part": len(files) > 1,
            "file_count": len(files),
            "shortcuts": shortcuts,
        }
    return tables


def _sync_owned_shortcut(
    store_or_storage: Any,
    parent_id: str,
    desired: dict[str, Any],
    owned: set[str],
    *,
    recovering_pending: bool,
) -> str:
    children = _list_children(store_or_storage, parent_id)
    matches = [item for item in children if item.get("name") == desired["name"]]
    if any(item.get("mimeType") != SHORTCUT_MIME_TYPE for item in matches):
        raise NavigationError(f"Navigation name collision for shortcut {desired['name']!r}")
    foreign = [item for item in matches if item["id"] not in owned]
    exact = [
        item for item in matches
        if (item.get("shortcutDetails") or {}).get("targetId") == desired["target_id"]
    ]
    if foreign and not (
        recovering_pending and len(matches) == 1 and len(exact) == 1
    ):
        raise NavigationError(f"Refusing to replace unowned shortcut {desired['name']!r}")
    if len(matches) == 1 and len(exact) == 1:
        owned.add(matches[0]["id"])
        return matches[0]["id"]
    for item in matches:
        if item["id"] not in owned:
            raise NavigationError(f"Refusing to delete unowned shortcut {desired['name']!r}")
        _delete_item(store_or_storage, item["id"])
        owned.discard(item["id"])
    shortcut_id = _create_shortcut(
        store_or_storage, desired["name"], desired["target_id"], parent_id
    )
    owned.add(shortcut_id)
    return shortcut_id


def _remove_owned_tree(
    store_or_storage: Any, item: dict[str, Any], owned: set[str]
) -> None:
    item_id = item["id"]
    if item_id not in owned:
        raise NavigationError(f"Refusing to prune unowned navigation item {item_id}")
    if item.get("mimeType") == FOLDER_MIME_TYPE:
        for child in _list_children(store_or_storage, item_id):
            _remove_owned_tree(store_or_storage, child, owned)
    _delete_item(store_or_storage, item_id)
    owned.discard(item_id)


def _preflight_existing_source_namespace(
    store_or_storage: Any,
    source_item: dict[str, Any],
    source_id: str,
    layer: str,
    datasets: list[dict[str, Any]],
) -> None:
    """Validate an existing namespace before any missing peer is created."""
    index_item = _unique_child(
        store_or_storage, source_item["id"], "navigation-index.json",
        "application/json", required=False,
    )
    children = _list_children(store_or_storage, source_item["id"])
    if index_item is None:
        if children:
            raise NavigationError(
                f"Unowned non-empty source folder exists in {layer}/current/{source_id}"
            )
        return
    old_index = _read_index(store_or_storage, index_item)
    if (
        old_index is None
        or old_index.get("format_version") != 1
        or old_index.get("source_id") != source_id
        or old_index.get("layer") != layer
        or old_index.get("status") not in {"pending", "current_verified"}
        or not isinstance(old_index.get("tables"), dict)
    ):
        raise NavigationError(
            f"Navigation index identity/status is invalid in {layer}/current/{source_id}"
        )
    recovering_pending = old_index["status"] == "pending"
    owned = _index_owned_ids(old_index)
    pending_tables = old_index["tables"] if recovering_pending else {}
    desired = _desired_tables(datasets)
    expected_names = {
        shortcut["name"]
        for table in desired.values()
        if not table["is_multi_part"]
        for shortcut in table["shortcuts"]
    } | {
        name for name, table in desired.items() if table["is_multi_part"]
    } | {
        shortcut["name"]
        for table in pending_tables.values()
        if isinstance(table, dict) and not table.get("is_multi_part")
        for shortcut in table.get("shortcuts", [])
        if isinstance(shortcut, dict) and isinstance(shortcut.get("name"), str)
    } | {
        name for name, table in pending_tables.items()
        if isinstance(name, str) and isinstance(table, dict) and table.get("is_multi_part")
    }
    for child in children:
        if child["id"] == index_item["id"]:
            continue
        if child["id"] in owned:
            if child.get("mimeType") == FOLDER_MIME_TYPE:
                nested = _list_children(store_or_storage, child["id"])
                if any(item["id"] not in owned for item in nested):
                    raise NavigationError("Managed navigation folder contains unowned content")
            continue
        if recovering_pending and child.get("name") in expected_names:
            candidate_tables = [
                table for table in (desired.get(child.get("name")), pending_tables.get(child.get("name")))
                if isinstance(table, dict) and table.get("is_multi_part")
            ]
            if child.get("mimeType") == FOLDER_MIME_TYPE:
                wanted = {
                    (part.get("name"), part.get("target_id"))
                    for table in candidate_tables
                    for part in table.get("shortcuts", [])
                    if isinstance(part, dict)
                }
                nested = _list_children(store_or_storage, child["id"])
                if not candidate_tables or any(
                    item.get("mimeType") != SHORTCUT_MIME_TYPE
                    or (item.get("name"), (item.get("shortcutDetails") or {}).get("targetId")) not in wanted
                    for item in nested
                ):
                    raise NavigationError("Pending navigation contains unowned content")
            elif not (
                child.get("mimeType") == SHORTCUT_MIME_TYPE
                and any(
                    shortcut.get("name") == child.get("name")
                    and shortcut.get("target_id")
                    == (child.get("shortcutDetails") or {}).get("targetId")
                    for table in list(pending_tables.values()) + list(desired.values())
                    if isinstance(table, dict)
                    for shortcut in table.get("shortcuts", [])
                    if isinstance(shortcut, dict)
                )
            ):
                raise NavigationError("Pending navigation item does not match the target")
            continue
        raise NavigationError(
            f"Unowned item {child.get('name')!r} exists in {layer}/current/{source_id}"
        )


def _ensure_source_navigation_folders(
    store_or_storage: Any,
    root_id: str,
    source_id: str,
    by_layer: dict[str, list[dict[str, Any]]],
) -> None:
    """Create empty source namespaces only after every layer passes preflight."""
    missing: list[tuple[str, str]] = []
    for layer in ALL_MEDALLION_LAYERS:
        datasets = by_layer[layer]
        layer_item = _unique_child(
            store_or_storage, root_id, layer, FOLDER_MIME_TYPE, required=True
        )
        current_item = _unique_child(
            store_or_storage, layer_item["id"], CURRENT_NAV_DIR, FOLDER_MIME_TYPE,
            required=bool(datasets),
        )
        if current_item is None:
            continue
        source_item = _unique_child(
            store_or_storage, current_item["id"], source_id, FOLDER_MIME_TYPE,
            required=False,
        )
        if source_item is None:
            if datasets:
                missing.append((layer, current_item["id"]))
            continue
        _preflight_existing_source_namespace(
            store_or_storage, source_item, source_id, layer, datasets,
        )
    for layer, current_id in missing:
        created_id = _mkdir(store_or_storage, source_id, current_id)
        source_item = _unique_child(
            store_or_storage, current_id, source_id, FOLDER_MIME_TYPE, required=True,
        )
        if source_item["id"] != created_id:
            raise NavigationError(
                f"Created source folder identity changed in {layer}/current/{source_id}"
            )


def sync_source_medallion_navigation(
    store_or_storage: Any,
    root_id: str,
    source_id: str,
    manifest: dict[str, Any],
    *,
    finalize: bool = True,
) -> dict[str, Any]:
    """Durably reconcile canonical shortcuts before a release pointer changes."""
    by_layer = _manifest_layers(source_id, manifest)
    _ensure_source_navigation_folders(store_or_storage, root_id, source_id, by_layer)
    release_id = manifest["release_id"]
    states: list[dict[str, Any]] = []
    for layer in ALL_MEDALLION_LAYERS:
        datasets = by_layer[layer]
        layer_item = _unique_child(
            store_or_storage, root_id, layer, FOLDER_MIME_TYPE, required=True
        )
        current_item = _unique_child(
            store_or_storage, layer_item["id"], CURRENT_NAV_DIR, FOLDER_MIME_TYPE,
            required=bool(datasets),
        )
        if current_item is None:
            continue
        source_item = _unique_child(
            store_or_storage, current_item["id"], source_id, FOLDER_MIME_TYPE,
            required=bool(datasets),
        )
        if source_item is None:
            continue
        index_item = _unique_child(
            store_or_storage, source_item["id"], "navigation-index.json",
            "application/json", required=False,
        )
        old_index = _read_index(store_or_storage, index_item)
        recovering_pending = bool(old_index and old_index.get("status") == "pending")
        if old_index:
            if old_index.get("format_version") != 1:
                raise NavigationError("Navigation index has unsupported format version")
            if old_index.get("source_id") != source_id or old_index.get("layer") != layer:
                raise NavigationError("Navigation index identity does not match its folder")
            if old_index.get("status") not in {"pending", "current_verified"}:
                raise NavigationError("Navigation index has unsupported status")
        owned = _index_owned_ids(old_index)
        desired = _desired_tables(datasets)
        pending_tables = (old_index or {}).get("tables", {}) if recovering_pending else {}
        if recovering_pending and not isinstance(pending_tables, dict):
            raise NavigationError("Pending navigation tables are malformed")
        expected_names = {
            shortcut["name"]
            for table in desired.values()
            if not table["is_multi_part"]
            for shortcut in table["shortcuts"]
        } | {
            name for name, table in desired.items() if table["is_multi_part"]
        } | {
            shortcut["name"]
            for table in pending_tables.values()
            if isinstance(table, dict) and not table.get("is_multi_part")
            for shortcut in table.get("shortcuts", [])
            if isinstance(shortcut, dict) and isinstance(shortcut.get("name"), str)
        } | {
            name for name, table in pending_tables.items()
            if isinstance(name, str) and isinstance(table, dict) and table.get("is_multi_part")
        }
        for child in _list_children(store_or_storage, source_item["id"]):
            if index_item and child["id"] == index_item["id"]:
                continue
            if child["id"] in owned:
                if child.get("mimeType") == FOLDER_MIME_TYPE:
                    candidate_tables = [
                        table for table in (desired.get(child.get("name")), pending_tables.get(child.get("name")))
                        if isinstance(table, dict) and table.get("is_multi_part")
                    ]
                    wanted = {
                        (part.get("name"), part.get("target_id"))
                        for table in candidate_tables
                        for part in table.get("shortcuts", [])
                        if isinstance(part, dict)
                    }
                    for part in _list_children(store_or_storage, child["id"]):
                        identity = (
                            part.get("name"),
                            (part.get("shortcutDetails") or {}).get("targetId"),
                        )
                        if part["id"] not in owned:
                            if recovering_pending and part.get("mimeType") == SHORTCUT_MIME_TYPE and identity in wanted:
                                owned.add(part["id"])
                            else:
                                raise NavigationError("Managed navigation folder contains unowned content")
                continue
            if recovering_pending and child.get("name") in expected_names:
                if child.get("mimeType") == FOLDER_MIME_TYPE:
                    candidate_tables = [
                        table for table in (desired.get(child.get("name")), pending_tables.get(child.get("name")))
                        if isinstance(table, dict) and table.get("is_multi_part")
                    ]
                    if not candidate_tables:
                        raise NavigationError("Pending navigation folder does not match the target")
                    wanted = {
                        (part.get("name"), part.get("target_id"))
                        for table in candidate_tables
                        for part in table.get("shortcuts", [])
                        if isinstance(part, dict)
                    }
                    for part in _list_children(store_or_storage, child["id"]):
                        identity = (part.get("name"), (part.get("shortcutDetails") or {}).get("targetId"))
                        if part.get("mimeType") != SHORTCUT_MIME_TYPE or identity not in wanted:
                            raise NavigationError("Pending navigation contains unowned content")
                        owned.add(part["id"])
                    owned.add(child["id"])
                elif (
                    child.get("mimeType") == SHORTCUT_MIME_TYPE
                    and any(
                        shortcut["name"] == child.get("name")
                        and shortcut["target_id"]
                        == (child.get("shortcutDetails") or {}).get("targetId")
                        for table in list(pending_tables.values()) + list(desired.values())
                        if isinstance(table, dict)
                        for shortcut in table.get("shortcuts", [])
                        if isinstance(shortcut, dict)
                    )
                ):
                    owned.add(child["id"])
                else:
                    raise NavigationError("Pending navigation item does not match the target")
            else:
                raise NavigationError(
                    f"Unowned item {child.get('name')!r} exists in {layer}/current/{source_id}"
                )
        pending = {
            "format_version": 1,
            "source_id": source_id,
            "layer": layer,
            "release_id": release_id,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "pending",
            "previous_managed_ids": sorted(owned),
            "tables": desired,
        }
        states.append({
            "layer": layer,
            "source_id": source_item["id"],
            "index_id": index_item["id"] if index_item else None,
            "owned": owned,
            "desired": desired,
            "pending": pending,
            "recovering": recovering_pending,
        })
    # Every applicable layer records intent before shortcut mutation.
    for state in states:
        state["index_id"] = _create_or_replace_file(
            store_or_storage, "navigation-index.json",
            _json_bytes(state["pending"]), state["source_id"],
        )
    for state in states:
        owned = state["owned"]
        actual_tables: dict[str, Any] = {}
        expected_top_ids: set[str] = set()
        for table_name, table in state["desired"].items():
            actual = {
                "is_multi_part": table["is_multi_part"],
                "file_count": table["file_count"],
                "shortcuts": [],
            }
            if table["is_multi_part"]:
                matches = [
                    item for item in _list_children(store_or_storage, state["source_id"])
                    if item.get("name") == table_name
                ]
                if len(matches) > 1 or any(
                    item.get("mimeType") != FOLDER_MIME_TYPE for item in matches
                ):
                    raise NavigationError(f"Navigation name collision for folder {table_name!r}")
                if matches:
                    container = matches[0]
                    if container["id"] not in owned and not state["recovering"]:
                        raise NavigationError(f"Refusing to adopt unowned folder {table_name!r}")
                    owned.add(container["id"])
                else:
                    container = {
                        "id": _mkdir(store_or_storage, table_name, state["source_id"]),
                        "name": table_name,
                        "mimeType": FOLDER_MIME_TYPE,
                    }
                    owned.add(container["id"])
                actual["container_id"] = container["id"]
                expected_top_ids.add(container["id"])
                expected_part_ids: set[str] = set()
                for desired_shortcut in table["shortcuts"]:
                    shortcut_id = _sync_owned_shortcut(
                        store_or_storage, container["id"], desired_shortcut, owned,
                        recovering_pending=state["recovering"],
                    )
                    expected_part_ids.add(shortcut_id)
                    actual["shortcuts"].append({**desired_shortcut, "shortcut_id": shortcut_id})
                for child in _list_children(store_or_storage, container["id"]):
                    if child["id"] not in expected_part_ids:
                        _remove_owned_tree(store_or_storage, child, owned)
            else:
                desired_shortcut = table["shortcuts"][0]
                shortcut_id = _sync_owned_shortcut(
                    store_or_storage, state["source_id"], desired_shortcut, owned,
                    recovering_pending=state["recovering"],
                )
                expected_top_ids.add(shortcut_id)
                actual["shortcuts"].append({**desired_shortcut, "shortcut_id": shortcut_id})
            actual_tables[table_name] = actual
        for child in _list_children(store_or_storage, state["source_id"]):
            if child["id"] == state["index_id"] or child["id"] in expected_top_ids:
                continue
            _remove_owned_tree(store_or_storage, child, owned)
        state["pending"]["tables"] = actual_tables
        state["pending"]["previous_managed_ids"] = sorted(owned)
        _create_or_replace_file(
            store_or_storage, "navigation-index.json",
            _json_bytes(state["pending"]), state["source_id"],
        )
    verify_medallion_navigation(
        store_or_storage, root_id, source_id, manifest, _expected_status="pending"
    )
    receipts: dict[str, Any] = {
        "status": "navigation_pending",
        "source_id": source_id, "release_id": release_id, "layers": {
            state["layer"]: {
                "source_nav_id": state["source_id"],
                "index_file_id": state["index_id"],
                "tables": state["pending"]["tables"],
            }
            for state in states
        }
    }
    if not finalize:
        return receipts
    return finalize_source_medallion_navigation(
        store_or_storage, root_id, source_id, manifest
    )


def finalize_source_medallion_navigation(
    store_or_storage: Any,
    root_id: str,
    source_id: str,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Mark fully reconciled navigation current after exact pointer readback."""
    by_layer = _manifest_layers(source_id, manifest)
    release_id = manifest["release_id"]
    verify_medallion_navigation(
        store_or_storage, root_id, source_id, manifest,
        _expected_status={"pending", "current_verified"},
    )
    receipts: dict[str, Any] = {
        "status": "medallion_navigation_verified",
        "source_id": source_id, "release_id": release_id, "layers": {},
    }
    for layer in ALL_MEDALLION_LAYERS:
        datasets = by_layer[layer]
        layer_item = _unique_child(
            store_or_storage, root_id, layer, FOLDER_MIME_TYPE, required=True
        )
        current_item = _unique_child(
            store_or_storage, layer_item["id"], CURRENT_NAV_DIR, FOLDER_MIME_TYPE,
            required=bool(datasets),
        )
        if current_item is None:
            continue
        source_item = _unique_child(
            store_or_storage, current_item["id"], source_id, FOLDER_MIME_TYPE,
            required=bool(datasets),
        )
        if source_item is None:
            continue
        index_item = _unique_child(
            store_or_storage, source_item["id"], "navigation-index.json",
            "application/json", required=True,
        )
        index = _read_index(store_or_storage, index_item)
        if index is None:
            raise StaleNavigationError("Navigation index disappeared before finalization")
        if index.get("status") == "pending":
            final_index = dict(index)
            final_index["status"] = "current_verified"
            final_index["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
            index_id = _create_or_replace_file(
                store_or_storage, "navigation-index.json",
                _json_bytes(final_index), source_item["id"],
            )
            if index_id != index_item["id"]:
                raise StaleNavigationError("Navigation index identity changed during finalization")
            index = final_index
        receipts["layers"][layer] = {
            "source_nav_id": source_item["id"],
            "index_file_id": index_item["id"],
            "tables": index["tables"],
        }
    verify_medallion_navigation(store_or_storage, root_id, source_id, manifest)
    return receipts


def verify_medallion_navigation(
    store_or_storage: Any,
    root_id: str,
    source_id: str,
    manifest: dict[str, Any],
    *,
    _expected_status: str | set[str] = "current_verified",
) -> dict[str, Any]:
    """Verify index identity and the complete physical navigation subtree."""
    allowed_statuses = (
        {_expected_status} if isinstance(_expected_status, str) else set(_expected_status)
    )
    if not allowed_statuses or not allowed_statuses <= {"pending", "current_verified"}:
        raise ValueError("unsupported navigation verification status")
    by_layer = _manifest_layers(source_id, manifest)
    release_id = manifest["release_id"]
    verified_layers: dict[str, Any] = {}
    for layer in ALL_MEDALLION_LAYERS:
        datasets = by_layer[layer]
        layer_item = _unique_child(
            store_or_storage, root_id, layer, FOLDER_MIME_TYPE, required=True
        )
        current_item = _unique_child(
            store_or_storage, layer_item["id"], CURRENT_NAV_DIR, FOLDER_MIME_TYPE,
            required=bool(datasets),
        )
        if current_item is None:
            continue
        source_item = _unique_child(
            store_or_storage, current_item["id"], source_id, FOLDER_MIME_TYPE,
            required=bool(datasets),
        )
        if source_item is None:
            continue
        index_item = _unique_child(
            store_or_storage, source_item["id"], "navigation-index.json",
            "application/json", required=True,
        )
        index_doc = _read_index(store_or_storage, index_item)
        if (
            index_doc.get("format_version") != 1
            or index_doc.get("status") not in allowed_statuses
            or index_doc.get("source_id") != source_id
            or index_doc.get("layer") != layer
            or index_doc.get("release_id") != release_id
            or not isinstance(index_doc.get("tables"), dict)
        ):
            raise StaleNavigationError(
                f"Navigation index identity/status mismatch in {layer}/current/{source_id}"
            )
        expected = _desired_tables(datasets)
        if set(index_doc["tables"]) != set(expected):
            raise StaleNavigationError(f"Navigation table keys mismatch in {layer}")
        expected_top_ids = {index_item["id"]}
        for table_name, desired_table in expected.items():
            actual_table = index_doc["tables"].get(table_name)
            if (
                not isinstance(actual_table, dict)
                or actual_table.get("is_multi_part") != desired_table["is_multi_part"]
                or actual_table.get("file_count") != desired_table["file_count"]
                or not isinstance(actual_table.get("shortcuts"), list)
                or len(actual_table["shortcuts"]) != desired_table["file_count"]
            ):
                raise StaleNavigationError(f"Navigation table metadata mismatch for {layer}/{table_name}")
            indexed_by_name = {
                item.get("name"): item for item in actual_table["shortcuts"]
                if isinstance(item, dict)
            }
            if set(indexed_by_name) != {item["name"] for item in desired_table["shortcuts"]}:
                raise StaleNavigationError(f"Navigation shortcut keys mismatch for {layer}/{table_name}")
            parent_id = source_item["id"]
            if desired_table["is_multi_part"]:
                container = _unique_child(
                    store_or_storage, source_item["id"], table_name,
                    FOLDER_MIME_TYPE, required=True,
                )
                if actual_table.get("container_id") != container["id"]:
                    raise StaleNavigationError(f"Navigation container ID mismatch for {layer}/{table_name}")
                expected_top_ids.add(container["id"])
                parent_id = container["id"]
            expected_child_ids: set[str] = set()
            for wanted in desired_table["shortcuts"]:
                indexed = indexed_by_name[wanted["name"]]
                if indexed.get("target_id") != wanted["target_id"]:
                    raise StaleNavigationError(f"Navigation target index mismatch for {layer}/{table_name}")
                shortcut = _unique_child(
                    store_or_storage, parent_id, wanted["name"],
                    SHORTCUT_MIME_TYPE, required=True,
                )
                if (
                    indexed.get("shortcut_id") != shortcut["id"]
                    or _get_target_id(store_or_storage, shortcut["id"]) != wanted["target_id"]
                ):
                    raise StaleNavigationError(f"Navigation shortcut mismatch for {layer}/{table_name}")
                expected_child_ids.add(shortcut["id"])
            if desired_table["is_multi_part"]:
                actual_ids = {item["id"] for item in _list_children(store_or_storage, parent_id)}
                if actual_ids != expected_child_ids:
                    raise StaleNavigationError(f"Navigation folder contents mismatch for {layer}/{table_name}")
            else:
                expected_top_ids.update(expected_child_ids)
        actual_top_ids = {
            item["id"] for item in _list_children(store_or_storage, source_item["id"])
        }
        if actual_top_ids != expected_top_ids:
            raise StaleNavigationError(
                f"Navigation subtree contains obsolete or foreign items in {layer}/current/{source_id}"
            )
        verified_layers[layer] = {
            "status": "verified_current",
            "release_id": release_id,
            "table_count": len(datasets),
        }
    return {
        "status": "medallion_navigation_verified",
        "source_id": source_id,
        "layers": verified_layers,
    }
