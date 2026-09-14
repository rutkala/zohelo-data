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
        q = f"name='{escaped_name}' and '{parent_id}' in parents and trashed=false"
        if mime_type:
            q += f" and mimeType='{mime_type}'"
        result = []
        token = None
        while True:
            resp = drive_service.files().list(
                q=q,
                fields="nextPageToken,files(id,mimeType)",
                pageSize=50,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=token,
            ).execute()
            result.extend([f["id"] for f in resp.get("files", [])])
            token = resp.get("nextPageToken")
            if not token:
                break
        return result
    if hasattr(store_or_storage, "find"):
        return list(store_or_storage.find(name, parent_id))
    if hasattr(store_or_storage, "_list_exact_folders"):
        items = store_or_storage._list_exact_folders(name, parent_id=parent_id)
        return [item["id"] for item in items]
    raise TypeError(f"Unsupported store: {type(store_or_storage)}")


def _list_children(store_or_storage: Any, parent_id: str) -> list[dict[str, Any]]:
    drive_service = _get_drive_service(store_or_storage)
    if drive_service is not None:
        q = f"'{parent_id}' in parents and trashed=false"
        result = []
        token = None
        while True:
            resp = drive_service.files().list(
                q=q,
                fields="nextPageToken,files(id,name,mimeType,shortcutDetails)",
                pageSize=100,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=token,
            ).execute()
            result.extend(resp.get("files", []))
            token = resp.get("nextPageToken")
            if not token:
                break
        return result
    return []


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


def sync_source_medallion_navigation(
    store_or_storage: Any,
    root_id: str,
    source_id: str,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Sync navigation shortcuts and index in 02_bronze, 03_silver, 04_gold for source_id.

    Derived strictly from verified manifest. Never copies Parquet bytes.
    Covers multi-file datasets by grouping parts in a dataset folder.
    Prunes obsolete shortcuts across all layers (including layers becoming empty).
    """
    release_id = manifest["release_id"]
    receipts: dict[str, Any] = {"source_id": source_id, "release_id": release_id, "layers": {}}

    # Group datasets by layer
    by_layer: dict[str, list[dict[str, Any]]] = {l: [] for l in ALL_MEDALLION_LAYERS}
    for dataset in manifest.get("datasets", []):
        layer = dataset.get("layer")
        if layer in by_layer:
            by_layer[layer].append(dataset)

    for layer in ALL_MEDALLION_LAYERS:
        datasets = by_layer[layer]
        layer_folders = _find_item(store_or_storage, layer, root_id, mime_type=FOLDER_MIME_TYPE)
        if len(layer_folders) != 1:
            raise NavigationError(f"Medallion layer folder '{layer}' missing or ambiguous")
        layer_id = layer_folders[0]

        # Check if current/ exists; if no datasets in this layer and current/ does not exist, nothing to do
        current_folders = _find_item(store_or_storage, CURRENT_NAV_DIR, layer_id, mime_type=FOLDER_MIME_TYPE)
        if not datasets and not current_folders:
            continue

        if not current_folders:
            current_id = _mkdir(store_or_storage, CURRENT_NAV_DIR, layer_id)
        else:
            current_id = current_folders[0]

        # Check if current/<source_id>/ exists
        source_folders = _find_item(store_or_storage, source_id, current_id, mime_type=FOLDER_MIME_TYPE)
        if not datasets and not source_folders:
            continue

        if not source_folders:
            source_nav_id = _mkdir(store_or_storage, source_id, current_id)
        else:
            source_nav_id = source_folders[0]

        expected_top_level_shortcuts: set[str] = set()
        expected_dataset_folders: dict[str, set[str]] = {}
        table_receipts = {}

        for dataset in datasets:
            table_name = dataset["table_name"]
            files = dataset.get("files", [])
            if not files:
                continue

            if len(files) == 1:
                # Single-file dataset: direct shortcut <table_name>.parquet
                file_entry = files[0]
                shortcut_name = f"{table_name}.parquet"
                expected_top_level_shortcuts.add(shortcut_name)
                sc_id = _sync_shortcut(
                    store_or_storage, shortcut_name, file_entry["id"], source_nav_id
                )
                table_receipts[table_name] = {
                    "is_multi_part": False,
                    "file_count": 1,
                    "shortcuts": [
                        {
                            "name": shortcut_name,
                            "shortcut_id": sc_id,
                            "target_id": file_entry["id"],
                            "size": file_entry.get("size", 0),
                            "sha256": file_entry.get("sha256", ""),
                        }
                    ],
                }
            else:
                # Multi-file dataset: dataset subfolder containing part shortcuts
                ds_folders = _find_item(store_or_storage, table_name, source_nav_id, mime_type=FOLDER_MIME_TYPE)
                ds_folder_id = ds_folders[0] if ds_folders else _mkdir(
                    store_or_storage, table_name, source_nav_id
                )
                part_shortcuts = []
                part_names: set[str] = set()
                for part_idx, file_entry in enumerate(files):
                    part_name = file_entry.get("name", f"{table_name}--part-{part_idx}.parquet")
                    part_names.add(part_name)
                    sc_id = _sync_shortcut(
                        store_or_storage, part_name, file_entry["id"], ds_folder_id
                    )
                    part_shortcuts.append(
                        {
                            "name": part_name,
                            "shortcut_id": sc_id,
                            "target_id": file_entry["id"],
                            "size": file_entry.get("size", 0),
                            "sha256": file_entry.get("sha256", ""),
                        }
                    )
                expected_dataset_folders[table_name] = part_names
                table_receipts[table_name] = {
                    "is_multi_part": True,
                    "file_count": len(files),
                    "container_id": ds_folder_id,
                    "shortcuts": part_shortcuts,
                }

        # ---------------------------------------------------------------------
        # Pruning obsolete shortcuts in source_nav_id
        # ---------------------------------------------------------------------
        existing_children = _list_children(store_or_storage, source_nav_id)
        for child in existing_children:
            c_name = child.get("name", "")
            c_mime = child.get("mimeType", "")
            c_id = child["id"]

            if c_mime == SHORTCUT_MIME_TYPE:
                if c_name not in expected_top_level_shortcuts:
                    logger.info(f"Pruning obsolete shortcut '{c_name}' in {layer}/{source_id}")
                    _delete_item(store_or_storage, c_id)

            elif c_mime == FOLDER_MIME_TYPE:
                if c_name in expected_dataset_folders:
                    # Prune surplus parts inside expected multi-file folder
                    expected_parts = expected_dataset_folders[c_name]
                    folder_children = _list_children(store_or_storage, c_id)
                    for f_child in folder_children:
                        if f_child.get("mimeType") == SHORTCUT_MIME_TYPE and f_child.get("name") not in expected_parts:
                            logger.info(f"Pruning obsolete part shortcut '{f_child.get('name')}' in {layer}/{source_id}/{c_name}")
                            _delete_item(store_or_storage, f_child["id"])
                else:
                    # Obsolete dataset folder (e.g. dataset removed or converted to single-file)
                    folder_children = _list_children(store_or_storage, c_id)
                    for f_child in folder_children:
                        if f_child.get("mimeType") == SHORTCUT_MIME_TYPE:
                            _delete_item(store_or_storage, f_child["id"])
                    # Check if empty now
                    remaining = _list_children(store_or_storage, c_id)
                    if not remaining:
                        logger.info(f"Pruning obsolete empty dataset folder '{c_name}' in {layer}/{source_id}")
                        _delete_item(store_or_storage, c_id)

        # Write or update navigation-index.json
        index_doc = {
            "format_version": 1,
            "source_id": source_id,
            "layer": layer,
            "release_id": release_id,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "current_verified",
            "tables": table_receipts,
        }
        index_bytes = _json_bytes(index_doc)
        index_id = _create_or_replace_file(
            store_or_storage, "navigation-index.json", index_bytes, source_nav_id
        )
        receipts["layers"][layer] = {
            "source_nav_id": source_nav_id,
            "index_file_id": index_id,
            "tables": table_receipts,
        }

    return receipts


def verify_medallion_navigation(
    store_or_storage: Any,
    root_id: str,
    source_id: str,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    """Verify that medallion navigation links in 02_bronze, 03_silver, 04_gold match manifest.

    Inspects actual Drive items: shortcutDetails.targetId, names, parents, exact counts.
    Fails closed if shortcuts are missing, target IDs differ, index is stale, or surplus shortcuts exist.
    """
    expected_release_id = manifest["release_id"]
    by_layer: dict[str, list[dict[str, Any]]] = {l: [] for l in ALL_MEDALLION_LAYERS}
    for dataset in manifest.get("datasets", []):
        layer = dataset.get("layer")
        if layer in by_layer:
            by_layer[layer].append(dataset)

    verified_layers = {}
    for layer in ALL_MEDALLION_LAYERS:
        datasets = by_layer[layer]
        layer_folders = _find_item(store_or_storage, layer, root_id, mime_type=FOLDER_MIME_TYPE)
        if len(layer_folders) != 1:
            raise StaleNavigationError(f"Layer folder '{layer}' not found or ambiguous")
        layer_id = layer_folders[0]

        current_folders = _find_item(store_or_storage, CURRENT_NAV_DIR, layer_id, mime_type=FOLDER_MIME_TYPE)
        if not datasets and not current_folders:
            continue
        if len(current_folders) != 1:
            raise StaleNavigationError(f"'{layer}/current' folder not found or ambiguous")
        current_id = current_folders[0]

        source_folders = _find_item(store_or_storage, source_id, current_id, mime_type=FOLDER_MIME_TYPE)
        if not datasets and not source_folders:
            continue
        if len(source_folders) != 1:
            raise StaleNavigationError(f"'{layer}/current/{source_id}' folder not found or ambiguous")
        source_nav_id = source_folders[0]

        # Read navigation index
        index_ids = _find_item(store_or_storage, "navigation-index.json", source_nav_id)
        if len(index_ids) != 1:
            raise StaleNavigationError(f"'{layer}/current/{source_id}/navigation-index.json' missing or ambiguous")
        raw_index = _read_file_bytes(store_or_storage, index_ids[0])
        try:
            index_doc = json.loads(raw_index.decode("utf-8"))
        except Exception as exc:
            raise StaleNavigationError(f"Corrupt navigation index in '{layer}/current/{source_id}'") from exc

        if index_doc.get("release_id") != expected_release_id:
            raise StaleNavigationError(
                f"Navigation index in '{layer}/current/{source_id}' points to release "
                f"'{index_doc.get('release_id')}', but current release is '{expected_release_id}'"
            )

        # Verify actual Drive items and match against datasets
        expected_top_shortcuts: set[str] = set()
        expected_folders: dict[str, set[str]] = {}
        indexed_tables = index_doc.get("tables", {})

        for dataset in datasets:
            table_name = dataset["table_name"]
            if table_name not in indexed_tables:
                raise StaleNavigationError(f"Table '{table_name}' missing from navigation index in {layer}")
            expected_files = dataset.get("files", [])
            table_meta = indexed_tables[table_name]
            if len(expected_files) != table_meta.get("file_count"):
                raise StaleNavigationError(
                    f"Table '{table_name}' file count mismatch in navigation index for {layer}"
                )

            if len(expected_files) == 1:
                # Single-file dataset
                sc_name = f"{table_name}.parquet"
                expected_top_shortcuts.add(sc_name)
                sc_items = _find_item(store_or_storage, sc_name, source_nav_id, mime_type=SHORTCUT_MIME_TYPE)
                if len(sc_items) != 1:
                    raise StaleNavigationError(
                        f"Expected exactly 1 shortcut for '{sc_name}' in {layer}/current/{source_id}, found {len(sc_items)}"
                    )
                actual_target = _get_target_id(store_or_storage, sc_items[0])
                if actual_target != expected_files[0]["id"]:
                    raise StaleNavigationError(
                        f"Drive shortcut target ID mismatch for '{sc_name}' in {layer}: "
                        f"expected {expected_files[0]['id']}, got {actual_target}"
                    )
            else:
                # Multi-file dataset
                ds_folders = _find_item(store_or_storage, table_name, source_nav_id, mime_type=FOLDER_MIME_TYPE)
                if len(ds_folders) != 1:
                    raise StaleNavigationError(
                        f"Expected multi-file folder '{table_name}' in {layer}/current/{source_id}, found {len(ds_folders)}"
                    )
                ds_folder_id = ds_folders[0]
                part_names: set[str] = set()
                for part_idx, file_entry in enumerate(expected_files):
                    part_name = file_entry.get("name", f"{table_name}--part-{part_idx}.parquet")
                    part_names.add(part_name)
                    part_items = _find_item(store_or_storage, part_name, ds_folder_id, mime_type=SHORTCUT_MIME_TYPE)
                    if len(part_items) != 1:
                        raise StaleNavigationError(
                            f"Expected shortcut '{part_name}' in multi-file folder '{table_name}', found {len(part_items)}"
                        )
                    actual_target = _get_target_id(store_or_storage, part_items[0])
                    if actual_target != file_entry["id"]:
                        raise StaleNavigationError(
                            f"Drive shortcut target ID mismatch for '{part_name}' in {layer}/{table_name}: "
                            f"expected {file_entry['id']}, got {actual_target}"
                        )
                expected_folders[table_name] = part_names

                # Check no surplus shortcuts in multi-file folder
                folder_children = _list_children(store_or_storage, ds_folder_id)
                actual_parts = {c["name"] for c in folder_children if c.get("mimeType") == SHORTCUT_MIME_TYPE}
                if actual_parts != part_names:
                    surplus = actual_parts - part_names
                    raise StaleNavigationError(
                        f"Surplus shortcuts found in multi-file folder '{table_name}' in {layer}: {surplus}"
                    )

        # Check no surplus top-level shortcuts in source_nav_id
        top_children = _list_children(store_or_storage, source_nav_id)
        actual_top_shortcuts = {c["name"] for c in top_children if c.get("mimeType") == SHORTCUT_MIME_TYPE}
        if actual_top_shortcuts != expected_top_shortcuts:
            surplus = actual_top_shortcuts - expected_top_shortcuts
            raise StaleNavigationError(
                f"Surplus shortcuts found in {layer}/current/{source_id}: {surplus}"
            )

        verified_layers[layer] = {
            "status": "verified_current",
            "release_id": expected_release_id,
            "table_count": len(datasets),
        }

    return {"status": "medallion_navigation_verified", "source_id": source_id, "layers": verified_layers}
