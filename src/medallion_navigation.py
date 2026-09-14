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


class NavigationError(RuntimeError):
    """Base exception for navigation errors."""


class StaleNavigationError(NavigationError):
    """Raised when navigation links or index do not match the current release."""


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, indent=2, sort_keys=True).encode("utf-8")


def _find_item(store_or_storage: Any, name: str, parent_id: str) -> list[str]:
    if hasattr(store_or_storage, "find"):
        return store_or_storage.find(name, parent_id)
    if hasattr(store_or_storage, "_list_exact_folders"):
        items = store_or_storage._list_exact_folders(name, parent_id=parent_id)
        return [item["id"] for item in items]
    raise TypeError(f"Unsupported store: {type(store_or_storage)}")


def _mkdir(store_or_storage: Any, name: str, parent_id: str) -> str:
    if hasattr(store_or_storage, "mkdir"):
        return store_or_storage.mkdir(name, parent_id)
    if hasattr(store_or_storage, "get_or_create_nested_folder"):
        return store_or_storage.get_or_create_nested_folder([name], root_id=parent_id)
    raise TypeError(f"Unsupported store: {type(store_or_storage)}")


def _create_shortcut(store_or_storage: Any, name: str, target_id: str, parent_id: str) -> str:
    """Create a Drive shortcut pointing to target_id."""
    if hasattr(store_or_storage, "create_shortcut"):
        return store_or_storage.create_shortcut(name, target_id, parent_id)
    if hasattr(store_or_storage, "drive_service"):
        files = store_or_storage.drive_service.files()
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
    drive_service = getattr(store_or_storage, "drive_service", None) or getattr(
        getattr(store_or_storage, "storage", None), "drive_service", None
    )
    if drive_service:
        try:
            meta = drive_service.files().get(
                fileId=shortcut_id, fields="id,shortcutDetails", supportsAllDrives=True
            ).execute()
            return (meta.get("shortcutDetails") or {}).get("targetId")
        except Exception:
            return None
    return None


def _delete_item(store_or_storage: Any, item_id: str) -> None:
    drive_service = getattr(store_or_storage, "drive_service", None) or getattr(
        getattr(store_or_storage, "storage", None), "drive_service", None
    )
    if drive_service:
        try:
            drive_service.files().delete(fileId=item_id, supportsAllDrives=True).execute()
            return
        except Exception:
            pass
    if hasattr(store_or_storage, "delete"):
        store_or_storage.delete(item_id)


def _sync_shortcut(store_or_storage: Any, name: str, target_id: str, parent_id: str) -> str:
    existing = _find_item(store_or_storage, name, parent_id)
    if existing:
        current_target = _get_target_id(store_or_storage, existing[0])
        if current_target == target_id:
            return existing[0]
        for old_id in existing:
            _delete_item(store_or_storage, old_id)
    return _create_shortcut(store_or_storage, name, target_id, parent_id)


def _create_or_replace_file(store_or_storage: Any, name: str, data: bytes, parent_id: str) -> str:
    """Create or replace a file in parent_id."""
    drive_service = getattr(store_or_storage, "drive_service", None) or getattr(
        getattr(store_or_storage, "storage", None), "drive_service", None
    )
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
    if hasattr(store_or_storage, "drive_service"):
        return store_or_storage.drive_service.files().get_media(
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
    """
    release_id = manifest["release_id"]
    receipts: dict[str, Any] = {"source_id": source_id, "release_id": release_id, "layers": {}}

    # Group datasets by layer
    by_layer: dict[str, list[dict[str, Any]]] = {}
    for dataset in manifest.get("datasets", []):
        layer = dataset.get("layer")
        if layer in {"02_bronze", "03_silver", "04_gold"}:
            by_layer.setdefault(layer, []).append(dataset)

    for layer, datasets in sorted(by_layer.items()):
        layer_folders = _find_item(store_or_storage, layer, root_id)
        if len(layer_folders) != 1:
            raise NavigationError(f"Medallion layer folder '{layer}' missing or ambiguous")
        layer_id = layer_folders[0]

        # Get or create current/
        current_folders = _find_item(store_or_storage, CURRENT_NAV_DIR, layer_id)
        if not current_folders:
            current_id = _mkdir(store_or_storage, CURRENT_NAV_DIR, layer_id)
        else:
            current_id = current_folders[0]

        # Get or create current/<source_id>/
        source_folders = _find_item(store_or_storage, source_id, current_id)
        if not source_folders:
            source_nav_id = _mkdir(store_or_storage, source_id, current_id)
        else:
            source_nav_id = source_folders[0]

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
                            "size": file_entry["size"],
                            "sha256": file_entry["sha256"],
                        }
                    ],
                }
            else:
                # Multi-file dataset (e.g. WDI observations): create dataset folder
                ds_folders = _find_item(store_or_storage, table_name, source_nav_id)
                ds_folder_id = ds_folders[0] if ds_folders else _mkdir(
                    store_or_storage, table_name, source_nav_id
                )
                part_shortcuts = []
                for part_idx, file_entry in enumerate(files):
                    part_name = file_entry.get("name", f"{table_name}--part-{part_idx}.parquet")
                    sc_id = _sync_shortcut(
                        store_or_storage, part_name, file_entry["id"], ds_folder_id
                    )
                    part_shortcuts.append(
                        {
                            "name": part_name,
                            "shortcut_id": sc_id,
                            "target_id": file_entry["id"],
                            "size": file_entry["size"],
                            "sha256": file_entry["sha256"],
                        }
                    )
                table_receipts[table_name] = {
                    "is_multi_part": True,
                    "file_count": len(files),
                    "container_id": ds_folder_id,
                    "shortcuts": part_shortcuts,
                }

        # Write navigation index for this layer
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

    Fails closed if shortcuts are missing, target IDs differ, or index is stale.
    """
    expected_release_id = manifest["release_id"]
    by_layer: dict[str, list[dict[str, Any]]] = {}
    for dataset in manifest.get("datasets", []):
        layer = dataset.get("layer")
        if layer in {"02_bronze", "03_silver", "04_gold"}:
            by_layer.setdefault(layer, []).append(dataset)

    verified_layers = {}
    for layer, datasets in by_layer.items():
        layer_folders = _find_item(store_or_storage, layer, root_id)
        if len(layer_folders) != 1:
            raise StaleNavigationError(f"Layer folder '{layer}' not found or ambiguous")
        layer_id = layer_folders[0]

        current_folders = _find_item(store_or_storage, CURRENT_NAV_DIR, layer_id)
        if len(current_folders) != 1:
            raise StaleNavigationError(f"'{layer}/current' folder not found or ambiguous")
        current_id = current_folders[0]

        source_folders = _find_item(store_or_storage, source_id, current_id)
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

        # Verify tables in index
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
            for file_entry, sc_entry in zip(expected_files, table_meta.get("shortcuts", [])):
                if file_entry["id"] != sc_entry.get("target_id"):
                    raise StaleNavigationError(
                        f"Shortcut target ID mismatch for table '{table_name}' in {layer}: "
                        f"expected {file_entry['id']}, got {sc_entry.get('target_id')}"
                    )

        verified_layers[layer] = {
            "status": "verified_current",
            "release_id": expected_release_id,
            "table_count": len(datasets),
        }

    return {"status": "medallion_navigation_verified", "source_id": source_id, "layers": verified_layers}
