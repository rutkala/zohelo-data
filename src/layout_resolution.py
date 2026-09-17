"""Layout resolution for Google Drive storage: legacy and canonical medallion layout.

Supports dual-read during migration, deterministic writer targeting,
and fail-closed behavior for ambiguous or conflicting pointers.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Canonical layout constants
CANONICAL_RELEASES_FOLDER = "releases"
CANONICAL_CONTROL_FOLDER = "06_control"
CANONICAL_NBP_CONTROL_FOLDER = "nbp"
CANONICAL_SOURCE_CAMPAIGNS_FOLDER = "source_campaigns"
CANONICAL_SOURCES = ("nbp", "bdl", "wdi")
RELEASE_SOURCES = (*CANONICAL_SOURCES, "eurostat")

# Legacy layout constants
LEGACY_NBP_CONTROL_FOLDER = "ingestion-control"
LEGACY_BDL_WRAPPER = "bdl-platform"
LEGACY_WDI_WRAPPER = "wdi-platform"
ARCHIVE_FOLDER = "05_archive"

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut"


class AmbiguousLayoutError(RuntimeError):
    """Raised when conflicting legacy and canonical layout pointers or containers exist."""


class LayoutResolutionError(RuntimeError):
    """Raised when layout resolution fails to find or safely initialize paths."""


def _find_items(store_or_storage: Any, name: str, parent_id: str) -> list[str]:
    """Find exact child IDs, failing closed on incomplete or malformed Drive pages."""
    drive_service = getattr(store_or_storage, "drive_service", None) or getattr(
        getattr(store_or_storage, "storage", None), "drive_service", None
    )
    if drive_service is not None:
        escaped_name = name.replace("\\", "\\\\").replace("'", "\\'")
        q = f"name='{escaped_name}' and '{parent_id}' in parents and trashed=false"
        result: list[str] = []
        token = None
        seen_tokens: set[str] = set()
        for _page_number in range(1000):
            response = drive_service.files().list(
                q=q,
                fields="nextPageToken,incompleteSearch,files(id)",
                pageSize=50,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=token,
            ).execute()
            if not isinstance(response, dict):
                raise LayoutResolutionError("Drive list returned a malformed response")
            incomplete = response.get("incompleteSearch")
            if incomplete not in (None, False):
                raise LayoutResolutionError("Drive list reported an incomplete search")
            files = response.get("files")
            if not isinstance(files, list):
                raise LayoutResolutionError("Drive list response has no valid files list")
            for item in files:
                if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"]:
                    raise LayoutResolutionError("Drive list response contains a malformed item")
                result.append(item["id"])
            next_token = response.get("nextPageToken")
            if next_token in (None, ""):
                return result
            if not isinstance(next_token, str):
                raise LayoutResolutionError("Drive list response has a malformed page token")
            if next_token == token or next_token in seen_tokens:
                raise LayoutResolutionError("Drive list repeated a page token")
            seen_tokens.add(next_token)
            token = next_token
        raise LayoutResolutionError("Drive list exceeded the bounded page limit")
    if hasattr(store_or_storage, "find"):
        return list(store_or_storage.find(name, parent_id))
    if hasattr(store_or_storage, "_list_exact_folders"):
        folders = store_or_storage._list_exact_folders(name, parent_id=parent_id)
        return [f["id"] for f in folders]
    raise TypeError(f"Unsupported store or storage object: {type(store_or_storage)}")

def _mkdir(store_or_storage: Any, name: str, parent_id: str) -> str:
    """Create a folder under parent_id across ReleaseStore or StorageManager."""
    if hasattr(store_or_storage, "get_or_create_nested_folder"):
        return store_or_storage.get_or_create_nested_folder([name], root_id=parent_id)
    if hasattr(store_or_storage, "mkdir"):
        return store_or_storage.mkdir(name, parent_id)
    raise TypeError(f"Unsupported store or storage object: {type(store_or_storage)}")


def detect_layout_mode(store_or_storage: Any, root_id: str) -> str:
    """Detect whether root_id is established as canonical, legacy, fresh, or ambiguous.

    Returns:
        "canonical": Canonical layout established (e.g. releases/nbp exists or 06_control/nbp exists).
        "legacy": Legacy layout established (e.g. root current-release.json, bdl-platform, wdi-platform, or ingestion-control exists).
        "fresh": Neither layout exists (uninitialized isolated root).
        "ambiguous": Both layouts have active pointers or wrappers, or duplicate containers exist.
    """
    has_legacy = False
    has_canonical = False

    # Check legacy indicators
    root_pointers = _find_items(store_or_storage, "current-release.json", root_id)
    if len(root_pointers) > 1:
        return "ambiguous"
    if root_pointers:
        has_legacy = True

    bdl_legacy = _find_items(store_or_storage, LEGACY_BDL_WRAPPER, root_id)
    if len(bdl_legacy) > 1:
        return "ambiguous"
    if bdl_legacy:
        has_legacy = True

    wdi_legacy = _find_items(store_or_storage, LEGACY_WDI_WRAPPER, root_id)
    if len(wdi_legacy) > 1:
        return "ambiguous"
    if wdi_legacy:
        has_legacy = True

    ingestion_control = _find_items(store_or_storage, LEGACY_NBP_CONTROL_FOLDER, root_id)
    if len(ingestion_control) > 1:
        return "ambiguous"
    if ingestion_control:
        has_legacy = True

    # Check canonical indicators
    releases_folders = _find_items(store_or_storage, CANONICAL_RELEASES_FOLDER, root_id)
    if len(releases_folders) > 1:
        return "ambiguous"
    if releases_folders:
        rel_id = releases_folders[0]
        for src in CANONICAL_SOURCES:
            src_folders = _find_items(store_or_storage, src, rel_id)
            if len(src_folders) > 1:
                return "ambiguous"
            if src_folders:
                has_canonical = True
                break

    control_folders = _find_items(store_or_storage, CANONICAL_CONTROL_FOLDER, root_id)
    if len(control_folders) > 1:
        return "ambiguous"
    if control_folders:
        ctrl_id = control_folders[0]
        nbp_ctrl = _find_items(store_or_storage, CANONICAL_NBP_CONTROL_FOLDER, ctrl_id)
        if len(nbp_ctrl) > 1:
            return "ambiguous"
        if nbp_ctrl:
            has_canonical = True

    if has_legacy and has_canonical:
        return "ambiguous"
    if has_canonical:
        return "canonical"
    if has_legacy:
        return "legacy"
    return "fresh"


def resolve_source_release_root(
    store_or_storage: Any,
    root_id: str,
    source: str,
    *,
    is_writer: bool = False,
) -> tuple[str, bool]:
    """Resolve the release root folder ID and direct_releases flag for a source.

    Returns:
        (release_container_id, direct_releases: bool)
        where direct_releases=True indicates releases/<source>/ contains UUID release dirs directly.

    Raises:
        AmbiguousLayoutError: if duplicate containers or both canonical and legacy pointers exist.
        LayoutResolutionError: if resolution fails.
    """
    source = source.lower().strip()
    if source not in RELEASE_SOURCES:
        raise ValueError(f"Unknown source '{source}'; expected one of {RELEASE_SOURCES}")

    if is_writer:
        mode = detect_layout_mode(store_or_storage, root_id)
        if mode == "ambiguous":
            raise AmbiguousLayoutError(
                f"Cannot write for source '{source}': ambiguous or conflicting legacy and canonical layouts found under root"
            )
        if mode == "legacy":
            # Write to established legacy layout
            if source == "nbp":
                root_ptrs = _find_items(store_or_storage, "current-release.json", root_id)
                if len(root_ptrs) > 1:
                    raise AmbiguousLayoutError("Multiple root 'current-release.json' pointers found")
                return root_id, False
            elif source == "bdl":
                bdl_folders = _find_items(store_or_storage, LEGACY_BDL_WRAPPER, root_id)
                if len(bdl_folders) > 1:
                    raise AmbiguousLayoutError(f"Multiple '{LEGACY_BDL_WRAPPER}' folders found under root")
                if len(bdl_folders) != 1:
                    raise LayoutResolutionError(
                        f"Legacy BDL folder '{LEGACY_BDL_WRAPPER}' is missing or ambiguous"
                    )
                return bdl_folders[0], False
            elif source == "wdi":
                wdi_folders = _find_items(store_or_storage, LEGACY_WDI_WRAPPER, root_id)
                if len(wdi_folders) > 1:
                    raise AmbiguousLayoutError(f"Multiple '{LEGACY_WDI_WRAPPER}' folders found under root")
                if len(wdi_folders) != 1:
                    raise LayoutResolutionError(
                        f"Legacy WDI folder '{LEGACY_WDI_WRAPPER}' is missing or ambiguous"
                    )
                return wdi_folders[0], False

        # Fresh or canonical layout: target canonical
        releases_ids = _find_items(store_or_storage, CANONICAL_RELEASES_FOLDER, root_id)
        if len(releases_ids) > 1:
            raise AmbiguousLayoutError("Multiple 'releases' folders found under root")
        if not releases_ids:
            releases_id = _mkdir(store_or_storage, CANONICAL_RELEASES_FOLDER, root_id)
        else:
            releases_id = releases_ids[0]

        source_ids = _find_items(store_or_storage, source, releases_id)
        if len(source_ids) > 1:
            raise AmbiguousLayoutError(f"Multiple '{source}' folders found under releases")
        if not source_ids:
            source_id = _mkdir(store_or_storage, source, releases_id)
        else:
            source_id = source_ids[0]

        return source_id, True

    # Reader path: check both canonical and legacy; fail closed if ambiguous or both exist
    canonical_ptr_found = False
    canonical_folder_id = None
    legacy_ptr_found = False
    legacy_folder_id = None

    # Check canonical
    releases_ids = _find_items(store_or_storage, CANONICAL_RELEASES_FOLDER, root_id)
    if len(releases_ids) > 1:
        raise AmbiguousLayoutError("Multiple 'releases' folders found under root")
    elif len(releases_ids) == 1:
        source_ids = _find_items(store_or_storage, source, releases_ids[0])
        if len(source_ids) > 1:
            raise AmbiguousLayoutError(f"Multiple '{source}' folders found under releases")
        elif len(source_ids) == 1:
            canonical_folder_id = source_ids[0]
            ptrs = _find_items(store_or_storage, "current-release.json", canonical_folder_id)
            if len(ptrs) > 1:
                raise AmbiguousLayoutError(f"Ambiguous current-release.json in releases/{source}")
            elif len(ptrs) == 1:
                canonical_ptr_found = True

    # Check legacy
    if source == "nbp":
        root_ptrs = _find_items(store_or_storage, "current-release.json", root_id)
        if len(root_ptrs) > 1:
            raise AmbiguousLayoutError("Multiple root 'current-release.json' pointers found")
        elif len(root_ptrs) == 1:
            legacy_ptr_found = True
            legacy_folder_id = root_id
    elif source == "bdl":
        bdl_folders = _find_items(store_or_storage, LEGACY_BDL_WRAPPER, root_id)
        if len(bdl_folders) > 1:
            raise AmbiguousLayoutError(f"Multiple '{LEGACY_BDL_WRAPPER}' folders found under root")
        elif len(bdl_folders) == 1:
            legacy_folder_id = bdl_folders[0]
            ptrs = _find_items(store_or_storage, "current-release.json", legacy_folder_id)
            if len(ptrs) > 1:
                raise AmbiguousLayoutError(f"Ambiguous current-release.json in {LEGACY_BDL_WRAPPER}")
            elif len(ptrs) == 1:
                legacy_ptr_found = True
    elif source == "wdi":
        wdi_folders = _find_items(store_or_storage, LEGACY_WDI_WRAPPER, root_id)
        if len(wdi_folders) > 1:
            raise AmbiguousLayoutError(f"Multiple '{LEGACY_WDI_WRAPPER}' folders found under root")
        elif len(wdi_folders) == 1:
            legacy_folder_id = wdi_folders[0]
            ptrs = _find_items(store_or_storage, "current-release.json", legacy_folder_id)
            if len(ptrs) > 1:
                raise AmbiguousLayoutError(f"Ambiguous current-release.json in {LEGACY_WDI_WRAPPER}")
            elif len(ptrs) == 1:
                legacy_ptr_found = True

    if canonical_ptr_found and legacy_ptr_found:
        raise AmbiguousLayoutError(
            f"Conflicting current-release pointers found for '{source}' in both canonical (releases/{source}) and legacy locations"
        )

    if canonical_ptr_found:
        assert canonical_folder_id is not None
        return canonical_folder_id, True

    if legacy_ptr_found:
        assert legacy_folder_id is not None
        return legacy_folder_id, False

    # Neither pointer found: if canonical folder exists, return it
    if canonical_folder_id is not None:
        return canonical_folder_id, True
    if legacy_folder_id is not None:
        return legacy_folder_id, False

    raise LayoutResolutionError(f"No release root found for source '{source}'")


def resolve_nbp_control_root(
    store_or_storage: Any,
    root_id: str,
    *,
    is_writer: bool = False,
) -> str:
    """Resolve the NBP ingestion control folder ID under root_id.

    In canonical layout: 06_control/nbp/
    In legacy layout: ingestion-control/
    """
    if is_writer:
        mode = detect_layout_mode(store_or_storage, root_id)
        if mode == "ambiguous":
            raise AmbiguousLayoutError(
                "Cannot write NBP ingestion state: ambiguous or conflicting legacy and canonical control folders found"
            )
        if mode == "legacy":
            folders = _find_items(store_or_storage, LEGACY_NBP_CONTROL_FOLDER, root_id)
            if len(folders) > 1:
                raise AmbiguousLayoutError("Multiple legacy 'ingestion-control' folders found under root")
            if len(folders) != 1:
                raise LayoutResolutionError("Legacy 'ingestion-control' folder missing or ambiguous")
            return folders[0]

        # Fresh or canonical layout: target 06_control/nbp
        control_ids = _find_items(store_or_storage, CANONICAL_CONTROL_FOLDER, root_id)
        if len(control_ids) > 1:
            raise AmbiguousLayoutError("Multiple '06_control' folders found under root")
        if not control_ids:
            control_id = _mkdir(store_or_storage, CANONICAL_CONTROL_FOLDER, root_id)
        else:
            control_id = control_ids[0]

        nbp_ids = _find_items(store_or_storage, CANONICAL_NBP_CONTROL_FOLDER, control_id)
        if len(nbp_ids) > 1:
            raise AmbiguousLayoutError("Multiple 'nbp' folders found under 06_control")
        if not nbp_ids:
            return _mkdir(store_or_storage, CANONICAL_NBP_CONTROL_FOLDER, control_id)
        return nbp_ids[0]

    # Reader path: check both canonical and legacy; fail closed if ambiguous or both exist
    control_ids = _find_items(store_or_storage, CANONICAL_CONTROL_FOLDER, root_id)
    if len(control_ids) > 1:
        raise AmbiguousLayoutError(f"Multiple '{CANONICAL_CONTROL_FOLDER}' folders found under root")

    canonical_folder_id = None
    if len(control_ids) == 1:
        nbp_ids = _find_items(store_or_storage, CANONICAL_NBP_CONTROL_FOLDER, control_ids[0])
        if len(nbp_ids) > 1:
            raise AmbiguousLayoutError(f"Multiple '{CANONICAL_NBP_CONTROL_FOLDER}' folders found under {CANONICAL_CONTROL_FOLDER}")
        elif len(nbp_ids) == 1:
            canonical_folder_id = nbp_ids[0]

    legacy_folders = _find_items(store_or_storage, LEGACY_NBP_CONTROL_FOLDER, root_id)
    if len(legacy_folders) > 1:
        raise AmbiguousLayoutError(f"Multiple legacy '{LEGACY_NBP_CONTROL_FOLDER}' folders found under root")
    legacy_folder_id = legacy_folders[0] if len(legacy_folders) == 1 else None

    if canonical_folder_id and legacy_folder_id:
        raise AmbiguousLayoutError(
            "Conflicting NBP ingestion control folders found at both '06_control/nbp' and 'ingestion-control'"
        )

    if canonical_folder_id:
        return canonical_folder_id
    if legacy_folder_id:
        return legacy_folder_id

    raise LayoutResolutionError("No NBP ingestion control folder found")
