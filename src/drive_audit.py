"""Read-only Drive structure and publication release audit diagnostic.

This module inspects the Google Drive storage hierarchy for zohelo-data,
validates current publication pointers and manifest checksums, enumerates
historical release directories, and classifies all discovered folders.

Safety and correctness guarantees:
- Strictly read-only: never creates folders, modifies metadata, or writes objects.
- Shared global budget: tracks list, get, and media requests, inspected files,
  and elapsed execution time across ALL operations.
- Bounded metadata downloads: checks declared file size against small cap
  (1 MB) and uses bounded buffer reading so unexpected bodies cannot defeat budget.
- Metadata-only inspection: never downloads Parquet tables or raw archives.
- Protocol schema validation: validates pointer and manifest fields, code_sha,
  UUID format, exact-byte SHA-256, expected scopes, and dataset/artifact
  parent and metadata relationships against src/release_protocol.py.
- Manifest-and-metadata verification: labels checks accurately without claiming
  full data/hash/restore validation.
- Honest release classification: distinguishes verified current releases,
  unverified manifest-present folders, and candidates lacking manifests.
- Ambiguity detection: duplicate root, publication, or release folders are
  reported as ambiguous rather than silently picking the first match.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import re
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from uuid import UUID

logger = logging.getLogger(__name__)

# Constants aligned with src/capacity_report.py and src/release_protocol.py
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut"
MAX_METADATA_BYTES = 1_000_000  # 1 MB bound for pointer/manifest JSONs
RETENTION_POLICY = "retain_all_no_automatic_deletion"

_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,255}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_CODE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

# Platform definitions imported or mirrored from release_protocol
NBP_REQUIRED_DATASETS = frozenset({
    "bronze_nbp_exchange_rates_table_a", "bronze_nbp_exchange_rates_table_b",
    "bronze_nbp_exchange_rates_table_c", "bronze_nbp_gold_prices",
    "nbp_exchange_rates_table_a", "nbp_exchange_rates_table_b",
    "nbp_exchange_rates_table_c", "nbp_gold_prices", "nbp_change_events",
    "fact_fx_quotes", "fact_gold_prices", "dim_date", "dim_currency",
    "dim_source_table", "dim_commodity",
})

BDL_REQUIRED_DATASETS = frozenset({
    "bronze_bdl_variables", "bronze_bdl_subjects", "bronze_bdl_units",
    "bronze_bdl_dictionary_entries", "bronze_bdl_years", "bronze_bdl_observations",
    "bdl_variables", "bdl_subjects", "bdl_units", "bdl_dictionary_entries",
    "bdl_observation_revisions", "bdl_observations", "dim_bdl_period",
    "dim_bdl_subject", "dim_bdl_variable", "dim_bdl_unit",
    "fact_bdl_observations", "mart_bdl_coverage",
})

WDI_REQUIRED_DATASETS = frozenset({
    "bronze_wdi_country", "bronze_wdi_country_series", "bronze_wdi_data",
    "bronze_wdi_footnote", "bronze_wdi_series", "bronze_wdi_series_time",
    "wdi_countries", "wdi_indicators", "wdi_observations",
    "dim_wdi_geography", "dim_wdi_indicator", "dim_wdi_year",
    "fact_wdi_observations", "mart_wdi_coverage",
})

REQUIRED_ARTIFACT_NAMES = frozenset({
    "manifest.json", "catalog.json", "run_results.json",
    "business-catalog.json", "ingestion-state.json",
})


def _bytes(value: Any) -> Optional[int]:
    """Convert size values to non-negative integers; absence/invalid is not zero."""
    if isinstance(value, bool):
        return None
    if isinstance(value, str) and value.isdigit():
        value = int(value)
    return value if isinstance(value, int) and value >= 0 else None


def _escape_drive_query_literal(value: str) -> str:
    """Escape backslashes and single quotes for Drive query literals."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _sha256(data: bytes) -> str:
    """Compute lowercase hexadecimal SHA-256 checksum."""
    return hashlib.sha256(data).hexdigest()


def _is_valid_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def _is_safe_basename(value: Any) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value not in {".", ".."}
        and "/" not in value
        and "\\" not in value
        and "\x00" not in value
    )


class BudgetTracker:
    """Tracks global limits across Drive operations.

    The elapsed-time limit is cooperative: it is checked before API calls and
    media chunks. Callers must also enforce an outer process/request timeout.
    """

    def __init__(
        self,
        max_files: int = 10_000,
        max_requests: int = 250,
        max_seconds: float = 120.0,
        clock: Callable[[], float] = time.monotonic,
        fixture_reader: Optional[Callable[[str], bytes]] = None,
    ):
        self.max_files = max_files
        self.max_requests = max_requests
        self.max_seconds = max_seconds
        self.clock = clock
        # Test-only/injected reader. Production callers must use the bounded
        # MediaIoBaseDownload path below; arbitrary execute() fallback is unsafe.
        self.fixture_reader = fixture_reader
        self.started_at = clock()
        self.files_inspected = 0
        self._inspected_file_ids: Set[str] = set()
        self.requests_made = 0
        self.incomplete_reasons: Set[str] = set()

    def record_request(self) -> bool:
        """Record an API request (list/get/media). Returns False if budget exhausted."""
        if self.requests_made >= self.max_requests:
            self.incomplete_reasons.add("request_budget_exceeded")
            return False
        if self.clock() - self.started_at >= self.max_seconds:
            self.incomplete_reasons.add("time_budget_exceeded")
            return False
        self.requests_made += 1
        return True

    def admit_file(self, file_id: Optional[str] = None) -> bool:
        """Admit one inspected file against file limit. Returns False if limit reached."""
        if file_id is not None and file_id in self._inspected_file_ids:
            return True
        if self.files_inspected >= self.max_files:
            self.incomplete_reasons.add("file_budget_exceeded")
            return False
        self.files_inspected += 1
        if file_id is not None:
            self._inspected_file_ids.add(file_id)
        return True

    @property
    def is_exhausted(self) -> bool:
        if self.requests_made >= self.max_requests:
            self.incomplete_reasons.add("request_budget_exceeded")
            return True
        if self.files_inspected >= self.max_files:
            self.incomplete_reasons.add("file_budget_exceeded")
            return True
        if self.clock() - self.started_at >= self.max_seconds:
            self.incomplete_reasons.add("time_budget_exceeded")
            return True
        return False


def bounded_read_file_bytes(
    drive_files,
    file_id: str,
    budget: BudgetTracker,
    max_bytes: int = MAX_METADATA_BYTES,
    metadata_out: Optional[Dict[str, Any]] = None,
) -> bytes:
    """Read binary file contents from Drive with bounded size cap and streaming buffer.

    Enforces:
    1. Metadata get first to inspect declared size and trashed state.
    2. Size cap verification before allocation.
    3. Capped streaming read into memory buffer.
    """
    if not isinstance(file_id, str) or not _ID_RE.fullmatch(file_id):
        raise ValueError(f"Invalid file_id: {file_id}")

    if not budget.admit_file(file_id):
        raise RuntimeError("File budget exhausted before reading file metadata")

    if not budget.record_request():
        raise RuntimeError("Budget exhausted before reading file metadata")

    meta = drive_files.get(
        fileId=file_id,
        fields="id,name,size,trashed,parents",
        supportsAllDrives=True,
    ).execute(num_retries=0)

    if meta.get("trashed") is True:
        raise ValueError(f"File '{file_id}' is trashed in Drive")

    declared_size = _bytes(meta.get("size"))
    if declared_size is None:
        raise ValueError(f"File '{file_id}' has no known metadata size")
    if declared_size > max_bytes:
        raise ValueError(
            f"File '{file_id}' declared size {declared_size} exceeds maximum metadata cap {max_bytes}"
        )
    if metadata_out is not None:
        metadata_out.update(meta)

    if budget.fixture_reader is not None:
        if not budget.record_request():
            raise RuntimeError("Budget exhausted before reading file media")
        data = budget.fixture_reader(file_id)
        if not isinstance(data, bytes):
            raise ValueError(f"Fixture reader did not return binary content for file {file_id}")
        if len(data) > max_bytes:
            raise ValueError(
                f"File '{file_id}' downloaded {len(data)} bytes exceeding cap of {max_bytes}"
            )
        return data

    request = drive_files.get_media(fileId=file_id, supportsAllDrives=True)

    try:
        from googleapiclient.http import HttpRequest, MediaIoBaseDownload
    except ImportError as exc:
        raise RuntimeError("googleapiclient is required for bounded Drive media reads") from exc
    if not isinstance(request, HttpRequest):
        raise TypeError("non-HTTP media clients require an explicit fixture_reader")
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request, chunksize=64 * 1024)
    done = False
    while not done:
        if not budget.record_request():
            raise RuntimeError("Budget exhausted during media download")
        _status, done = downloader.next_chunk(num_retries=0)
        if buffer.tell() > max_bytes:
            raise ValueError(
                f"File '{file_id}' streaming download exceeded cap of {max_bytes} bytes"
            )
    return buffer.getvalue()


def list_folder_children(
    drive_files,
    parent_id: str,
    budget: BudgetTracker,
    *,
    q_extra: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    """List children of parent_id with complete pagination and budget tracking.

    Returns (items, is_complete).
    """
    if not isinstance(parent_id, str) or not _ID_RE.fullmatch(parent_id):
        raise ValueError(f"Invalid parent_id: {parent_id}")

    escaped_parent = _escape_drive_query_literal(parent_id)
    query = f"'{escaped_parent}' in parents and trashed=false"
    if q_extra:
        query += f" and ({q_extra})"

    items: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()
    seen_tokens: Set[str] = set()
    page_token: Optional[str] = None
    is_complete = True

    while True:
        if not budget.record_request():
            is_complete = False
            break

        list_args = {
            "q": query,
            "spaces": "drive",
            "includeItemsFromAllDrives": True,
            "supportsAllDrives": True,
            "fields": "nextPageToken,incompleteSearch,files(id,name,mimeType,size,createdTime,modifiedTime,parents)",
            "pageSize": 1000,
        }
        if page_token:
            list_args["pageToken"] = page_token

        try:
            response = drive_files.list(**list_args).execute(num_retries=0)
        except Exception as exc:
            budget.incomplete_reasons.add(f"drive_list_failed:{type(exc).__name__}")
            is_complete = False
            break

        if response.get("incompleteSearch"):
            budget.incomplete_reasons.add("incomplete_search_flag")
            is_complete = False

        page_files = response.get("files", [])
        for item in page_files:
            item_id = item.get("id")
            if not item_id or item_id in seen_ids:
                continue
            if not budget.admit_file(item_id):
                is_complete = False
                break
            seen_ids.add(item_id)
            items.append(item)

        page_token = response.get("nextPageToken")
        if not page_token:
            # End of pagination reached naturally
            break
        if budget.is_exhausted:
            is_complete = False
            break
        if page_token in seen_tokens:
            budget.incomplete_reasons.add("repeated_page_token")
            is_complete = False
            break
        seen_tokens.add(page_token)

    return items, is_complete


def find_child_by_name(
    drive_files,
    parent_id: str,
    name: str,
    budget: BudgetTracker,
    *,
    mime_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Find exact untrashed children of parent_id matching name."""
    if not isinstance(parent_id, str) or not _ID_RE.fullmatch(parent_id):
        raise ValueError(f"Invalid parent_id: {parent_id}")
    escaped_parent = _escape_drive_query_literal(parent_id)
    escaped_name = _escape_drive_query_literal(name)
    query = f"'{escaped_parent}' in parents and name='{escaped_name}' and trashed=false"
    if mime_type:
        escaped_mime = _escape_drive_query_literal(mime_type)
        query += f" and mimeType='{escaped_mime}'"

    items: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    seen_tokens: Set[str] = set()

    while True:
        if not budget.record_request():
            break

        list_args = {
            "q": query,
            "spaces": "drive",
            "includeItemsFromAllDrives": True,
            "supportsAllDrives": True,
            "fields": "nextPageToken,files(id,name,mimeType,size,createdTime,modifiedTime,parents)",
            "pageSize": 100,
        }
        if page_token:
            list_args["pageToken"] = page_token

        try:
            response = drive_files.list(**list_args).execute(num_retries=0)
        except Exception as exc:
            budget.incomplete_reasons.add(f"drive_lookup_failed:{type(exc).__name__}")
            break
        for item in response.get("files", []):
            if item.get("name") == name:
                if not budget.admit_file(item.get("id")):
                    break
                items.append(item)

        page_token = response.get("nextPageToken")
        if not page_token or budget.is_exhausted:
            break
        if page_token in seen_tokens:
            budget.incomplete_reasons.add("repeated_page_token")
            break
        seen_tokens.add(page_token)

    return items


def audit_publication_pointer(
    drive_files,
    publication_root_id: str,
    budget: BudgetTracker,
    pointer_name: str = "current-release.json",
    *,
    expected_scope: str = "nbp_platform",
    scope_label: str = "publication_root",
) -> Dict[str, Any]:
    """Audit current-release pointer and verify its manifest and child metadata.

    This performs manifest-and-metadata verification:
    - Verifies pointer JSON schema and values.
    - Verifies manifest original bytes SHA-256 matches pointer.
    - Verifies manifest format, scope, code_sha, status, and passed tests.
    - Checks both dataset and artifact file references in Drive metadata.
    - Checks expected parent relationships: each referenced file must belong
      to the designated release folder.
    - Does NOT download full datasets or compute data hashes.
    """
    matches = find_child_by_name(drive_files, publication_root_id, pointer_name, budget)
    if not matches:
        return {
            "status": "missing_pointer",
            "scope_label": scope_label,
            "publication_root_id": publication_root_id,
            "pointer_name": pointer_name,
            "error": f"No '{pointer_name}' found under {scope_label}",
        }
    if len(matches) > 1:
        return {
            "status": "ambiguous_pointer",
            "scope_label": scope_label,
            "publication_root_id": publication_root_id,
            "pointer_name": pointer_name,
            "match_count": len(matches),
            "match_ids": [m["id"] for m in matches],
            "error": f"Multiple '{pointer_name}' files found under {scope_label}",
        }

    pointer_file = matches[0]
    pointer_id = pointer_file["id"]

    try:
        pointer_raw_bytes = bounded_read_file_bytes(drive_files, pointer_id, budget)
        pointer_json = json.loads(pointer_raw_bytes.decode("utf-8"))
    except Exception as exc:
        return {
            "status": "corrupt_pointer_file",
            "scope_label": scope_label,
            "pointer_id": pointer_id,
            "error": f"Failed to read or parse pointer JSON: {exc}",
        }

    # 1. Validate pointer schema against release_protocol._validate_pointer
    if not isinstance(pointer_json, dict):
        return {"status": "invalid_pointer_schema", "error": "Pointer must be a JSON object"}
    if pointer_json.get("format_version") != 1:
        return {"status": "invalid_pointer_schema", "error": f"Unsupported format_version: {pointer_json.get('format_version')}"}

    release_id = pointer_json.get("release_id")
    if not _is_valid_uuid(release_id):
        return {"status": "invalid_pointer_schema", "error": f"Invalid release_id UUID: {release_id}"}

    manifest_file_id = pointer_json.get("manifest_file_id")
    if not isinstance(manifest_file_id, str) or not _ID_RE.fullmatch(manifest_file_id):
        return {"status": "invalid_pointer_schema", "error": f"Invalid manifest_file_id: {manifest_file_id}"}

    expected_manifest_sha = pointer_json.get("manifest_sha256")
    if not isinstance(expected_manifest_sha, str) or not _SHA_RE.fullmatch(expected_manifest_sha):
        return {"status": "invalid_pointer_schema", "error": f"Invalid manifest_sha256: {expected_manifest_sha}"}

    updated_at_utc = pointer_json.get("updated_at_utc")
    if not isinstance(updated_at_utc, str) or not updated_at_utc.strip():
        return {"status": "invalid_pointer_schema", "error": "Missing or empty updated_at_utc"}

    # 2. Download manifest bytes with bound and verify SHA-256 using original bytes
    manifest_meta: Dict[str, Any] = {}
    try:
        manifest_raw_bytes = bounded_read_file_bytes(
            drive_files, manifest_file_id, budget, metadata_out=manifest_meta
        )
    except Exception as exc:
        return {
            "status": "missing_manifest_file",
            "scope_label": scope_label,
            "pointer_id": pointer_id,
            "manifest_file_id": manifest_file_id,
            "error": f"Could not read referenced manifest file: {exc}",
        }

    observed_manifest_sha = _sha256(manifest_raw_bytes)
    if observed_manifest_sha != expected_manifest_sha:
        return {
            "status": "manifest_checksum_mismatch",
            "scope_label": scope_label,
            "pointer_id": pointer_id,
            "manifest_file_id": manifest_file_id,
            "expected_sha256": expected_manifest_sha,
            "observed_sha256": observed_manifest_sha,
            "error": "Manifest original bytes SHA-256 does not match pointer manifest_sha256",
        }

    try:
        manifest_json = json.loads(manifest_raw_bytes.decode("utf-8"))
    except Exception as exc:
        return {
            "status": "corrupt_manifest_json",
            "scope_label": scope_label,
            "manifest_file_id": manifest_file_id,
            "error": f"Manifest contains invalid JSON: {exc}",
        }

    # 3. Validate manifest content against release_protocol rules
    if not isinstance(manifest_json, dict):
        return {"status": "invalid_manifest_structure", "error": "Manifest must be a JSON object"}
    if manifest_json.get("format_version") not in {1, 2}:
        return {"status": "invalid_manifest_structure", "error": f"Unsupported manifest format_version: {manifest_json.get('format_version')}"}
    if manifest_json.get("status") != "validated":
        return {"status": "invalid_manifest_structure", "error": f"Manifest status is not validated: {manifest_json.get('status')}"}
    if manifest_json.get("tests") != {"passed": True}:
        return {"status": "invalid_manifest_structure", "error": "Manifest tests do not record passed: True"}
    if manifest_json.get("release_id") != release_id:
        return {"status": "invalid_manifest_structure", "error": "Manifest release_id does not match pointer release_id"}

    code_sha = manifest_json.get("code_sha")
    if not isinstance(code_sha, str) or not _CODE_SHA_RE.fullmatch(code_sha):
        return {"status": "invalid_manifest_structure", "error": f"Invalid manifest code_sha: {code_sha}"}

    manifest_scope = manifest_json.get("release_scope")
    expected_manifest_scope = "nbp_silver" if manifest_json.get("format_version") == 1 else expected_scope
    if manifest_json.get("format_version") == 1 and expected_scope != "nbp_platform":
        return {"status": "unexpected_release_scope", "error": "Legacy format-1 manifests are only valid for the NBP publication root"}
    if manifest_scope != expected_manifest_scope:
        return {"status": "unexpected_release_scope", "error": f"Manifest release_scope '{manifest_scope}' != expected '{expected_manifest_scope}'"}

    # 4. Find the release directory to verify expected parent relationships
    releases_dirs = find_child_by_name(drive_files, publication_root_id, "releases", budget, mime_type=FOLDER_MIME_TYPE)
    expected_release_folder_id = None
    if len(releases_dirs) > 1:
        return {
            "status": "ambiguous_releases_folder",
            "scope_label": scope_label,
            "error": "Multiple releases folders found under publication root",
        }
    search_dir = releases_dirs[0]["id"] if releases_dirs else publication_root_id
    rel_folder_matches = find_child_by_name(drive_files, search_dir, release_id, budget, mime_type=FOLDER_MIME_TYPE)
    if len(rel_folder_matches) == 1:
        expected_release_folder_id = rel_folder_matches[0]["id"]
    elif len(rel_folder_matches) > 1:
        return {
            "status": "ambiguous_release_folder",
            "error": f"Multiple folders named '{release_id}' under releases directory",
        }

    if not expected_release_folder_id:
        return {
            "status": "release_directory_not_found",
            "scope_label": scope_label,
            "release_id": release_id,
            "error": f"Release directory '{release_id}' not found under publication releases/",
        }

    if manifest_meta.get("name") != "release.json":
        return {
            "status": "manifest_parent_or_name_mismatch",
            "scope_label": scope_label,
            "error": "Pointer manifest is not named release.json",
        }
    if expected_release_folder_id not in (manifest_meta.get("parents") or []):
        return {
            "status": "manifest_parent_or_name_mismatch",
            "scope_label": scope_label,
            "release_id": release_id,
            "error": "Pointer manifest is not directly inside its release folder",
        }

    # 5. Manifest-and-metadata verification of dataset and artifact references
    datasets_info = []
    layer_summary: Dict[str, int] = {}
    reference_failures = []
    total_dataset_bytes = 0
    total_rows = 0

    manifest_datasets = manifest_json.get("datasets", [])
    if not isinstance(manifest_datasets, list) or not manifest_datasets:
        return {"status": "empty_manifest_datasets", "error": "Manifest declares no datasets"}

    seen_reference_ids: Set[str] = set()
    seen_dataset_ids: Set[str] = set()
    for ds in manifest_datasets:
        if not isinstance(ds, dict):
            return {"status": "invalid_manifest_structure", "error": "Dataset entries must be objects"}
        ds_id = ds.get("dataset_id")
        if not isinstance(ds_id, str) or not ds_id or ds_id in seen_dataset_ids:
            return {"status": "invalid_manifest_structure", "error": f"Invalid or duplicate dataset_id: {ds_id}"}
        seen_dataset_ids.add(ds_id)
        table_name = ds.get("table_name")
        layer = ds.get("layer", "unknown_layer")
        row_count = ds.get("row_count", 0)
        if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count < 0:
            return {"status": "invalid_manifest_structure", "error": f"Invalid row_count for {ds_id}"}
        total_rows += row_count
        layer_summary[layer] = layer_summary.get(layer, 0) + 1

        files_meta = ds.get("files", [])
        if not isinstance(files_meta, list):
            return {"status": "invalid_manifest_structure", "error": f"Dataset files must be a list for {ds_id}"}
        if not files_meta:
            reference_failures.append({"dataset_id": ds_id, "reason": "no_files_declared"})

        for f in files_meta:
            if not isinstance(f, dict):
                return {"status": "invalid_manifest_structure", "error": f"Dataset file entries must be objects for {ds_id}"}
            f_id = f.get("id")
            f_size = _bytes(f.get("size"))
            if not isinstance(f_id, str) or not _ID_RE.fullmatch(f_id):
                reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "invalid_file_id"})
                continue
            if f_id in seen_reference_ids:
                reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "duplicate_file_reference"})
                continue
            seen_reference_ids.add(f_id)
            if f_size is None:
                reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "invalid_declared_size"})
            if "name" in f and not _is_safe_basename(f.get("name")):
                reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "invalid_file_name"})
            if "sha256" in f and (not isinstance(f.get("sha256"), str) or not _SHA_RE.fullmatch(f.get("sha256"))):
                reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "invalid_file_sha256"})

            if not budget.admit_file(f_id):
                reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "file_budget_exhausted"})
                break
            if not budget.record_request():
                reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "budget_exhausted"})
                break

            try:
                f_meta = drive_files.get(
                    fileId=f_id, fields="id,name,trashed,size,parents", supportsAllDrives=True
                ).execute(num_retries=0)
                if f_meta.get("trashed") is True:
                    reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "file_trashed"})
                if expected_release_folder_id and expected_release_folder_id not in (f_meta.get("parents") or []):
                    reference_failures.append({
                        "dataset_id": ds_id, "file_id": f_id,
                        "reason": f"parent_mismatch: expected {expected_release_folder_id} in {f_meta.get('parents')}"
                    })
                meta_size = _bytes(f_meta.get("size"))
                if meta_size is None:
                    reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": "unknown_drive_size"})
                else:
                    total_dataset_bytes += meta_size
                if meta_size is not None and f_size is not None and meta_size != f_size:
                    reference_failures.append({
                        "dataset_id": ds_id, "file_id": f_id,
                        "reason": f"size_mismatch: declared {f_size} vs Drive {meta_size}"
                    })
            except Exception as exc:
                reference_failures.append({"dataset_id": ds_id, "file_id": f_id, "reason": f"get_failed:{exc}"})

        datasets_info.append({
            "dataset_id": ds_id,
            "table_name": table_name,
            "layer": layer,
            "row_count": row_count,
            "file_count": len(files_meta),
            "sha256_signatures": [f.get("sha256") for f in files_meta if f.get("sha256")],
        })

    # Check required artifacts metadata
    manifest_artifacts = manifest_json.get("artifacts", [])
    if not isinstance(manifest_artifacts, list):
        return {"status": "invalid_manifest_structure", "error": "Manifest artifacts must be a list"}
    artifact_names_found = set()
    for art in manifest_artifacts:
        if not isinstance(art, dict):
            return {"status": "invalid_manifest_structure", "error": "Artifact entries must be objects"}
        art_name = art.get("name")
        art_id = art.get("id")
        art_size = _bytes(art.get("size"))
        if art_name in artifact_names_found:
            reference_failures.append({"artifact_name": art_name, "reason": "duplicate_artifact_name"})
        if not _is_safe_basename(art_name):
            reference_failures.append({"artifact_name": art_name, "reason": "invalid_artifact_name"})
        if not isinstance(art_id, str) or not _ID_RE.fullmatch(art_id):
            reference_failures.append({"artifact_name": art_name, "file_id": art_id, "reason": "invalid_artifact_id"})
            continue
        if art_id in seen_reference_ids:
            reference_failures.append({"artifact_name": art_name, "file_id": art_id, "reason": "duplicate_file_reference"})
            continue
        seen_reference_ids.add(art_id)
        if art_size is None:
            reference_failures.append({"artifact_name": art_name, "file_id": art_id, "reason": "invalid_artifact_size"})
        if "sha256" in art and (not isinstance(art.get("sha256"), str) or not _SHA_RE.fullmatch(art.get("sha256"))):
            reference_failures.append({"artifact_name": art_name, "file_id": art_id, "reason": "invalid_artifact_sha256"})
        artifact_names_found.add(art_name)

        if not budget.admit_file(art_id):
            reference_failures.append({"artifact_name": art_name, "file_id": art_id, "reason": "file_budget_exhausted"})
            break
        if not budget.record_request():
            reference_failures.append({"artifact_name": art_name, "reason": "budget_exhausted"})
            break

        try:
            art_meta = drive_files.get(
                fileId=art_id, fields="id,name,trashed,size,parents", supportsAllDrives=True
            ).execute(num_retries=0)
            if art_meta.get("trashed") is True:
                reference_failures.append({"artifact_name": art_name, "file_id": art_id, "reason": "artifact_trashed"})
            if expected_release_folder_id and expected_release_folder_id not in (art_meta.get("parents") or []):
                reference_failures.append({
                    "artifact_name": art_name, "file_id": art_id,
                    "reason": f"artifact_parent_mismatch: expected {expected_release_folder_id} in {art_meta.get('parents')}"
                })
            if art_meta.get("name") != art_name:
                reference_failures.append({
                    "artifact_name": art_name, "file_id": art_id,
                    "reason": f"name_mismatch: manifest {art_name} vs Drive {art_meta.get('name')}",
                })
            meta_size = _bytes(art_meta.get("size"))
            if meta_size is None:
                reference_failures.append({"artifact_name": art_name, "file_id": art_id, "reason": "unknown_drive_size"})
            elif art_size is not None and meta_size != art_size:
                reference_failures.append({
                    "artifact_name": art_name, "file_id": art_id,
                    "reason": f"size_mismatch: declared {art_size} vs Drive {meta_size}",
                })
        except Exception as exc:
            reference_failures.append({"artifact_name": art_name, "file_id": art_id, "reason": f"artifact_get_failed:{exc}"})

    required_artifacts = (
        {"manifest.json", "catalog.json", "run_results.json"}
        if manifest_json.get("format_version") == 1
        else REQUIRED_ARTIFACT_NAMES
    )
    missing_required_artifacts = sorted(required_artifacts - artifact_names_found)
    if missing_required_artifacts:
        reference_failures.append({"reason": f"missing_required_artifacts:{missing_required_artifacts}"})

    if reference_failures:
        return {
            "status": "reference_metadata_failed",
            "scope_label": scope_label,
            "release_id": release_id,
            "manifest_file_id": manifest_file_id,
            "manifest_sha256_verified": True,
            "manifest_scope": manifest_scope,
            "reference_failures": reference_failures,
            "error": f"{len(reference_failures)} dataset or artifact references failed metadata verification",
        }

    return {
        "status": "current_manifest_and_metadata_verified",
        "scope_label": scope_label,
        "pointer_file_id": pointer_id,
        "updated_at_utc": updated_at_utc,
        "format_version": pointer_json.get("format_version"),
        "release_id": release_id,
        "release_folder_id": expected_release_folder_id,
        "manifest_file_id": manifest_file_id,
        "manifest_sha256_verified": True,
        "manifest_sha256": observed_manifest_sha,
        "manifest_scope": manifest_scope,
        "code_sha": code_sha,
        "created_at_utc": manifest_json.get("created_at_utc"),
        "dataset_count": len(manifest_datasets),
        "artifact_count": len(manifest_artifacts),
        "total_rows": total_rows,
        "total_dataset_bytes": total_dataset_bytes,
        "layer_counts": layer_summary,
        "reference_failures": [],
        "datasets": datasets_info,
    }


def audit_release_directories(
    drive_files,
    releases_folder_id: str,
    budget: BudgetTracker,
    current_release_id: Optional[str] = None,
    current_pointer_verified: bool = False,
    current_manifest_file_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Inspect all release subdirectories under a releases folder.

    Applies honest classes:
    - current_manifest_and_metadata_verified: current release matching pointer and passed checks.
    - manifest_present_unverified: contains release.json, but is not active or verified.
    - no_manifest_candidate: missing release.json.
    - partial: child enumeration truncated by budget.
    """
    children, is_complete = list_folder_children(drive_files, releases_folder_id, budget)
    folders = [c for c in children if c.get("mimeType") == FOLDER_MIME_TYPE]
    folders.sort(key=lambda x: x.get("createdTime") or x.get("name", ""))

    classified_releases = []
    current_folder_found = False
    total_retained_bytes = 0
    unknown_size_count = 0
    total_retained_files = 0
    manifests_by_folder: Dict[str, Dict[str, Any]] = {}

    for folder in folders:
        f_id = folder["id"]
        f_name = folder["name"]
        created = folder.get("createdTime")
        modified = folder.get("modifiedTime")
        is_current = (f_name == current_release_id)
        if is_current:
            current_folder_found = True

        # List files inside release folder (bounded)
        rel_files, rel_complete = list_folder_children(drive_files, f_id, budget)
        if not rel_complete:
            is_complete = False
        manifest_file = next((rf for rf in rel_files if rf.get("name") == "release.json"), None)
        has_manifest = (manifest_file is not None)

        dir_bytes = 0
        dir_unknown_sizes = 0
        file_count = 0
        for rf in rel_files:
            if rf.get("mimeType") == FOLDER_MIME_TYPE:
                continue
            file_count += 1
            sz = _bytes(rf.get("size"))
            if sz is not None:
                dir_bytes += sz
            else:
                dir_unknown_sizes += 1

        total_retained_bytes += dir_bytes
        unknown_size_count += dir_unknown_sizes
        total_retained_files += file_count

        # Honest classification
        if not rel_complete:
            classification = "partial"
        elif (
            is_current
            and current_pointer_verified
            and manifest_file is not None
            and manifest_file.get("id") == current_manifest_file_id
        ):
            classification = "current_manifest_and_metadata_verified"
        elif has_manifest:
            classification = "manifest_present_unverified"
        else:
            classification = "no_manifest_candidate"

        # Bounded manifest read for comparing dataset signatures across releases
        dataset_signatures: Dict[str, str] = {}
        if has_manifest and len(manifests_by_folder) < 3 and not budget.is_exhausted:
            try:
                m_bytes = bounded_read_file_bytes(drive_files, manifest_file["id"], budget)
                m_json = json.loads(m_bytes.decode("utf-8"))
                for ds in m_json.get("datasets", []):
                    ds_id = ds.get("dataset_id")
                    first_file = (ds.get("files") or [{}])[0]
                    if ds_id and first_file.get("sha256"):
                        dataset_signatures[ds_id] = first_file["sha256"]
                manifests_by_folder[f_name] = {
                    "code_sha": m_json.get("code_sha"),
                    "created_at_utc": m_json.get("created_at_utc"),
                    "signatures": dataset_signatures,
                }
            except Exception:
                pass

        classified_releases.append({
            "folder_id": f_id,
            "folder_name": f_name,
            "created_time": created,
            "modified_time": modified,
            "file_count": file_count,
            "known_bytes": dir_bytes,
            "classification": classification,
            "is_current": is_current,
        })

    # Compare metadata signatures between consecutive releases
    signature_comparisons = []
    folder_names = list(manifests_by_folder.keys())
    for i in range(len(folder_names) - 1):
        prev_name, curr_name = folder_names[i], folder_names[i + 1]
        prev_sig = manifests_by_folder[prev_name]["signatures"]
        curr_sig = manifests_by_folder[curr_name]["signatures"]
        same_data = (prev_sig == curr_sig and bool(prev_sig))
        same_code = (manifests_by_folder[prev_name]["code_sha"] == manifests_by_folder[curr_name]["code_sha"])
        signature_comparisons.append({
            "earlier_release": prev_name,
            "later_release": curr_name,
            "identical_dataset_hashes": same_data,
            "identical_code_sha": same_code,
        })

    # Cadence calculation across releases with valid created_time
    cadence_hours = []
    prev_dt = None
    for r in classified_releases:
        c_str = r.get("created_time")
        if c_str:
            try:
                dt = datetime.fromisoformat(c_str.replace("Z", "+00:00"))
                if prev_dt:
                    diff_hours = (dt - prev_dt).total_seconds() / 3600.0
                    cadence_hours.append(round(diff_hours, 2))
                prev_dt = dt
            except ValueError:
                pass

    counts_by_class: Dict[str, int] = {}
    for r in classified_releases:
        counts_by_class[r["classification"]] = counts_by_class.get(r["classification"], 0) + 1

    return {
        "status": "complete" if is_complete else "partial",
        "current_release_id": current_release_id,
        "current_folder_found": current_folder_found,
        "total_release_folders": len(folders),
        "counts_by_classification": counts_by_class,
        "total_retained_files": total_retained_files,
        "total_known_bytes": total_retained_bytes,
        "unknown_size_file_count": unknown_size_count,
        "releases": classified_releases,
        "signature_comparisons": signature_comparisons,
        "cadence_hours_between_releases": cadence_hours,
        "average_cadence_hours": round(sum(cadence_hours) / len(cadence_hours), 2) if cadence_hours else None,
    }


def classify_top_level_folder(name: str) -> Dict[str, str]:
    """Map top-level path to honest purpose, producer, consumer, and classification."""
    mappings = {
        "01_landing": {
            "purpose": "Retained raw source inputs, exact HTTP responses, and bulk archives",
            "producer": "Ingestion adapters (NBP, GUS BDL, WDI, Eurostat, OpenData)",
            "consumers": "Bronze loaders, dbt ingestion models, replay/audit verification",
            "classification": "active",
        },
        "02_bronze": {
            "purpose": "Streaming Bronze entity Parquets for OpenData and legacy NBP files",
            "producer": "OpenData bronze loader; legacy NBP runners",
            "consumers": "Portal Lakehouse Explorer, dbt staging models",
            "classification": "active_and_historical",
        },
        "03_silver": {
            "purpose": "Historical prototype folder; current active silver data lives in release directories",
            "producer": "Legacy NBP transformation scripts (retired)",
            "consumers": "Unresolved pending live evidence; portal reads release packages",
            "classification": "unresolved_historical",
        },
        "04_gold": {
            "purpose": "Historical prototype folder; current active gold data lives in release directories",
            "producer": "Legacy NBP transformation scripts (retired)",
            "consumers": "Unresolved pending live evidence; portal reads release packages",
            "classification": "unresolved_historical",
        },
        "05_archive": {
            "purpose": "Archived lifecycle material and legacy migrated datasets",
            "producer": "Manual migrations and lifecycle archival scripts",
            "consumers": "Historical audits",
            "classification": "retained_historical",
        },
        "06_control": {
            "purpose": "Source campaign state shards, quota ledgers, and Landing pointers",
            "producer": "Campaign runners (src/source_campaign.py, opendata_bronze_loader.py)",
            "consumers": "Campaign runners, portal landing catalog",
            "classification": "active",
        },
        "ingestion-control": {
            "purpose": "NBP ingestion state snapshots, attempt receipts, and raw references",
            "producer": "src/nbp_platform.py via DriveStateStore",
            "consumers": "NBP platform runner",
            "classification": "active_nbp_convention",
        },
        "releases": {
            "purpose": "Publication root: immutable release directories and current pointer",
            "producer": "src/nbp_platform.py, bdl_platform.py, wdi_platform.py via src/release_protocol.py",
            "consumers": "Portal release catalog, restore_release.py, SQL consumers",
            "classification": "active",
        },
        "bdl-platform": {
            "purpose": "BDL publication namespace: immutable release directories and current pointer",
            "producer": "src/bdl_platform.py via _release_root and src/release_protocol.py",
            "consumers": "Portal release catalog (explicitly checks bdl-platform/)",
            "classification": "active_or_legacy",
        },
        "wdi-platform": {
            "purpose": "WDI publication namespace: immutable release directories and current pointer",
            "producer": "src/wdi_platform.py via _release_root and src/release_protocol.py",
            "consumers": "Portal release catalog (checks wdi-platform/)",
            "classification": "active_or_legacy",
        },
        "promotion-audits": {
            "purpose": "Optional audit trail receipts for release promotions (code-supported path)",
            "producer": "scripts/promote_release.py",
            "consumers": "Release auditing",
            "classification": "optional_code_supported",
        },
    }
    return mappings.get(name, {
        "purpose": "Unclassified or custom directory",
        "producer": "Unknown",
        "consumers": "Unknown",
        "classification": "unresolved",
    })


def audit_physical_storage_map(
    drive_files,
    root_id: str,
    budget: BudgetTracker,
) -> Dict[str, Any]:
    """Produce an honest physical storage map of top-level folders under root_id."""
    children, is_complete = list_folder_children(drive_files, root_id, budget)

    folders_by_name: Dict[str, List[Dict[str, Any]]] = {}
    direct_files: List[Dict[str, Any]] = []

    for item in children:
        if item.get("mimeType") == FOLDER_MIME_TYPE:
            folders_by_name.setdefault(item["name"], []).append(item)
        else:
            direct_files.append(item)

    storage_map = []
    duplicate_folders = []

    for name, items in sorted(folders_by_name.items()):
        if len(items) > 1:
            duplicate_folders.append({
                "name": name,
                "count": len(items),
                "folder_ids": [i["id"] for i in items],
            })

        # Process each folder
        for idx, folder in enumerate(items):
            classification = classify_top_level_folder(name)
            folder_label = name if len(items) == 1 else f"{name} (duplicate #{idx + 1})"

            # Count direct child items bounded
            child_items, child_complete = list_folder_children(drive_files, folder["id"], budget)
            if not child_complete:
                is_complete = False
            dir_child_folders = sum(1 for c in child_items if c.get("mimeType") == FOLDER_MIME_TYPE)
            dir_child_files = sum(1 for c in child_items if c.get("mimeType") != FOLDER_MIME_TYPE)
            dir_known_bytes = sum(_bytes(c.get("size")) or 0 for c in child_items if c.get("mimeType") != FOLDER_MIME_TYPE)

            storage_map.append({
                "name": folder_label,
                "id": folder["id"],
                "created_time": folder.get("createdTime"),
                "modified_time": folder.get("modifiedTime"),
                "purpose": classification["purpose"],
                "producer": classification["producer"],
                "consumers": classification["consumers"],
                "classification": classification["classification"],
                "is_ambiguous": (len(items) > 1),
                "direct_child_folder_count": dir_child_folders,
                "direct_child_file_count": dir_child_files,
                "direct_known_bytes": dir_known_bytes,
            })

    return {
        "status": "complete" if is_complete else "partial",
        "top_level_folders": storage_map,
        "direct_files": [
            {"id": f["id"], "name": f["name"], "size": _bytes(f.get("size")), "mimeType": f.get("mimeType")}
            for f in direct_files
        ],
        "duplicate_folders": duplicate_folders,
    }


def run_full_drive_audit(
    drive_service,
    root_id: Optional[str] = None,
    root_name: str = "zohelo-data",
    *,
    max_files: int = 10_000,
    max_requests: int = 250,
    max_seconds: float = 120.0,
    clock: Callable[[], float] = time.monotonic,
) -> Dict[str, Any]:
    """Execute a read-only audit with shared cooperative budgets.

    ``max_seconds`` cannot interrupt a blocked HTTP transport; callers must
    provide an outer timeout in addition to this between-call check.
    """
    drive_files = drive_service.files()
    budget = BudgetTracker(
        max_files=max_files,
        max_requests=max_requests,
        max_seconds=max_seconds,
        clock=clock,
    )

    # 1. Resolve root (read-only, ambiguity-aware)
    resolved_root_id = root_id
    if not resolved_root_id:
        escaped_name = _escape_drive_query_literal(root_name)
        q = f"name='{escaped_name}' and mimeType='{FOLDER_MIME_TYPE}' and trashed=false"
        roots: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        seen_tokens: Set[str] = set()
        while True:
            if not budget.record_request():
                return {"status": "audit_incomplete", "incomplete_reasons": sorted(budget.incomplete_reasons)}
            list_args = {
                "q": q,
                "spaces": "drive",
                "includeItemsFromAllDrives": True,
                "supportsAllDrives": True,
                "fields": "nextPageToken,incompleteSearch,files(id,name,mimeType)",
                "pageSize": 100,
            }
            if page_token:
                list_args["pageToken"] = page_token
            try:
                response = drive_files.list(**list_args).execute(num_retries=0)
            except Exception as exc:
                budget.incomplete_reasons.add(f"drive_root_search_failed:{type(exc).__name__}")
                return {"status": "audit_incomplete", "incomplete_reasons": sorted(budget.incomplete_reasons)}
            if response.get("incompleteSearch"):
                budget.incomplete_reasons.add("incomplete_search_flag")
            for item in response.get("files", []):
                if not budget.admit_file(item.get("id")):
                    break
                roots.append(item)
            page_token = response.get("nextPageToken")
            if not page_token or budget.is_exhausted:
                break
            if page_token in seen_tokens:
                budget.incomplete_reasons.add("repeated_page_token")
                break
            seen_tokens.add(page_token)
        if budget.incomplete_reasons:
            return {"status": "audit_incomplete", "incomplete_reasons": sorted(budget.incomplete_reasons)}
        if not roots:
            return {
                "status": "root_not_found",
                "error": f"Root folder '{root_name}' not found in Drive (read-only search)",
            }
        if len(roots) > 1:
            return {
                "status": "ambiguous_root",
                "error": f"Multiple root folders named '{root_name}' found (count: {len(roots)})",
                "root_matches": roots,
            }
        resolved_root_id = roots[0]["id"]

    # 2. Produce physical storage map of top-level folders
    physical_map = audit_physical_storage_map(drive_files, resolved_root_id, budget)
    if budget.incomplete_reasons:
        return {"status": "audit_incomplete", "incomplete_reasons": sorted(budget.incomplete_reasons)}

    # 3. Check for canonical releases folder and subfolders
    releases_folders = find_child_by_name(drive_files, resolved_root_id, "releases", budget, mime_type=FOLDER_MIME_TYPE)
    releases_root_id = releases_folders[0]["id"] if len(releases_folders) == 1 else None

    # NBP publication pointer & releases
    nbp_canonical = find_child_by_name(drive_files, releases_root_id, "nbp", budget, mime_type=FOLDER_MIME_TYPE) if releases_root_id else []
    nbp_pointer = None
    nbp_releases_audit = None
    if len(nbp_canonical) == 1:
        nbp_dir_id = nbp_canonical[0]["id"]
        nbp_pointer = audit_publication_pointer(
            drive_files, nbp_dir_id, budget, "current-release.json",
            expected_scope="nbp_platform", scope_label="nbp_canonical",
        )
        current_nbp_id = nbp_pointer.get("release_id") if nbp_pointer else None
        nbp_is_verified = (nbp_pointer.get("status") == "current_manifest_and_metadata_verified")
        nbp_releases_audit = audit_release_directories(
            drive_files, nbp_dir_id, budget,
            current_release_id=current_nbp_id,
            current_pointer_verified=nbp_is_verified,
            current_manifest_file_id=nbp_pointer.get("manifest_file_id") if nbp_pointer else None,
        )
    else:
        # Legacy NBP: root current-release.json and releases/
        nbp_pointer = audit_publication_pointer(
            drive_files, resolved_root_id, budget, "current-release.json",
            expected_scope="nbp_platform", scope_label="nbp_root",
        )
        if releases_root_id:
            current_nbp_id = nbp_pointer.get("release_id") if nbp_pointer else None
            nbp_is_verified = (nbp_pointer.get("status") == "current_manifest_and_metadata_verified")
            nbp_releases_audit = audit_release_directories(
                drive_files, releases_root_id, budget,
                current_release_id=current_nbp_id,
                current_pointer_verified=nbp_is_verified,
                current_manifest_file_id=nbp_pointer.get("manifest_file_id") if nbp_pointer else None,
            )

    # 4. BDL publication pointer & releases
    bdl_canonical = find_child_by_name(drive_files, releases_root_id, "bdl", budget, mime_type=FOLDER_MIME_TYPE) if releases_root_id else []
    bdl_pointer = None
    bdl_releases_audit = None
    if len(bdl_canonical) == 1:
        bdl_dir_id = bdl_canonical[0]["id"]
        bdl_pointer = audit_publication_pointer(
            drive_files, bdl_dir_id, budget, "current-release.json",
            expected_scope="bdl_platform", scope_label="bdl_canonical",
        )
        current_bdl_id = bdl_pointer.get("release_id") if bdl_pointer else None
        bdl_is_verified = (bdl_pointer.get("status") == "current_manifest_and_metadata_verified")
        bdl_releases_audit = audit_release_directories(
            drive_files, bdl_dir_id, budget,
            current_release_id=current_bdl_id,
            current_pointer_verified=bdl_is_verified,
            current_manifest_file_id=bdl_pointer.get("manifest_file_id") if bdl_pointer else None,
        )
    else:
        # Legacy BDL: bdl-platform/current-release.json and bdl-platform/releases
        bdl_folders = find_child_by_name(drive_files, resolved_root_id, "bdl-platform", budget, mime_type=FOLDER_MIME_TYPE)
        if len(bdl_folders) == 1:
            bdl_folder_id = bdl_folders[0]["id"]
            bdl_pointer = audit_publication_pointer(
                drive_files, bdl_folder_id, budget, "current-release.json",
                expected_scope="bdl_platform", scope_label="bdl_platform",
            )
            bdl_rel_dirs = find_child_by_name(drive_files, bdl_folder_id, "releases", budget, mime_type=FOLDER_MIME_TYPE)
            if len(bdl_rel_dirs) == 1:
                current_bdl_id = bdl_pointer.get("release_id") if bdl_pointer else None
                bdl_is_verified = (bdl_pointer.get("status") == "current_manifest_and_metadata_verified")
                bdl_releases_audit = audit_release_directories(
                    drive_files, bdl_rel_dirs[0]["id"], budget,
                    current_release_id=current_bdl_id,
                    current_pointer_verified=bdl_is_verified,
                    current_manifest_file_id=bdl_pointer.get("manifest_file_id") if bdl_pointer else None,
                )
            elif len(bdl_rel_dirs) > 1:
                bdl_releases_audit = {"status": "ambiguous_releases_folder", "count": len(bdl_rel_dirs)}
        elif len(bdl_folders) > 1:
            bdl_pointer = {"status": "ambiguous_bdl_platform_folder", "count": len(bdl_folders)}
        else:
            bdl_pointer = {"status": "bdl_platform_folder_not_found"}

    # 5. WDI publication pointer & releases
    wdi_canonical = find_child_by_name(drive_files, releases_root_id, "wdi", budget, mime_type=FOLDER_MIME_TYPE) if releases_root_id else []
    wdi_pointer = None
    wdi_releases_audit = None
    if len(wdi_canonical) == 1:
        wdi_dir_id = wdi_canonical[0]["id"]
        wdi_pointer = audit_publication_pointer(
            drive_files, wdi_dir_id, budget, "current-release.json",
            expected_scope="wdi_platform", scope_label="wdi_canonical",
        )
        current_wdi_id = wdi_pointer.get("release_id") if wdi_pointer else None
        wdi_is_verified = (wdi_pointer.get("status") == "current_manifest_and_metadata_verified")
        wdi_releases_audit = audit_release_directories(
            drive_files, wdi_dir_id, budget,
            current_release_id=current_wdi_id,
            current_pointer_verified=wdi_is_verified,
            current_manifest_file_id=wdi_pointer.get("manifest_file_id") if wdi_pointer else None,
        )
    else:
        # Legacy WDI: wdi-platform/current-release.json and wdi-platform/releases
        wdi_folders = find_child_by_name(drive_files, resolved_root_id, "wdi-platform", budget, mime_type=FOLDER_MIME_TYPE)
        if len(wdi_folders) == 1:
            wdi_folder_id = wdi_folders[0]["id"]
            wdi_pointer = audit_publication_pointer(
                drive_files, wdi_folder_id, budget, "current-release.json",
                expected_scope="wdi_platform", scope_label="wdi_platform",
            )
            wdi_rel_dirs = find_child_by_name(drive_files, wdi_folder_id, "releases", budget, mime_type=FOLDER_MIME_TYPE)
            if len(wdi_rel_dirs) == 1:
                current_wdi_id = wdi_pointer.get("release_id") if wdi_pointer else None
                wdi_is_verified = (wdi_pointer.get("status") == "current_manifest_and_metadata_verified")
                wdi_releases_audit = audit_release_directories(
                    drive_files, wdi_rel_dirs[0]["id"], budget,
                    current_release_id=current_wdi_id,
                    current_pointer_verified=wdi_is_verified,
                    current_manifest_file_id=wdi_pointer.get("manifest_file_id") if wdi_pointer else None,
                )
            elif len(wdi_rel_dirs) > 1:
                wdi_releases_audit = {"status": "ambiguous_releases_folder", "count": len(wdi_rel_dirs)}
        elif len(wdi_folders) > 1:
            wdi_pointer = {"status": "ambiguous_wdi_platform_folder", "count": len(wdi_folders)}
        else:
            wdi_pointer = {"status": "wdi_platform_folder_not_found"}

    # Determine overall audit status
    has_ambiguity = bool(
        physical_map.get("duplicate_folders")
        or (nbp_pointer and "ambiguous" in nbp_pointer.get("status", ""))
        or (bdl_pointer and "ambiguous" in bdl_pointer.get("status", ""))
        or (wdi_pointer and "ambiguous" in wdi_pointer.get("status", ""))
        or (nbp_releases_audit and "ambiguous" in nbp_releases_audit.get("status", ""))
        or (bdl_releases_audit and "ambiguous" in bdl_releases_audit.get("status", ""))
        or (wdi_releases_audit and "ambiguous" in wdi_releases_audit.get("status", ""))
    )

    nbp_verified = (nbp_pointer and nbp_pointer.get("status") == "current_manifest_and_metadata_verified")
    bdl_verified = (bdl_pointer and bdl_pointer.get("status") == "current_manifest_and_metadata_verified")
    wdi_verified = (
        wdi_pointer is None
        or wdi_pointer.get("status") == "wdi_platform_folder_not_found"
        or wdi_pointer.get("status") == "current_manifest_and_metadata_verified"
    )
    releases_complete = (
        nbp_releases_audit and nbp_releases_audit.get("current_folder_found")
        and bdl_releases_audit and bdl_releases_audit.get("current_folder_found")
    )

    if has_ambiguity:
        overall_status = "ambiguous_structure"
    elif budget.incomplete_reasons:
        overall_status = "audit_incomplete"
    elif nbp_verified and bdl_verified and wdi_verified and releases_complete:
        overall_status = "audit_manifest_and_metadata_verified"
    else:
        overall_status = "audit_verification_failed"

    duration = round(clock() - budget.started_at, 3)

    return {
        "status": overall_status,
        "audited_at_utc": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": duration,
        "read_only": True,
        "retention_policy": RETENTION_POLICY,
        "budget_summary": {
            "requests_made": budget.requests_made,
            "max_requests": budget.max_requests,
            "files_inspected": budget.files_inspected,
            "max_files": budget.max_files,
            "duration_seconds": duration,
            "max_seconds": budget.max_seconds,
            "incomplete_reasons": sorted(budget.incomplete_reasons),
        },
        "root": {
            "name": root_name,
            "id": resolved_root_id,
        },
        "physical_storage_map": physical_map,
        "nbp_current_pointer": nbp_pointer,
        "bdl_current_pointer": bdl_pointer,
        "wdi_current_pointer": wdi_pointer,
        "nbp_releases": nbp_releases_audit,
        "bdl_releases": bdl_releases_audit,
        "wdi_releases": wdi_releases_audit,
    }
