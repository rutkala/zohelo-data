"""Native-only transfer of GLEIF's current three-member Golden Copy product.

This adapter downloads the provider's small publication descriptor, pins its three
CSV archive URLs and expected byte sizes, and transfers those bytes unchanged.  It
does not unpack or inspect archive payloads.  A completed receipt proves only one
current ``lei2``/``rr``/``repex`` product snapshot; it never claims provider history.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sys
from typing import Any, Mapping
from urllib.parse import urlsplit
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ingestion.bulk_transport import (  # noqa: E402
    BulkDriveRawStore,
    BulkTransportError,
    DriveIntegrityError,
    fetch_to_file,
)
from ingestion.source_campaign_store import (  # noqa: E402
    CampaignStoreError,
    DriveCampaignStore,
)
from storage_manager import StorageManager  # noqa: E402

logger = logging.getLogger("gleif_bulk")

SOURCE_ID = "gleif"
PRODUCT_MEMBERS = ("lei2", "rr", "repex")
DISCOVERY_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"
ALLOWED_HOSTS = frozenset({"goldencopy.gleif.org"})
DISCOVERY_MAX_BYTES = 4 * 1024 * 1024
DOWNLOAD_TIMEOUT_SECONDS = 300
# This is a transfer safety ceiling, not a claim about a provider's current size.
MAX_MEMBER_BYTES = 2 * 1024 * 1024 * 1024
CACHE_NAME = "gleif-native-cache.json"

# Kept as a public compatibility constant.  Filenames are disposable local staging
# names only; durable raw objects have immutable content-addressed names.
GLEIF_DATASETS = [
    {
        "dataset_key": "lei2",
        "url": "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv",
        "description": "GLEIF Golden Copy Level 1 LEI records",
        "default_filename": "gleif_goldencopy_lei2.csv.zip",
    },
    {
        "dataset_key": "rr",
        "url": "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/rr/latest.csv",
        "description": "GLEIF Golden Copy Level 2 Relationship Records",
        "default_filename": "gleif_goldencopy_rr.csv.zip",
    },
    {
        "dataset_key": "repex",
        "url": "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/repex/latest.csv",
        "description": "GLEIF Golden Copy Level 2 Reporting Exceptions",
        "default_filename": "gleif_goldencopy_repex.csv.zip",
    },
]


class GleifBulkError(RuntimeError):
    """GLEIF discovery, cache, transfer, or immutable publication failed."""


def _hash_file(path: Path) -> tuple[str, str]:
    """Return native-file checksums without interpreting payload bytes."""
    sha256 = hashlib.sha256()
    md5 = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            sha256.update(chunk)
            md5.update(chunk)
    return sha256.hexdigest(), md5.hexdigest()


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise GleifBulkError("GLEIF control metadata is not safe JSON") from exc


def _selected_keys(datasets: list[str] | None) -> tuple[str, ...]:
    if datasets is None:
        return PRODUCT_MEMBERS
    if not isinstance(datasets, list) or not datasets:
        raise GleifBulkError("dataset selection must name one or more GLEIF product members")
    if any(not isinstance(key, str) for key in datasets):
        raise GleifBulkError("dataset selection contains a non-string member")
    if len(set(datasets)) != len(datasets):
        raise GleifBulkError("dataset selection contains duplicate members")
    unknown = sorted(set(datasets) - set(PRODUCT_MEMBERS))
    if unknown:
        raise GleifBulkError("unknown GLEIF product member: " + ", ".join(unknown))
    # Preserve authoritative product order in every receipt regardless of CLI order.
    return tuple(key for key in PRODUCT_MEMBERS if key in datasets)


_PUBLISH_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MD5_RE = re.compile(r"^[0-9a-f]{32}$")


def _safe_publish_date(value: Any) -> str:
    if not isinstance(value, str) or _PUBLISH_DATE_RE.fullmatch(value) is None:
        raise GleifBulkError("GLEIF discovery has an invalid publish_date")
    try:
        datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise GleifBulkError("GLEIF discovery publish_date is not a calendar time") from exc
    # Golden Copy filenames encode publication time to minutes.  Do not quietly
    # collapse a provider timestamp that cannot be bound exactly to that name.
    if not value.endswith(":00"):
        raise GleifBulkError("GLEIF discovery publish_date has unsupported non-minute seconds")
    return value


def _safe_url(value: Any, *, key: str, publish_date: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise GleifBulkError("GLEIF discovery has an invalid native archive URL")
    parsed = urlsplit(value)
    date_part, time_part = publish_date.split(" ")
    year, month, day = date_part.split("-")
    stamp = date_part.replace("-", "") + "-" + time_part[:5].replace(":", "")
    pattern = (
        rf"/storage/golden-copy-files/{year}/{month}/{day}/[0-9]+/"
        rf"{re.escape(stamp)}-gleif-goldencopy-{re.escape(key)}-golden-copy\.csv\.zip"
    )
    if (
        parsed.scheme != "https" or parsed.hostname != "goldencopy.gleif.org"
        or parsed.port not in (None, 443) or parsed.query or parsed.fragment
        or re.fullmatch(pattern, parsed.path) is None
    ):
        raise GleifBulkError("GLEIF discovery archive URL is not the pinned dated Golden Copy file")
    return value


def _positive_size(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= MAX_MEMBER_BYTES:
        raise GleifBulkError(f"GLEIF discovery has an invalid {label} size")
    return value


def _parse_discovery(raw: bytes) -> dict[str, Any]:
    """Pin one provider-advertised snapshot without parsing payload archives."""
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GleifBulkError("GLEIF publication discovery was not valid JSON") from exc
    if not isinstance(document, dict) or not isinstance(document.get("data"), list) or not document["data"]:
        raise GleifBulkError("GLEIF publication discovery has no snapshot data")
    # The documented discovery response is newest-first.  We deliberately retain
    # this provider selection as evidence instead of manufacturing a local date.
    snapshot = document["data"][0]
    if not isinstance(snapshot, dict):
        raise GleifBulkError("GLEIF publication discovery has an invalid first snapshot")
    publish_date = _safe_publish_date(snapshot.get("publish_date"))
    members: dict[str, dict[str, Any]] = {}
    for key in PRODUCT_MEMBERS:
        product = snapshot.get(key)
        if not isinstance(product, dict) or _safe_publish_date(product.get("publish_date")) != publish_date:
            raise GleifBulkError("GLEIF product members do not share one publish_date")
        csv = product.get("full_file", {}).get("csv") if isinstance(product.get("full_file"), dict) else None
        if not isinstance(csv, dict):
            raise GleifBulkError(f"GLEIF discovery lacks {key} CSV archive metadata")
        if (
            csv.get("type") != key or csv.get("format") != "csv"
            or csv.get("delta_type") != "GoldenCopy"
        ):
            raise GleifBulkError(f"GLEIF discovery has an unexpected {key} CSV descriptor")
        members[key] = {
            "dataset_key": key,
            "url": _safe_url(csv.get("url"), key=key, publish_date=publish_date),
            "expected_size_bytes": _positive_size(csv.get("size"), key),
            "publish_date": publish_date,
            "provider_type": csv["type"],
            "provider_format": csv["format"],
            "delta_type": csv.get("delta_type"),
            "cdf_version": csv.get("cdf_version"),
        }
    return {
        "source_id": SOURCE_ID,
        "discovery_url": DISCOVERY_URL,
        "provider_publish_date": publish_date,
        "members": members,
    }


def _read_cache(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise GleifBulkError(
            "--skip-download now requires a verified gleif-native-cache.json; "
            "run without it to create one"
        ) from exc
    try:
        cache = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GleifBulkError("GLEIF native cache is invalid or interrupted") from exc
    if not isinstance(cache, dict) or cache.get("format_version") != 1:
        raise GleifBulkError("GLEIF native cache has an unsupported format")
    snapshot = cache.get("snapshot")
    if not isinstance(snapshot, dict):
        raise GleifBulkError("GLEIF native cache has no pinned discovery snapshot")
    # Revalidate the metadata through the same strict parser.  A cache can hold
    # a verified subset, but callers may reuse only members it actually records.
    parsed = _parse_discovery(_canonical_json({"data": [
        {
            "publish_date": snapshot.get("provider_publish_date"),
            **{
                key: {
                    "publish_date": item.get("publish_date"),
                    "full_file": {"csv": {
                        "type": item.get("provider_type"),
                        "format": item.get("provider_format"),
                        "url": item.get("url"),
                        "size": item.get("expected_size_bytes"),
                        "delta_type": item.get("delta_type"),
                        "cdf_version": item.get("cdf_version"),
                    }},
                }
                for key, item in (snapshot.get("members") or {}).items()
                if isinstance(item, dict)
            },
        }
    ]}))
    discovery_transport = snapshot.get("discovery_transport")
    _validate_discovery_transport(discovery_transport)
    parsed["discovery_transport"] = dict(discovery_transport)
    records = cache.get("records")
    if not isinstance(records, dict):
        raise GleifBulkError("GLEIF native cache has no verified member records")
    for key, record in records.items():
        if key not in PRODUCT_MEMBERS or not isinstance(record, dict):
            raise GleifBulkError("GLEIF native cache has an invalid member record")
        transport = record.get("transport")
        if not isinstance(transport, dict):
            raise GleifBulkError(f"GLEIF native cache has invalid {key} transport")
        _validate_transport(transport, parsed["members"][key])
    return {"snapshot": parsed, "records": records}


def _write_cache(path: Path, snapshot: dict[str, Any], records: dict[str, Any]) -> None:
    payload = _canonical_json({"format_version": 1, "snapshot": snapshot, "records": records})
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        raise GleifBulkError("could not atomically write GLEIF native cache") from exc
    finally:
        temporary.unlink(missing_ok=True)


def _validate_transport(transport: Mapping[str, Any], member: Mapping[str, Any]) -> None:
    size = transport.get("size_bytes")
    sha256 = transport.get("sha256")
    md5 = transport.get("md5")
    final_url = transport.get("final_url")
    if size != member["expected_size_bytes"]:
        raise GleifBulkError(
            f"GLEIF {member['dataset_key']} bytes do not match provider discovery size"
        )
    request_url = transport.get("request_url")
    if (
        not isinstance(sha256, str) or _SHA256_RE.fullmatch(sha256) is None
        or not isinstance(md5, str) or _MD5_RE.fullmatch(md5) is None
    ):
        raise GleifBulkError(f"GLEIF {member['dataset_key']} transport hashes are invalid")
    if request_url != member["url"] or final_url != member["url"]:
        raise GleifBulkError(f"GLEIF {member['dataset_key']} transport URL is not the pinned native archive")


def _validate_discovery_transport(transport: Any) -> None:
    if not isinstance(transport, Mapping):
        raise GleifBulkError("GLEIF cache has no discovery transport descriptor")
    if (
        transport.get("request_url") != DISCOVERY_URL
        or transport.get("final_url") != DISCOVERY_URL
        or transport.get("status_code") != 200
        or type(transport.get("size_bytes")) is not int
        or transport["size_bytes"] < 1 or transport["size_bytes"] > DISCOVERY_MAX_BYTES
        or not isinstance(transport.get("sha256"), str)
        or _SHA256_RE.fullmatch(transport["sha256"]) is None
        or not isinstance(transport.get("md5"), str)
        or _MD5_RE.fullmatch(transport["md5"]) is None
    ):
        raise GleifBulkError("GLEIF discovery transport descriptor is invalid")


def _verify_cached_local(path: Path, transport: Mapping[str, Any], member: Mapping[str, Any]) -> None:
    if path.is_symlink() or not path.is_file():
        raise GleifBulkError(f"GLEIF cached {member['dataset_key']} file is not a regular file")
    _validate_transport(transport, member)
    sha256, md5 = _hash_file(path)
    if (
        path.stat().st_size != transport["size_bytes"]
        or sha256 != transport["sha256"]
        or md5 != transport["md5"]
    ):
        raise GleifBulkError(
            f"GLEIF cached {member['dataset_key']} bytes do not match its verified cache"
        )


def _verify_cached_discovery(path: Path, transport: Mapping[str, Any]) -> None:
    if path.is_symlink() or not path.is_file():
        raise GleifBulkError("GLEIF cached discovery file is not a regular file")
    _validate_discovery_transport(transport)
    sha256, md5 = _hash_file(path)
    if (
        path.stat().st_size != transport["size_bytes"]
        or sha256 != transport["sha256"]
        or md5 != transport["md5"]
    ):
        raise GleifBulkError("GLEIF cached discovery bytes do not match its verified cache")


def _raw_matches_transport(raw: Mapping[str, Any], transport: Mapping[str, Any]) -> bool:
    return all(raw.get(key) == transport.get(key) for key in ("sha256", "md5", "size_bytes"))


@contextmanager
def _workspace_lock(workspace: Path):
    """Serialize cache/target promotion for one local GLEIF workspace."""
    try:
        import fcntl
    except ImportError as exc:  # pragma: no cover - supported project runners are Linux
        raise GleifBulkError("GLEIF workspace locking requires fcntl on this runner") from exc
    lock_path = workspace / ".gleif-native.lock"
    try:
        handle = lock_path.open("a+b")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, BlockingIOError) as exc:
        try:
            handle.close()
        except UnboundLocalError:
            pass
        raise GleifBulkError("another GLEIF transfer already owns this workspace") from exc
    try:
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _same_snapshot(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """Compare provider snapshot identity while allowing a fresh discovery fetch."""
    return all(left.get(key) == right.get(key) for key in (
        "source_id", "discovery_url", "provider_publish_date", "members"
    ))


def _download_snapshot(workspace: Path) -> dict[str, Any]:
    staged = workspace / f".gleif-publishes.{uuid4().hex}.download"
    try:
        descriptor = fetch_to_file(
            {"url": DISCOVERY_URL}, staged, ALLOWED_HOSTS, DOWNLOAD_TIMEOUT_SECONDS,
            DISCOVERY_MAX_BYTES,
        )
        _validate_discovery_transport(descriptor)
        snapshot = _parse_discovery(staged.read_bytes())
        discovery_path = _discovery_path(workspace, snapshot)
        os.replace(staged, discovery_path)
        snapshot["discovery_transport"] = descriptor
        return snapshot
    except (BulkTransportError, OSError) as exc:
        raise GleifBulkError("GLEIF publication discovery failed") from exc
    finally:
        staged.unlink(missing_ok=True)


def _snapshot_workspace(workspace: Path, snapshot: Mapping[str, Any]) -> Path:
    """Isolate source snapshots without treating provider date text as a path."""
    publish_date = snapshot["provider_publish_date"]
    assert isinstance(publish_date, str)
    token = hashlib.sha256(publish_date.encode("utf-8")).hexdigest()[:16]
    directory = workspace / "snapshots" / token
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _member_path(workspace: Path, snapshot: Mapping[str, Any], key: str) -> Path:
    return _snapshot_workspace(workspace, snapshot) / f"gleif_goldencopy_{key}.csv.zip"


def _discovery_path(workspace: Path, snapshot: Mapping[str, Any]) -> Path:
    return _snapshot_workspace(workspace, snapshot) / "gleif-publishes.json"


def _download_member(workspace: Path, snapshot: Mapping[str, Any], member: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    target = _member_path(workspace, snapshot, member["dataset_key"])
    staged = workspace / f".gleif_goldencopy_{member['dataset_key']}.{uuid4().hex}.download"
    try:
        transport = fetch_to_file(
            {"url": member["url"]}, staged, ALLOWED_HOSTS, DOWNLOAD_TIMEOUT_SECONDS,
            member["expected_size_bytes"],
        )
        _validate_transport(transport, member)
        # Provider inventory validation happens before this atomic replacement,
        # so a self-consistent short response cannot displace a prior good file.
        os.replace(staged, target)
        return target, transport
    except (BulkTransportError, OSError) as exc:
        raise GleifBulkError(f"GLEIF {member['dataset_key']} download failed") from exc
    finally:
        staged.unlink(missing_ok=True)


def _completion_receipt(
    snapshot: Mapping[str, Any], selected: tuple[str, ...], archives: list[dict[str, Any]],
    discovery_native: Mapping[str, Any],
) -> dict[str, Any]:
    complete = selected == PRODUCT_MEMBERS
    return {
        "format_version": 1,
        "kind": "gleif_current_golden_copy_native_transfer",
        "source_id": SOURCE_ID,
        "provider_snapshot": {
            "discovery_url": snapshot["discovery_url"],
            "provider_publish_date": snapshot["provider_publish_date"],
            "evidence": "provider-advertised publish_date; no local date certainty inferred",
            "discovery_transport": snapshot["discovery_transport"],
            "discovery_native": discovery_native,
        },
        "product": {
            "member_order": list(PRODUCT_MEMBERS),
            "selected_members": list(selected),
            "unselected_members": [key for key in PRODUCT_MEMBERS if key not in selected],
            "completion_status": "complete_current_product" if complete else "incomplete_selected_subset",
            "complete_current_product": complete,
            "provider_history_verified": False,
            "scope": "current three-member Golden Copy product only; historical Golden Copy snapshots are not verified",
        },
        "archives": archives,
    }


def _run_gleif_ingestion_locked(
    workspace: Path,
    selected: tuple[str, ...],
    allow_codespace: bool = False,
    skip_download: bool = False,
    skip_upload: bool = False,
    *,
    allow_production_write: bool = False,
) -> dict[str, Any]:
    """Transfer selected native members and optionally store verified immutable evidence.

    ``--skip-download`` is intentionally stricter than the legacy behavior: it
    accepts only the cache written by this adapter after byte/hash verification.
    ``GITHUB_ACTIONS`` alone never enables a write.
    """
    cache_path = workspace / CACHE_NAME

    if skip_download:
        cached = _read_cache(cache_path)
        snapshot = cached["snapshot"]
        records = cached["records"]
        discovery_path = _discovery_path(workspace, snapshot)
        _verify_cached_discovery(discovery_path, snapshot["discovery_transport"])
        if not _same_snapshot(_parse_discovery(discovery_path.read_bytes()), snapshot):
            raise GleifBulkError("GLEIF cached discovery bytes do not match pinned metadata")
        local_records: list[dict[str, Any]] = []
        for key in selected:
            member = snapshot["members"][key]
            record = records.get(key)
            if not isinstance(record, dict) or not isinstance(record.get("transport"), dict):
                raise GleifBulkError(f"GLEIF native cache is missing {key} verification")
            target = _member_path(workspace, snapshot, key)
            transport = record["transport"]
            _verify_cached_local(target, transport, member)
            local_records.append({"dataset_key": key, "target_path": target, "transport": transport, "member": member})
    else:
        snapshot = _download_snapshot(workspace)
        local_records = []
        cache_records: dict[str, Any] = {}
        # A fresh discovery can safely reuse only records pinned to its exact
        # provider-advertised member descriptors.  The new discovery response
        # itself is persisted before member work starts.
        if cache_path.exists():
            try:
                existing = _read_cache(cache_path)
                if _same_snapshot(existing["snapshot"], snapshot):
                    cache_records = dict(existing["records"])
            except GleifBulkError:
                pass
        _write_cache(cache_path, snapshot, cache_records)
        for key in selected:
            member = snapshot["members"][key]
            cached = cache_records.get(key)
            target = _member_path(workspace, snapshot, key)
            if isinstance(cached, dict) and isinstance(cached.get("transport"), dict):
                transport = cached["transport"]
                try:
                    _verify_cached_local(target, transport, member)
                except GleifBulkError:
                    # A bad cache is never reused.  The bounded download stages
                    # separately, preserving the prior local target on failure.
                    target, transport = _download_member(workspace, snapshot, member)
            else:
                target, transport = _download_member(workspace, snapshot, member)
            local_records.append({"dataset_key": key, "target_path": target, "transport": transport, "member": member})
            cache_records[key] = {"transport": transport}
            # A failed later member leaves every earlier member durably eligible
            # for verified reuse on the next run.
            _write_cache(cache_path, snapshot, cache_records)

    write_requested = (allow_codespace or allow_production_write) and not skip_upload
    if not write_requested:
        return {
            "status": "downloaded_locally",
            "provider_publish_date": snapshot["provider_publish_date"],
            "selected_members": list(selected),
            "complete_current_product": selected == PRODUCT_MEMBERS,
            "files": [item["target_path"].name for item in local_records],
            "cache": str(cache_path),
        }

    storage = StorageManager(allow_interactive_auth=False)
    # The adapter flag only authorizes entering its publication path.  The
    # StorageManager remains the authority for the selected root and requires
    # its documented environment opt-in outside Actions.
    storage.authorize_writes()
    raw_store = BulkDriveRawStore(storage, SOURCE_ID)
    campaign_store = DriveCampaignStore(storage, SOURCE_ID)
    discovery_native = raw_store.put_file(
        _discovery_path(workspace, snapshot),
        {
            "source_id": SOURCE_ID,
            "kind": "publication_discovery",
            "transport": snapshot["discovery_transport"],
        },
    )
    if not _raw_matches_transport(discovery_native, snapshot["discovery_transport"]):
        raise DriveIntegrityError("GLEIF immutable discovery bytes differ from its transport descriptor")
    archives: list[dict[str, Any]] = []
    for item in local_records:
        member = item["member"]
        raw = raw_store.put_file(item["target_path"], {
            "source_id": SOURCE_ID,
            "dataset_key": member["dataset_key"],
            "provider_publish_date": member["publish_date"],
            "provider_expected_size_bytes": member["expected_size_bytes"],
            "provider_url": member["url"],
            "transport": item["transport"],
        })
        if not _raw_matches_transport(raw, item["transport"]):
            raise DriveIntegrityError(
                f"GLEIF {member['dataset_key']} immutable bytes differ from transport descriptor"
            )
        archives.append({
            "dataset_key": member["dataset_key"],
            "native": raw,
            "provider_descriptor": member,
            "transport": item["transport"],
        })
        # A per-object put verifies itself.  Reverify the entire candidate set
        # before receipt creation, so no completion can be emitted for a mixed
        # or later-corrupted remote set.
    if raw_store.verify(discovery_native) != discovery_native:
        raise DriveIntegrityError("GLEIF immutable discovery descriptor changed during verification")
    for archive in archives:
        verified = raw_store.verify(archive["native"])
        if verified != archive["native"]:
            raise DriveIntegrityError("GLEIF raw descriptor changed during verification")
    receipt = _completion_receipt(snapshot, selected, archives, discovery_native)
    receipt_descriptor = campaign_store.put_receipt(receipt)
    if campaign_store.read_receipt(receipt_descriptor) != receipt:
        raise CampaignStoreError("GLEIF completion receipt did not read back exactly")

    return {
        "status": "published_native",
        "receipt": receipt_descriptor,
        "completion_status": receipt["product"]["completion_status"],
        "complete_current_product": receipt["product"]["complete_current_product"],
        "provider_publish_date": snapshot["provider_publish_date"],
        "archives": archives,
    }


def run_gleif_ingestion(
    workspace: Path,
    datasets: list[str] | None = None,
    allow_codespace: bool = False,
    skip_download: bool = False,
    skip_upload: bool = False,
    *,
    allow_production_write: bool = False,
) -> dict[str, Any]:
    """Transfer selected native members and optionally store verified immutable evidence.

    ``--skip-download`` is intentionally stricter than the legacy behavior: it
    accepts only the cache written by this adapter after byte/hash verification.
    ``GITHUB_ACTIONS`` alone never enables a write.
    """
    selected = _selected_keys(datasets)  # Must fail before workspace/network I/O.
    if not isinstance(workspace, Path):
        raise GleifBulkError("workspace must be a pathlib.Path")
    workspace.mkdir(parents=True, exist_ok=True)
    with _workspace_lock(workspace):
        return _run_gleif_ingestion_locked(
            workspace, selected, allow_codespace, skip_download, skip_upload,
            allow_production_write=allow_production_write,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="GLEIF Golden Copy native bulk transfer")
    parser.add_argument("--workspace", type=str, default="portal/test-results/gleif")
    parser.add_argument("--datasets", nargs="*", default=None, help="Members: lei2 rr repex")
    parser.add_argument("--allow-codespace", action="store_true", help="Legacy explicit write opt-in")
    parser.add_argument("--allow-production-write", action="store_true", help="Explicit write opt-in")
    parser.add_argument("--skip-download", action="store_true", help="Require verified native cache")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()
    result = run_gleif_ingestion(
        workspace=Path(args.workspace).resolve(), datasets=args.datasets,
        allow_codespace=args.allow_codespace, allow_production_write=args.allow_production_write,
        skip_download=args.skip_download, skip_upload=args.skip_upload,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
