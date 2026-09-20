"""GUS Dziedzinowe Bazy Wiedzy (DBW) Web bulk extractor.

Extracts complete thematic datasets, cross-sections, dictionaries, metadata,
and High-Value Datasets (HVD) directly from the official Statistics Poland
DBW Web portal (https://dbw.stat.gov.pl).

Complies strictly with ADR 0009:
- Landing is native transfer only: original .zip, .csv, and .json bytes are
  stored directly without unpacking archives, parsing data, converting schemas,
  or transforming to Parquet in the ingestion step.
- Resumable checkpoints and technical manifests are preserved in Google Drive
  under 01_landing/gus_dbw/ and 06_control/source_campaigns/gus_dbw/.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from hashlib import md5, sha256
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from googleapiclient.http import MediaFileUpload, MediaInMemoryUpload
from storage_manager import StorageManager

DBW_WEB_BASE = "https://dbw.stat.gov.pl"
TREE_URL = f"{DBW_WEB_BASE}/api_app/wsk/getIndicatorsTree"
AGGREGATES_URL = f"{DBW_WEB_BASE}/api_app/getAggregatesById"
METRYKA_URL = f"{DBW_WEB_BASE}/api_app/GetMetrykaCSV"
BULK_DOWNLOAD_URL = f"{DBW_WEB_BASE}/bulk_new"
HVD_DOWNLOAD_URL = f"{DBW_WEB_BASE}/HVD"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
_CHUNK_BYTES = 8 * 1024 * 1024
COMPLETION_PREFIX = "landing-complete-v1"


def _require_production_context(allow_codespace: bool = False) -> None:
    is_actions = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"
    is_main = os.environ.get("GITHUB_REF") == "refs/heads/main"
    codespace_opt_in = allow_codespace or os.environ.get("ZOHELO_ALLOW_CODESPACE_EXECUTION", "").lower() == "true"
    if is_actions and is_main:
        return
    if codespace_opt_in:
        return
    raise PermissionError(
        "DBW Web extraction must run in serialized main-branch GitHub Actions "
        "or with explicit Codespace/local authorization (ZOHELO_ALLOW_CODESPACE_EXECUTION=true or --allow-codespace)"
    )


def _http_get(url: str, timeout: int = 30, retries: int = 3, backoff: float = 1.0, proxy: str | None = None) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({"http": proxy, "https": proxy})) if proxy else urllib.request.build_opener()
    last_exc = None
    for attempt in range(retries):
        try:
            with opener.open(req, timeout=timeout) as resp:
                if resp.status == 200:
                    return resp.read()
                raise RuntimeError(f"HTTP GET {url} returned status {resp.status}")
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_exc}") from last_exc


def _hash_bytes(data: bytes) -> tuple[str, str]:
    return sha256(data).hexdigest(), md5(data).hexdigest()


def _hash_file(path: Path) -> tuple[str, str]:
    d_sha = sha256()
    d_md5 = md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(_CHUNK_BYTES), b""):
            d_sha.update(chunk)
            d_md5.update(chunk)
    return d_sha.hexdigest(), d_md5.hexdigest()


DRIVE_LOCK = threading.Lock()


def _escape_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _find_exact_file(storage: StorageManager, name: str, parent_id: str) -> list[dict[str, Any]]:
    query = f"name='{_escape_query(name)}' and '{_escape_query(parent_id)}' in parents and trashed=false"
    result: list[dict[str, Any]] = []
    token = None
    while True:
        args: dict[str, Any] = {
            "q": query,
            "spaces": "drive",
            "fields": "nextPageToken, files(id,name,size,md5Checksum,appProperties,trashed)",
        }
        if token:
            args["pageToken"] = token
        with DRIVE_LOCK:
            response = storage.drive_service.files().list(**args).execute(num_retries=4)
        result.extend(response.get("files", []))
        token = response.get("nextPageToken")
        if not token:
            break
    return [item for item in result if item.get("name") == name and item.get("trashed") is not True]


def _versioned_name(name: str, sha256_hex: str) -> str:
    """Keep a provider/logical name recognizable while making revisions immutable."""
    path = Path(name)
    suffix = "".join(path.suffixes)
    stem = name[: -len(suffix)] if suffix else name
    marker = f"--sha256-{sha256_hex}"
    max_stem = max(1, 240 - len(marker) - len(suffix))
    return f"{stem[:max_stem]}{marker}{suffix}"


def _upload_bytes(
    storage: StorageManager,
    data: bytes,
    *,
    name: str,
    parent_id: str,
    kind: str,
    mime_type: str = "application/octet-stream",
    extra_properties: dict[str, str] | None = None,
) -> dict[str, Any]:
    sha256_hex, md5_hex = _hash_bytes(data)
    logical_name = name
    existing = _find_exact_file(storage, logical_name, parent_id)
    if len(existing) > 1:
        raise RuntimeError(f"Ambiguous existing DBW bulk object: {name}")
    if existing:
        item = existing[0]
        props = item.get("appProperties") or {}
        if (
            props.get("sha256") == sha256_hex
            and item.get("md5Checksum") == md5_hex
            and int(item.get("size", -1)) == len(data)
        ):
            return {
                "id": item["id"],
                "name": logical_name,
                "size": len(data),
                "sha256": sha256_hex,
                "md5": md5_hex,
                "reused": True,
            }
        name = _versioned_name(logical_name, sha256_hex)
        existing = _find_exact_file(storage, name, parent_id)
        if len(existing) > 1:
            raise RuntimeError(f"Ambiguous existing DBW revision object: {name}")
        if existing:
            item = existing[0]
            props = item.get("appProperties") or {}
            if (
                props.get("sha256") == sha256_hex
                and item.get("md5Checksum") == md5_hex
                and int(item.get("size", -1)) == len(data)
            ):
                return {
                    "id": item["id"], "name": name, "size": len(data),
                    "sha256": sha256_hex, "md5": md5_hex, "reused": True,
                }
            raise RuntimeError(f"Existing DBW revision object failed verification: {name}")
    properties = {
        "sha256": sha256_hex,
        "kind": kind,
        "source_id": "gus_dbw",
        "transport": "web_bulk",
    }
    properties.update(extra_properties or {})
    metadata = {
        "name": name,
        "parents": [parent_id],
        "appProperties": properties,
    }
    media = MediaInMemoryUpload(data, mimetype=mime_type, resumable=False)
    with DRIVE_LOCK:
        response = (
            storage.drive_service.files()
            .create(body=metadata, media_body=media, fields="id,name,size,md5Checksum,appProperties")
            .execute(num_retries=4)
        )
    if (
        not response
        or response.get("name") != name
        or int(response.get("size", -1)) != len(data)
        or response.get("md5Checksum") != md5_hex
        or (response.get("appProperties") or {}).get("sha256") != sha256_hex
    ):
        raise RuntimeError(f"DBW Drive bytes upload did not verify: {name}")
    return {
        "id": response["id"],
        "name": name,
        "size": len(data),
        "sha256": sha256_hex,
        "md5": md5_hex,
        "reused": False,
    }


def _upload_file(
    storage: StorageManager,
    local_path: Path,
    *,
    name: str,
    parent_id: str,
    kind: str,
    mime_type: str = "application/zip",
) -> dict[str, Any]:
    sha256_hex, md5_hex = _hash_file(local_path)
    file_size = local_path.stat().st_size
    logical_name = name
    existing = _find_exact_file(storage, logical_name, parent_id)
    if len(existing) > 1:
        raise RuntimeError(f"Ambiguous existing DBW bulk object: {name}")
    if existing:
        item = existing[0]
        props = item.get("appProperties") or {}
        if (
            props.get("sha256") == sha256_hex
            and item.get("md5Checksum") == md5_hex
            and int(item.get("size", -1)) == file_size
        ):
            return {
                "id": item["id"],
                "name": logical_name,
                "size": file_size,
                "sha256": sha256_hex,
                "md5": md5_hex,
                "reused": True,
            }
        name = _versioned_name(logical_name, sha256_hex)
        existing = _find_exact_file(storage, name, parent_id)
        if len(existing) > 1:
            raise RuntimeError(f"Ambiguous existing DBW revision object: {name}")
        if existing:
            item = existing[0]
            props = item.get("appProperties") or {}
            if (
                props.get("sha256") == sha256_hex
                and item.get("md5Checksum") == md5_hex
                and int(item.get("size", -1)) == file_size
            ):
                return {
                    "id": item["id"], "name": name, "size": file_size,
                    "sha256": sha256_hex, "md5": md5_hex, "reused": True,
                }
            raise RuntimeError(f"Existing DBW revision object failed verification: {name}")
    metadata = {
        "name": name,
        "parents": [parent_id],
        "appProperties": {
            "sha256": sha256_hex,
            "kind": kind,
            "source_id": "gus_dbw",
            "transport": "web_bulk",
        },
    }
    media = MediaFileUpload(str(local_path), mimetype=mime_type, resumable=True, chunksize=_CHUNK_BYTES)
    with DRIVE_LOCK:
        request = storage.drive_service.files().create(
            body=metadata, media_body=media, fields="id,name,size,md5Checksum,appProperties"
        )
        response = None
        while response is None:
            _, response = request.next_chunk(num_retries=4)
    if (
        not response
        or response.get("name") != name
        or int(response.get("size", -1)) != file_size
        or response.get("md5Checksum") != md5_hex
        or (response.get("appProperties") or {}).get("sha256") != sha256_hex
    ):
        raise RuntimeError(f"DBW Drive file upload did not verify: {name}")
    return {
        "id": response["id"],
        "name": name,
        "size": file_size,
        "sha256": sha256_hex,
        "md5": md5_hex,
        "reused": False,
    }


class DbwWebExtractor:
    """Manages discovery, batch downloading, and Google Drive landing for GUS DBW."""

    def __init__(
        self,
        workspace: Path,
        storage: StorageManager,
        allow_codespace: bool = False,
        proxy: str | None = None,
    ):
        self.workspace = workspace
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.storage = storage
        self.allow_codespace = allow_codespace
        self.proxy = proxy
        self.catalogue_sha256: str | None = None
        _require_production_context(allow_codespace)

        self.session = storage.begin_write_session()
        self.landing_root = storage.resolve_zone("landing", create=False)
        self.control_root = storage.resolve_zone("control", create=False)

        # Build folder hierarchy according to ADR 0009 & DBW contract
        self.dbw_landing = storage.get_or_create_nested_folder(["gus_dbw"], root_id=self.landing_root, write_session=self.session)
        self.native_root = storage.get_or_create_nested_folder(["native"], root_id=self.dbw_landing, write_session=self.session)
        self.taxonomy_dir = storage.get_or_create_nested_folder(["taxonomy"], root_id=self.native_root, write_session=self.session)
        self.metadata_dir = storage.get_or_create_nested_folder(["metadata"], root_id=self.native_root, write_session=self.session)
        self.bulk_dir = storage.get_or_create_nested_folder(["bulk"], root_id=self.native_root, write_session=self.session)
        self.hvd_dir = storage.get_or_create_nested_folder(["hvd"], root_id=self.native_root, write_session=self.session)

        self.control_landing = storage.get_or_create_nested_folder(["_control"], root_id=self.dbw_landing, write_session=self.session)
        self.checkpoints_dir = storage.get_or_create_nested_folder(["checkpoints"], root_id=self.control_landing, write_session=self.session)
        self.campaign_control = storage.get_or_create_nested_folder(["source_campaigns", "gus_dbw"], root_id=self.control_root, write_session=self.session)

    def fetch_indicators_tree(self) -> list[dict[str, Any]]:
        """Fetch the full indicator tree from DBW Web UI and land original JSON bytes."""
        print("Fetching DBW indicators tree from Web UI...")
        tree_bytes = _http_get(TREE_URL, timeout=30, proxy=self.proxy)
        tree_json = json.loads(tree_bytes.decode("utf-8"))

        # Save locally
        local_tree = self.workspace / "indicators_tree.json"
        local_tree.write_bytes(tree_bytes)

        # Land to Drive native/taxonomy/
        res = _upload_bytes(
            self.storage,
            tree_bytes,
            name="indicators_tree.json",
            parent_id=self.taxonomy_dir,
            kind="taxonomy",
            mime_type="application/json",
        )
        print(f"Landed indicators_tree.json ({len(tree_bytes)} bytes, reused={res['reused']})")
        return tree_json

    @staticmethod
    def extract_indicators(tree: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Extract flat list of all indicators with domain breadcrumbs."""
        indicators: dict[int, dict[str, Any]] = {}

        def walk(nodes: list[dict[str, Any]], path: str = ""):
            for n in nodes:
                name = n.get("name", "")
                current_path = f"{path} > {name}" if path else name
                if n.get("type") == "INDICATOR" and "indicator_id" in n:
                    iid = int(n["indicator_id"])
                    if iid not in indicators:
                        indicators[iid] = {
                            "id": iid,
                            "name": name,
                            "name_en": n.get("name_en", ""),
                            "path": current_path,
                            "node_id": n.get("id"),
                            "parent_id": n.get("parrent_id"),
                        }
                if "children" in n and n["children"]:
                    walk(n["children"], current_path)

        walk(tree)
        return [indicators[k] for k in sorted(indicators)]

    def load_completed_checkpoints(self, catalogue_sha256: str) -> set[int]:
        """Read full-bulk receipts bound to the exact current catalogue revision."""
        query = f"'{_escape_query(self.checkpoints_dir)}' in parents and trashed=false"
        completed = set()
        token = None
        while True:
            args: dict[str, Any] = {
                "q": query,
                "spaces": "drive",
                "fields": "nextPageToken, files(id,name,appProperties,trashed)",
            }
            if token:
                args["pageToken"] = token
            with DRIVE_LOCK:
                response = self.storage.drive_service.files().list(**args).execute(num_retries=4)
            for f in response.get("files", []):
                name = f.get("name", "")
                match = re.fullmatch(
                    r"completed-v3-(\d+)(?:--sha256-[0-9a-f]{64})?\.json", name
                )
                props = f.get("appProperties") or {}
                if (
                    match
                    and props.get("checkpoint_schema") == "3"
                    and props.get("checkpoint_status") == "completed"
                    and props.get("bulk_complete") == "true"
                    and props.get("metadata_complete") == "true"
                    and props.get("catalogue_sha256") == catalogue_sha256
                ):
                    completed.add(int(match.group(1)))
            token = response.get("nextPageToken")
            if not token:
                break
        return completed

    def process_indicator(
        self,
        indicator: dict[str, Any],
        skip_bulk_zips: bool = False,
    ) -> dict[str, Any]:
        """Process one indicator: metadata, metryka, and bulk zip packages."""
        if not self.catalogue_sha256:
            raise RuntimeError("DBW indicator processing requires a bound catalogue SHA-256.")
        ind_id = indicator["id"]
        result = {
            "indicator_id": ind_id,
            "name": indicator["name"],
            "files_landed": [],
            "landed_objects": [],
            "new_bytes": 0,
            "status": "pending",
        }

        def retain_object(upload: dict[str, Any], role: str) -> None:
            result["files_landed"].append(upload["name"])
            result["landed_objects"].append({
                "id": upload["id"],
                "name": upload["name"],
                "size": upload["size"],
                "sha256": upload["sha256"],
                "md5": upload["md5"],
                "role": role,
            })

        # 1. Fetch and land aggregates metadata (PL)
        agg_url = f"{AGGREGATES_URL}?id={ind_id}&czy_pl=true"
        agg_bytes = _http_get(agg_url, timeout=20, proxy=self.proxy)
        agg_res = _upload_bytes(
            self.storage,
            agg_bytes,
            name=f"aggregates_{ind_id}_pl.json",
            parent_id=self.metadata_dir,
            kind="metadata",
            mime_type="application/json",
        )
        retain_object(agg_res, "aggregates")
        if not agg_res["reused"]:
            result["new_bytes"] += agg_res["size"]

        # 2. Fetch and land Metryka CSV
        met_url = f"{METRYKA_URL}?id_zmienne={ind_id}"
        met_bytes = _http_get(met_url, timeout=20, proxy=self.proxy)
        met_res = _upload_bytes(
            self.storage,
            met_bytes,
            name=f"metryka_{ind_id}.csv",
            parent_id=self.metadata_dir,
            kind="metadata",
            mime_type="text/csv",
        )
        retain_object(met_res, "metryka")
        if not met_res["reused"]:
            result["new_bytes"] += met_res["size"]

        # 3. Parse bulk zip filenames from aggregates response
        expected_bulk_files: list[str] = []
        if not skip_bulk_zips:
            try:
                agg_data = json.loads(agg_bytes.decode("utf-8"))
                rows = agg_data.get("data", {}).get("table", {}).get("rows", [])
                for row in rows:
                    files_cell = next((c for c in row if isinstance(c, dict) and "files" in c), None)
                    if not files_cell:
                        continue
                    for file_info in files_cell.get("files", []):
                        filename = file_info.get("filename")
                        if not filename or not filename.endswith(".zip"):
                            continue
                        expected_bulk_files.append(filename)
                        zip_url = f"{BULK_DOWNLOAD_URL}/{filename}"
                        local_zip = self.workspace / f"worker_{ind_id}_{filename}"
                        # Download if not present locally
                        if not local_zip.exists() or local_zip.stat().st_size == 0:
                            zip_data = _http_get(zip_url, timeout=120, proxy=self.proxy)
                            local_zip.write_bytes(zip_data)

                        # Upload to Drive
                        zip_res = _upload_file(
                            self.storage,
                            local_zip,
                            name=filename,
                            parent_id=self.bulk_dir,
                            kind="bulk_zip",
                            mime_type="application/zip",
                        )
                        retain_object(zip_res, "bulk_zip")
                        if not zip_res["reused"]:
                            result["new_bytes"] += zip_res["size"]
                        # Clean up temporary local file after verified Drive upload
                        local_zip.unlink(missing_ok=True)
            except Exception as exc:
                print(f"Warning: Bulk zip download for indicator {ind_id} encountered an error: {exc}")
                raise

        # 4. Record either a full-bulk completion receipt or an explicit partial receipt.
        result["status"] = "metadata_only" if skip_bulk_zips else "completed"
        checkpoint_data = {
            "schema_version": 3,
            "record_type": "gus_dbw_indicator_completion",
            "indicator_id": ind_id,
            "name": indicator["name"],
            "status": result["status"],
            "bulk_complete": not skip_bulk_zips,
            "metadata_complete": True,
            "catalogue_sha256": self.catalogue_sha256,
            "expected_bulk_files": sorted(set(expected_bulk_files)),
            "files_landed": result["files_landed"],
            "landed_objects": result["landed_objects"],
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        cp_bytes = json.dumps(checkpoint_data, ensure_ascii=False, indent=2).encode("utf-8")
        checkpoint_name = (
            f"partial-v3-{ind_id}.json" if skip_bulk_zips else f"completed-v3-{ind_id}.json"
        )
        _upload_bytes(
            self.storage,
            cp_bytes,
            name=checkpoint_name,
            parent_id=self.checkpoints_dir,
            kind="checkpoint",
            mime_type="application/json",
            extra_properties={
                "checkpoint_schema": "3",
                "checkpoint_status": result["status"],
                "bulk_complete": str(not skip_bulk_zips).lower(),
                "metadata_complete": "true",
                "indicator_id": str(ind_id),
                "catalogue_sha256": self.catalogue_sha256,
            },
        )
        return result

    def publish_catalogue_completion(
        self,
        *,
        catalogue_indicators: int,
        completed_indicators: int,
        catalogue_sha256: str,
    ) -> dict[str, Any]:
        """Publish the deterministic marker that alone unlocks the Bronze stage."""
        document = {
            "schema_version": 1,
            "record_type": "gus_dbw_landing_completion",
            "source_id": "gus_dbw",
            "status": "complete_current_catalogue",
            "landing_scope": "native_bytes_only",
            "catalogue_indicators": catalogue_indicators,
            "completed_indicators": completed_indicators,
            "pending_indicators": catalogue_indicators - completed_indicators,
            "failed_indicators": 0,
            "bulk_complete": True,
            "metadata_complete": True,
            "catalogue_sha256": catalogue_sha256,
        }
        if catalogue_indicators <= 0 or completed_indicators != catalogue_indicators:
            raise RuntimeError("Cannot publish DBW completion before catalogue exhaustion.")
        raw = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return _upload_bytes(
            self.storage,
            raw,
            name=f"{COMPLETION_PREFIX}-{catalogue_sha256}.json",
            parent_id=self.control_landing,
            kind="completion",
            mime_type="application/json",
            extra_properties={"completion_schema": "1", "completion_status": "complete_current_catalogue"},
        )


def main():
    parser = argparse.ArgumentParser(description="GUS DBW Web bulk extractor")
    parser.add_argument("--workspace", default="portal/test-results/dbw-web-bulk", help="Local workspace directory")
    parser.add_argument("--max-seconds", type=int, default=18600, help="Maximum execution seconds")
    parser.add_argument("--concurrency", type=int, default=1, help="Concurrent workers")
    parser.add_argument("--proxies", type=str, default=None, help="Comma-separated list of proxy URLs (e.g. http://127.0.0.1:8081)")
    parser.add_argument("--max-indicators", type=int, default=None, help="Limit number of indicators to extract")
    parser.add_argument("--indicators", type=str, default=None, help="Comma-separated list of indicator IDs")
    parser.add_argument("--skip-bulk-zips", action="store_true", help="Skip downloading bulk zip observation packages")
    parser.add_argument("--allow-codespace", action="store_true", help="Allow running outside main GitHub Actions")
    parser.add_argument("--summary", type=str, default=None, help="Path to write execution summary JSON")
    args = parser.parse_args()

    start_time = time.time()
    workspace = Path(args.workspace).resolve()
    workspace.mkdir(parents=True, exist_ok=True)

    # Discover proxies from args or cluster.json
    proxies: list[str] = []
    if args.proxies:
        proxies = [p.strip() for p in args.proxies.split(",") if p.strip()]
    else:
        cluster_file = Path("/workspaces/zohelo-data/.wireguard/cluster.json")
        if cluster_file.is_file():
            try:
                cdata = json.loads(cluster_file.read_text(encoding="utf-8"))
                proxies = [
                    f"http://127.0.0.1:{inst['http_port']}"
                    for inst in cdata
                    if inst.get("status") == "HEALTHY"
                ]
            except Exception:
                pass

    if proxies:
        print(f"Loaded {len(proxies)} DBW proxy endpoints: {proxies}")

    print("Initializing Google Drive storage for GUS DBW...")
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)

    primary_proxy = proxies[0] if proxies else None
    extractor = DbwWebExtractor(
        workspace=workspace,
        storage=storage,
        allow_codespace=args.allow_codespace,
        proxy=primary_proxy,
    )

    tree = extractor.fetch_indicators_tree()
    catalogue_sha256, _ = _hash_file(workspace / "indicators_tree.json")
    all_indicators = extractor.extract_indicators(tree)
    catalogue_indicator_ids = {item["id"] for item in all_indicators}
    print(f"Discovered {len(all_indicators)} total indicators across all DBW thematic areas.")

    # Filter indicators if specific IDs requested
    if args.indicators:
        wanted_ids = {int(x.strip()) for x in args.indicators.split(",") if x.strip()}
        all_indicators = [ind for ind in all_indicators if ind["id"] in wanted_ids]
        print(f"Filtered to {len(all_indicators)} specified indicators: {wanted_ids}")

    extractor.catalogue_sha256 = catalogue_sha256
    completed_ids = extractor.load_completed_checkpoints(catalogue_sha256)
    print(f"Found {len(completed_ids)} already completed indicators on Drive.")

    pending_indicators = [ind for ind in all_indicators if ind["id"] not in completed_ids]
    if args.max_indicators:
        pending_indicators = pending_indicators[:args.max_indicators]
    print(f"Scheduled {len(pending_indicators)} indicators for extraction in this run.")

    summary = {
        "source_id": "gus_dbw",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_indicators_known": len(catalogue_indicator_ids),
        "previously_completed": len(completed_ids),
        "scheduled_this_run": len(pending_indicators),
        "completed_this_run": 0,
        "failed_this_run": 0,
        "new_files": 0,
        "new_bytes": 0,
        "status": "running",
    }

    errors = []
    num_workers = max(1, args.concurrency)
    worker_extractors = []
    for w in range(num_workers):
        p = proxies[w % len(proxies)] if proxies else None
        worker_extractors.append(
            DbwWebExtractor(
                workspace=workspace,
                storage=storage,
                allow_codespace=args.allow_codespace,
                proxy=p,
            )
        )
        worker_extractors[-1].catalogue_sha256 = catalogue_sha256

    lock = threading.Lock()
    stop_event = threading.Event()
    consecutive_errors = 0

    def process_item(item_and_index):
        nonlocal consecutive_errors
        idx, ind = item_and_index
        if stop_event.is_set():
            return
        elapsed = time.time() - start_time
        if elapsed > args.max_seconds:
            with lock:
                if not stop_event.is_set():
                    print(f"Time budget reached ({elapsed:.1f}s > {args.max_seconds}s). Stopping run.")
                    stop_event.set()
            return

        worker_id = idx % num_workers
        ext = worker_extractors[worker_id]
        try:
            res = ext.process_indicator(ind, skip_bulk_zips=args.skip_bulk_zips)
            with lock:
                if res["status"] == "completed":
                    summary["completed_this_run"] += 1
                summary["new_files"] += len(res["files_landed"])
                summary["new_bytes"] += res["new_bytes"]
                consecutive_errors = 0
                proxy_label = f" [Proxy: {proxies[worker_id % len(proxies)]}]" if proxies else ""
                print(
                    f"[{summary['completed_this_run']}/{len(pending_indicators)}] (Worker {worker_id}{proxy_label}) "
                    f"Indicator {ind['id']} ({ind['name'][:30]}): "
                    f"{len(res['files_landed'])} files landed, {res['new_bytes']:,} new bytes."
                )
        except Exception as exc:
            with lock:
                summary["failed_this_run"] += 1
                consecutive_errors += 1
                errors.append({"indicator_id": ind["id"], "error": str(exc)})
                print(f"Error processing indicator {ind['id']} (Worker {worker_id}): {exc}", file=sys.stderr)
                if consecutive_errors >= 10:
                    print(f"Encountered {consecutive_errors} consecutive failures. Pausing run.")
                    stop_event.set()

    if num_workers > 1 and len(pending_indicators) > 1:
        print(f"Launching {num_workers} concurrent DBW extraction workers...")
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = [pool.submit(process_item, (i, ind)) for i, ind in enumerate(pending_indicators)]
            for f in as_completed(futures):
                if stop_event.is_set():
                    break
    else:
        for i, ind in enumerate(pending_indicators):
            if stop_event.is_set():
                break
            process_item((i, ind))

    verified_completed_ids = extractor.load_completed_checkpoints(catalogue_sha256)
    catalogue_ids = catalogue_indicator_ids
    catalogue_complete = (
        not args.skip_bulk_zips
        and not errors
        and bool(catalogue_ids)
        and catalogue_ids <= verified_completed_ids
    )
    if catalogue_complete:
        extractor.publish_catalogue_completion(
            catalogue_indicators=len(catalogue_ids),
            completed_indicators=len(catalogue_ids),
            catalogue_sha256=catalogue_sha256,
        )

    summary["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    summary["elapsed_seconds"] = round(time.time() - start_time, 2)
    summary["errors"] = errors
    summary["catalogue_complete"] = catalogue_complete
    summary["verified_completed_total"] = len(catalogue_ids & verified_completed_ids)
    summary["status"] = "complete_current_catalogue" if catalogue_complete else "incomplete"

    print("\n--- DBW Extraction Summary ---")
    print(f"Indicators completed this run: {summary['completed_this_run']}")
    print(f"New files landed: {summary['new_files']}")
    print(f"New bytes landed: {summary['new_bytes']:,}")
    print(f"Errors encountered: {summary['failed_this_run']}")
    print(f"Elapsed time: {summary['elapsed_seconds']}s")

    # Write summary
    summary_path = Path(args.summary) if args.summary else workspace / "dbw-extraction-summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    main()
