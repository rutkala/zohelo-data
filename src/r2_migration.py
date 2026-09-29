"""Byte-preserving, root-scoped Drive -> R2 migration. No Drive writes or deletes.

SDK imports live in adapters; the transfer/manifest contract is tested offline.
A completed copy is NOT an Iceberg publication or a production portal cutover.
"""
from __future__ import annotations

import base64
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import math
import re
import threading
import time
from urllib.parse import quote, urlsplit

FOLDER = "application/vnd.google-apps.folder"
SHORTCUT = "application/vnd.google-apps.shortcut"
ROOT_ID = "1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf"
CONTROL = "06_control/drive_to_r2"
FIELDS = ("id,name,mimeType,size,parents,createdTime,modifiedTime,version,"
          "headRevisionId,md5Checksum,sha256Checksum,appProperties,trashed,"
          "shortcutDetails(targetId,targetMimeType),capabilities(canDownload)")
PART_BYTES = 16 * 1024 * 1024


class MigrationError(RuntimeError):
    pass


class TimeBoundReached(MigrationError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def component(value):
    if not isinstance(value, str) or not value:
        raise MigrationError("empty_name")
    encoded = quote(value, safe="-_.~")
    return encoded if encoded not in (".", "..") else encoded.replace(".", "%2E")


def is_missing(exc):
    response = getattr(exc, "response", {})
    return str(response.get("Error", {}).get("Code", "")) in ("404", "NoSuchKey", "NotFound")


def head_or_none(s3, bucket, key):
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if is_missing(exc):
            return None
        raise


def snapshot(source, root_id=ROOT_ID, progress=lambda **kw: None):
    """Exhaust every page and every descendant. Never walk shortcut targets.

    Duplicate names are retained under distinct deterministic keys. No account-wide
    search and no entry/request safety cap that could silently truncate a tree.
    """
    if root_id != ROOT_ID:
        raise MigrationError("unapproved_drive_root")
    root = source.metadata(root_id)
    if root.get("mimeType") != FOLDER or root.get("trashed"):
        raise MigrationError("root_not_accessible")
    root = dict(root, path="", key_path="", parent_id="root")
    nodes = {root_id: root}
    queue = deque([root_id])
    while queue:
        parents = [queue.popleft() for _ in range(min(40, len(queue)))]
        children = source.children(parents)
        groups = defaultdict(list)
        local_ids = set()
        for raw in children:
            if raw.get("trashed") or not raw.get("id") or raw["id"] in local_ids:
                raise MigrationError("invalid_or_duplicate_listing")
            local_ids.add(raw["id"])
            candidates = set(raw.get("parents", ())) & set(parents)
            if len(candidates) != 1:
                raise MigrationError("ambiguous_parent")
            groups[next(iter(candidates))].append(raw)
        for parent_id in parents:
            siblings = groups[parent_id]
            names = Counter(x["name"] for x in siblings)
            for raw in sorted(siblings, key=lambda x: x["id"]):
                identity = raw["id"]
                if identity in nodes:
                    raise MigrationError("source_moved_or_cycle")
                name = component(raw["name"])
                if names[raw["name"]] > 1:
                    name += "~drive-" + component(identity)
                parent = nodes[parent_id]
                node = dict(raw, parent_id=parent_id,
                            path="/".join(x for x in (parent["path"], raw["name"]) if x),
                            key_path="/".join(x for x in (parent["key_path"], name) if x))
                nodes[identity] = node
                if raw.get("mimeType") == FOLDER:
                    queue.append(identity)
        progress(discovered_entries=len(nodes), pending_folders=len(queue))
    ordered = [nodes[k] for k in sorted(nodes)]
    return {"format_version": 1, "root_id": root_id, "inventory_sha256": digest(ordered), "nodes": ordered}


def source_hashes(node):
    md5 = node.get("md5Checksum")
    declared = (node.get("appProperties") or {}).get("sha256")
    sha = node.get("sha256Checksum") or declared
    if declared and node.get("sha256Checksum") and declared != node["sha256Checksum"]:
        raise MigrationError("source_checksum_metadata_disagrees")
    if md5 is not None and not re.fullmatch(r"[0-9a-f]{32}", md5):
        raise MigrationError("invalid_source_md5")
    if sha is not None and not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise MigrationError("invalid_source_sha256")
    if not md5 and not sha:
        raise MigrationError("source_has_no_checksum")
    size = int(node.get("size", -1))
    if size < 0 or size > 5 * 1024**4:
        raise MigrationError("unsupported_object_size")
    return size, md5, sha


def transfer_identity(node):
    size, md5, sha = source_hashes(node)
    return {"source-drive-id": node["id"], "source-md5": md5 or "",
            "source-sha256": sha or "", "source-size": str(size)}


def matches(head, node):
    if not head:
        return False
    meta = head.get("Metadata") or {}
    expected = transfer_identity(node)
    return int(head.get("ContentLength", -1)) == int(expected["source-size"]) and all(
        meta.get(k, "") == v for k, v in expected.items())


def target(s3, node, landing, lakehouse):
    """Use original paths; never overwrite a different retained version."""
    bucket = landing if node["key_path"].split("/", 1)[0] == "01_landing" else lakehouse
    key = node["key_path"]
    # Reuse the already-verified DBW pilot objects rather than making a duplicate.
    if re.fullmatch(r"02_bronze/gus_dbw/observations/part_\d+\.parquet", key):
        legacy = ("02_bronze/gus_dbw/retained/"
                  "15f587a7d0631befac394e6cc1d0183fb0f564801f144b7d6ca340e8d092a12e/observations/"
                  + key.rsplit("/", 1)[-1])
        head = head_or_none(s3, bucket, legacy)
        if head:
            meta = head.get("Metadata") or {}
            size, md5, sha = source_hashes(node)
            if (int(head.get("ContentLength", -1)) == size and sha and md5 and
                    meta.get("source-drive-id") == node["id"] and
                    meta.get("source-sha256") == sha and meta.get("source-md5") == md5 and
                    head.get("ETag", "").strip('"') == md5):
                return bucket, legacy, head
    head = head_or_none(s3, bucket, key) if len(key.encode()) <= 1024 else {"collision": True}
    if head and not matches(head, node):
        key = "_drive_versions/" + component(node["id"]) + "/" + digest(transfer_identity(node))
        head = head_or_none(s3, bucket, key)
        if head and not matches(head, node):
            raise MigrationError("immutable_destination_conflict")
    return bucket, key, head


def blocks(chunks, size):
    buffer = bytearray()
    for chunk in chunks:
        if not isinstance(chunk, bytes):
            raise MigrationError("invalid_download_chunk")
        buffer.extend(chunk)
        while len(buffer) >= size:
            yield bytes(buffer[:size])
            del buffer[:size]
    if buffer:
        yield bytes(buffer)


def copy_object(source, s3, node, landing, lakehouse, *, deadline=float("inf"), part_bytes=PART_BYTES):
    """Stream one object with bounded memory. Complete only after source checksum proof."""
    size, expected_md5, expected_sha = source_hashes(node)
    bucket, key, existing = target(s3, node, landing, lakehouse)
    if existing:
        return {"id": node["id"], "bucket": bucket, "key": key, "size": size,
                "etag": existing["ETag"], "sha256": expected_sha, "reused": True}
    # Never more than 10,000 multipart chunks; all but the last are equal-sized.
    if size > part_bytes * 9999:
        part_bytes = math.ceil(size / 9999 / (1024**2)) * 1024**2
    md5 = hashlib.md5()
    sha = hashlib.sha256()
    observed = 0
    parts, part_digests = [], []
    upload_id = None
    response = None
    options = {"Bucket": bucket, "Key": key, "Metadata": transfer_identity(node),
               "ContentType": node.get("mimeType", "application/octet-stream")}
    try:
        if size > part_bytes:
            upload_id = s3.create_multipart_upload(**options)["UploadId"]
        data = b""
        for data in blocks(source.chunks(node), part_bytes):
            if time.monotonic() >= deadline:
                raise TimeBoundReached("transfer_time_bound")
            observed += len(data)
            if observed > size:
                raise MigrationError("download_exceeds_pinned_size")
            md5.update(data)
            sha.update(data)
            if upload_id:
                part_digest = hashlib.md5(data).digest()
                part = s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id,
                                      PartNumber=len(parts) + 1, Body=data,
                                      ContentMD5=base64.b64encode(part_digest).decode())
                if part["ETag"].strip('"').lower() != part_digest.hex():
                    raise MigrationError("uploaded_part_etag_mismatch")
                parts.append({"PartNumber": len(parts) + 1, "ETag": part["ETag"]})
                part_digests.append(part_digest)
        if (observed != size or (expected_md5 and md5.hexdigest() != expected_md5) or
                (expected_sha and sha.hexdigest() != expected_sha)):
            raise MigrationError("download_checksum_mismatch")
        if upload_id:
            response = s3.complete_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id,
                                                     MultipartUpload={"Parts": parts}, IfNoneMatch="*")
            expected_etag = hashlib.md5(b"".join(part_digests)).hexdigest() + "-" + str(len(parts))
        else:
            response = s3.put_object(**options, Body=data, ContentLength=size,
                                     ContentMD5=base64.b64encode(md5.digest()).decode(), IfNoneMatch="*")
            expected_etag = md5.hexdigest()
        if response.get("ETag", "").strip('"').lower() != expected_etag:
            raise MigrationError("completed_etag_mismatch")
        head = head_or_none(s3, bucket, key)
        if not matches(head, node) or head["ETag"].strip('"').lower() != expected_etag:
            raise MigrationError("destination_readback_mismatch")
        return {"id": node["id"], "bucket": bucket, "key": key, "size": size,
                "etag": head["ETag"], "sha256": sha.hexdigest(), "reused": False}
    finally:
        if upload_id and response is None:
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)


def put_json(s3, bucket, key, value):
    raw = canonical(value)
    s3.put_object(Bucket=bucket, Key=key, Body=raw, ContentType="application/json",
                  ContentMD5=base64.b64encode(hashlib.md5(raw).digest()).decode())


def inventory_summary(inventory):
    totals = defaultdict(lambda: {"files": 0, "bytes": 0, "folders": 0, "shortcuts": 0})
    for node in inventory["nodes"]:
        if not node["path"]:
            continue
        zone = node["key_path"].split("/", 1)[0]
        row = totals[zone]
        if node["mimeType"] == FOLDER:
            row["folders"] += 1
        elif node["mimeType"] == SHORTCUT:
            row["shortcuts"] += 1
        else:
            row["files"] += 1
            row["bytes"] += int(node.get("size", 0))
    return dict(totals)


def migrate(source_factory, s3_factory, landing, lakehouse, *, workers=4,
            seconds=18000, max_bytes=400 * 1024**3, progress=lambda value: None):
    """Run all layers. A stable, complete copy gets an index, not automatic cutover."""
    if workers not in range(1, 9) or landing == lakehouse:
        raise MigrationError("invalid_migration_configuration")
    deadline = time.monotonic() + seconds
    source, s3 = source_factory(), s3_factory()
    before = snapshot(source, progress=lambda **kw: progress({"stage": "inventory", **kw}))
    campaign = before["inventory_sha256"]
    prefix = f"{CONTROL}/snapshots/{campaign}"
    put_json(s3, lakehouse, prefix + "/source-inventory.json", before)
    totals = inventory_summary(before)
    receipt = {"format_version": 1, "inventory_sha256": campaign, "stage": "copy",
               "result": "running", "layers": totals, "copied_files": 0, "reused_files": 0,
               "verified_bytes": 0, "drive_writes": False, "portal_cutover": False,
               "iceberg_registration": "separate_pending_stage"}
    if sum(x["bytes"] for x in totals.values()) > max_bytes:
        raise MigrationError("inventory_exceeds_explicit_storage_budget")
    nodes = {x["id"]: x for x in before["nodes"]}
    payloads = [x for x in before["nodes"] if x["mimeType"] not in (FOLDER, SHORTCUT)]
    # Release/control records first; every layer is still included.
    payloads.sort(key=lambda x: (x["key_path"].split("/", 1)[0] == "01_landing", int(x.get("size", 0))))
    local = threading.local()
    mappings, errors = {}, []

    def copy(node):
        if time.monotonic() >= deadline:
            raise TimeBoundReached("migration_time_bound")
        if node["mimeType"].startswith("application/vnd.google-apps."):
            raise MigrationError("google_native_document_requires_export")
        if not hasattr(local, "source"):
            local.source, local.s3 = source_factory(), s3_factory()
        for attempt in range(3):
            try:
                return copy_object(local.source, local.s3, node, landing, lakehouse, deadline=deadline)
            except MigrationError:
                raise
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for offset in range(0, len(payloads), 100):
            if time.monotonic() >= deadline:
                break
            futures = {pool.submit(copy, node): node for node in payloads[offset:offset + 100]}
            for future in as_completed(futures):
                node = futures[future]
                try:
                    value = future.result()
                    mappings[node["id"]] = value
                    receipt["reused_files" if value["reused"] else "copied_files"] += 1
                    receipt["verified_bytes"] += value["size"]
                except Exception as exc:
                    errors.append({"id": node["id"], "category": type(exc).__name__,
                                   "code": str(exc) if isinstance(exc, MigrationError) else "provider_error"})
            receipt.update(verified_files=len(mappings), total_files=len(payloads), errors=len(errors))
            put_json(s3, lakehouse, prefix + "/progress.json", receipt)
            progress(dict(receipt))
    # Preserve shortcut meaning but do not follow links outside the authorized root.
    for node in nodes.values():
        if node["mimeType"] == SHORTCUT:
            target_id = node.get("shortcutDetails", {}).get("targetId")
            if target_id not in nodes:
                errors.append({"id": node["id"], "code": "shortcut_target_outside_inventory"})
    put_json(s3, lakehouse, prefix + "/copy-map.json", {"objects": mappings, "errors": errors})
    receipt["errors"] = len(errors)
    if errors or len(mappings) != len(payloads):
        receipt["result"] = "incomplete"
    else:
        receipt["stage"] = "source_reconciliation"
        progress(dict(receipt))
        after = snapshot(source)
        put_json(s3, lakehouse, prefix + "/after-inventory.json", after)
        if before["inventory_sha256"] != after["inventory_sha256"]:
            receipt.update(result="source_changed", next_step="repeat_delta_pass")
        else:
            # Snapshot metadata binds legacy IDs to R2, including empty folders.
            children = defaultdict(list)
            for node in nodes.values():
                entry = dict(node, r2=mappings.get(node["id"]))
                put_json(s3, lakehouse, prefix + "/entries/" + component(node["id"]) + ".json", entry)
                children[node["parent_id"]].append(entry)
                if node["mimeType"] == FOLDER:
                    children.setdefault(node["id"], [])
            for parent, entries in children.items():
                entries.sort(key=lambda x: (x["name"], x["id"]))
                put_json(s3, lakehouse, prefix + "/folders/" + component(parent) + ".json", {"files": entries})
            receipt.update(result="copy_verified", stage="complete", index_prefix=prefix,
                           immutable_reference_retained=True)
            put_json(s3, lakehouse, prefix + "/verified.json", receipt)
            # Candidate only: separate acceptance/cutover must publish current.json.
            put_json(s3, lakehouse, CONTROL + "/candidate.json", receipt)
    put_json(s3, lakehouse, prefix + "/progress.json", receipt)
    progress(dict(receipt))
    return receipt


class DriveSource:
    """Only files.get/files.list/media GET on the already authorized project root."""
    def __init__(self):
        from storage_manager import StorageManager
        from google.auth.transport.requests import AuthorizedSession
        self.storage = StorageManager(allow_interactive_auth=False, root_id=ROOT_ID)
        self.api = self.storage.drive_service.files()
        self.session = AuthorizedSession(self.storage.drive_service._http.credentials)

    def metadata(self, file_id):
        return self.api.get(fileId=file_id, fields=FIELDS).execute(num_retries=4)

    def children(self, parents):
        if any(not re.fullmatch(r"[A-Za-z0-9_-]+", x) for x in parents):
            raise MigrationError("invalid_parent_id")
        query = "(" + " or ".join("'%s' in parents" % x for x in parents) + ") and trashed=false"
        token, seen, files = None, set(), []
        while True:
            page = self.api.list(q=query, spaces="drive", pageSize=1000, pageToken=token,
                                 fields="nextPageToken,incompleteSearch,files(" + FIELDS + ")").execute(num_retries=4)
            if page.get("incompleteSearch") or not isinstance(page.get("files"), list):
                raise MigrationError("incomplete_drive_listing")
            files.extend(page["files"])
            token = page.get("nextPageToken")
            if not token:
                return files
            if token in seen:
                raise MigrationError("repeated_page_token")
            seen.add(token)

    def chunks(self, node):
        request = self.api.get_media(fileId=node["id"])
        with self.session.get(request.uri, stream=True, timeout=(15, 120),
                              headers={"Accept-Encoding": "identity"}) as response:
            response.raise_for_status()
            if response.status_code != 200:
                raise MigrationError("unexpected_media_status")
            yield from response.raw.stream(PART_BYTES, decode_content=False)


def make_s3(endpoint, access_key, secret_key):
    import boto3
    from botocore.config import Config
    url = urlsplit(endpoint)
    if (url.scheme != "https" or not re.fullmatch(r"[0-9a-f]{32}\.r2\.cloudflarestorage\.com", url.netloc)
            or url.path not in ("", "/") or url.query or url.fragment):
        raise MigrationError("invalid_r2_endpoint")
    return boto3.client("s3", endpoint_url=endpoint.rstrip("/"), region_name="auto",
                        aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                        config=Config(signature_version="s3v4", connect_timeout=15, read_timeout=120,
                                      retries={"max_attempts": 5, "mode": "standard"}))
