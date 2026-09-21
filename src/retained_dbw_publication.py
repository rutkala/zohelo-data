"""Publish audited, retained DBW Parquet as a bounded query snapshot.

This is a downstream transport operation over already-produced Bronze files.  It
does not call DBW, infer native lineage, or claim current source completeness.
"""
from __future__ import annotations

from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable
from uuid import uuid4

import duckdb

from ingestion.source_campaign_store import (
    MAX_LANDING_FILE_BYTES,
    MAX_LANDING_MANIFEST_BYTES,
    CampaignStoreError,
)


SOURCE_ID = "gus_dbw_retained_bronze"
KIND = "retained_bronze_snapshot"
MAX_INDEX_BYTES = MAX_LANDING_FILE_BYTES
SHA_RE = re.compile(r"^[0-9a-f]{64}$")

EXPECTED_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "observations": [
        ("indicator_id", "BIGINT"), ("przekroj_id", "BIGINT"),
        *[(name, "BIGINT") for i in range(1, 10) for name in (f"wymiar_{i}", f"pozycja_{i}")],
        ("okres_id", "INTEGER"), ("sposob_prezentacji_miara_id", "INTEGER"),
        ("period_year", "INTEGER"), ("wartosc_raw", "VARCHAR"),
        ("wartosc_numeric", "DOUBLE"), ("precyzja", "INTEGER"),
        ("brak_wartosci_id", "INTEGER"), ("tajnosci_id", "INTEGER"),
        ("flaga_id", "INTEGER"), ("raw_archive_file", "VARCHAR"),
        ("source_row_number", "BIGINT"), ("processed_at_utc", "VARCHAR"),
    ],
    "dictionaries": [
        ("indicator_id", "BIGINT"), ("column_name", "VARCHAR"),
        ("dictionary_name", "VARCHAR"), ("element_id", "BIGINT"),
        ("element_name", "VARCHAR"), ("processed_at_utc", "VARCHAR"),
    ],
    "metadata": [
        ("indicator_id", "BIGINT"), ("metric_name", "VARCHAR"),
        ("metric_name_en", "VARCHAR"), ("description", "VARCHAR"),
        ("frequency", "VARCHAR"), ("measure_unit", "VARCHAR"),
        ("data_source", "VARCHAR"), ("legal_basis", "VARCHAR"),
        ("last_update", "VARCHAR"), ("processed_at_utc", "VARCHAR"),
    ],
    "taxonomy": [
        ("indicator_id", "BIGINT"), ("indicator_name", "VARCHAR"),
        ("indicator_name_en", "VARCHAR"), ("thematic_area", "VARCHAR"),
        ("domain", "VARCHAR"), ("taxonomy_path", "VARCHAR"),
        ("node_id", "VARCHAR"), ("parent_id", "VARCHAR"),
        ("processed_at_utc", "VARCHAR"),
    ],
}


class RetainedBronzePublicationError(CampaignStoreError):
    """The retained audit or publication candidate is not trustworthy."""


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RetainedBronzePublicationError(f"{label} is not readable JSON") from exc
    if not isinstance(value, dict):
        raise RetainedBronzePublicationError(f"{label} must be an object")
    return value


def validate_audit(audit_dir: Path) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Authenticate the completed audit and its exact pinned inventory."""
    report_path = audit_dir / "audit-report.json"
    inventory_path = audit_dir / "descriptor-inventory.json"
    report = _load_json(report_path, "retained audit report")
    inventory = _load_json(inventory_path, "retained descriptor inventory")
    run_status = _load_json(audit_dir / "run-status.json", "retained audit run status")
    audit_sha = file_sha(report_path)
    if (
        report.get("format_version") != 1
        or report.get("source_id") != "gus_dbw"
        or report.get("status") != "retained_outputs_audited"
        or report.get("remote_inventory_stable_after_restore") is not True
        or run_status.get("status") != "complete"
        or run_status.get("run_id") != report.get("run_id")
        or run_status.get("inventory_sha256") != report.get("inventory_sha256")
        or inventory.get("format_version") != 1
        or inventory.get("source_id") != "gus_dbw"
        or inventory.get("scope") != "retained_pre_release_bronze_diagnostic"
        or report.get("inventory_sha256") != inventory.get("inventory_sha256")
        or not SHA_RE.fullmatch(str(inventory.get("inventory_sha256", "")))
    ):
        raise RetainedBronzePublicationError("retained audit identity or completion is invalid")
    objects = inventory.get("objects")
    if not isinstance(objects, list) or len(objects) != 3103 or inventory.get("object_count") != len(objects):
        raise RetainedBronzePublicationError("retained descriptor inventory is incomplete")
    canonical_items = canonical(objects)
    if hashlib.sha256(canonical_items).hexdigest() != inventory["inventory_sha256"]:
        raise RetainedBronzePublicationError("retained descriptor inventory digest changed")
    seen: set[str] = set()
    total = 0
    for item in objects:
        if not isinstance(item, dict) or set(item) != {"path", "id", "name", "size", "sha256", "md5"}:
            raise RetainedBronzePublicationError("retained descriptor has invalid fields")
        path = item.get("path")
        if (not isinstance(path, str) or not path or Path(path).is_absolute() or path in seen or
            ".." in Path(path).parts or not isinstance(item.get("id"), str) or not item["id"] or
            not isinstance(item.get("name"), str) or not item["name"] or
            type(item.get("size")) is not int or item["size"] <= 0 or
            not SHA_RE.fullmatch(str(item.get("sha256", ""))) or
            re.fullmatch(r"[0-9a-f]{32}", str(item.get("md5", ""))) is None):
            raise RetainedBronzePublicationError("retained descriptor path is unsafe or duplicated")
        seen.add(path)
        total += item["size"]
    if total != inventory.get("total_bytes"):
        raise RetainedBronzePublicationError("retained inventory byte total changed")
    return report, inventory, audit_sha


def _schema(path: Path) -> list[tuple[str, str]]:
    con = duckdb.connect()
    try:
        return [(row[0], row[1]) for row in con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()]
    finally:
        con.close()


def _rows(path: Path) -> int:
    con = duckdb.connect()
    try:
        return int(con.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
    finally:
        con.close()


def _write_slice(source: Path, output: Path, offset: int, count: int) -> None:
    con = duckdb.connect()
    try:
        scratch = output.parent / f"duckdb-spill-{uuid4()}"
        scratch.mkdir()
        con.execute("SET memory_limit='256MB'"); con.execute("SET threads=1")
        con.execute("SET temp_directory=?", [str(scratch)])
        con.execute("SET max_temp_directory_size='64GB'")
        con.execute(
            "COPY (SELECT * FROM read_parquet(?) LIMIT ? OFFSET ?) TO ? "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)",
            [str(source), count, offset, str(output)],
        )
    finally:
        con.close()


def _quoted(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _verify_repack(source: Path, fragments: list[Path], expected_rows: int) -> None:
    con = duckdb.connect()
    try:
        scratch = fragments[0].parent / f"duckdb-spill-{uuid4()}"
        scratch.mkdir()
        con.execute("SET memory_limit='256MB'")
        con.execute("SET threads=1")
        con.execute("SET temp_directory=?", [str(scratch)])
        con.execute("SET max_temp_directory_size='64GB'")
        source_sql = f"read_parquet({_quoted(source)})"
        fragment_sql = "read_parquet([" + ",".join(_quoted(path) for path in fragments) + "])"
        rows = int(con.execute(f"SELECT count(*) FROM {fragment_sql}").fetchone()[0])
        if rows != expected_rows:
            raise RetainedBronzePublicationError("repacked fragments changed row count")
        # EXCEPT ALL in both directions preserves duplicate and null multiplicity.
        changed = con.execute(
            f"SELECT EXISTS((SELECT * FROM {source_sql} EXCEPT ALL SELECT * FROM {fragment_sql}) "
            f"UNION ALL (SELECT * FROM {fragment_sql} EXCEPT ALL SELECT * FROM {source_sql}))"
        ).fetchone()[0]
        if changed:
            raise RetainedBronzePublicationError("repacked fragments changed retained values")
    finally:
        con.close()


def bounded_fragments(source: Path, schema: list[tuple[str, str]], work: Path) -> Iterable[tuple[Path, int]]:
    """Yield value-preserving Parquet slices no larger than the shared 8 MiB cap."""
    if _schema(source) != schema:
        raise RetainedBronzePublicationError(f"unexpected retained schema: {source.name}")
    rows = _rows(source)
    if rows <= 0:
        raise RetainedBronzePublicationError(f"retained Parquet is empty: {source.name}")
    if source.stat().st_size <= MAX_LANDING_FILE_BYTES:
        yield source, rows
        return
    output_dir = work / f"split-{uuid4()}"
    output_dir.mkdir()
    con = duckdb.connect()
    try:
        scratch = work / f"duckdb-spill-{uuid4()}"
        scratch.mkdir()
        con.execute("SET memory_limit='256MB'")
        con.execute("SET threads=1")
        con.execute("SET temp_directory=?", [str(scratch)])
        con.execute("SET max_temp_directory_size='64GB'")
        con.execute("SET preserve_insertion_order=true")
        con.execute(
            f"COPY (SELECT * FROM read_parquet({_quoted(source)})) TO {_quoted(output_dir)} "
            "(FORMAT PARQUET, PER_THREAD_OUTPUT true, FILE_SIZE_BYTES '4MB', "
            "ROW_GROUP_SIZE 10000, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    fragments = sorted(output_dir.glob("*.parquet"))
    if not fragments:
        raise RetainedBronzePublicationError("repacking produced no fragments")
    normalized: list[Path] = []
    for fragment in fragments:
        if fragment.stat().st_size <= MAX_LANDING_FILE_BYTES:
            normalized.append(fragment)
            continue
        fragment_rows = _rows(fragment)
        offset, estimate = 0, max(1, fragment_rows // 2)
        while offset < fragment_rows:
            count = min(estimate, fragment_rows - offset)
            while True:
                smaller = work / f"slice-{uuid4()}.parquet"
                _write_slice(fragment, smaller, offset, count)
                if smaller.stat().st_size <= MAX_LANDING_FILE_BYTES: break
                smaller.unlink(); count //= 2
                if count < 1: raise RetainedBronzePublicationError("one retained row exceeds 8 MiB")
            normalized.append(smaller); offset += count
    for fragment in normalized:
        if _schema(fragment) != schema:
            raise RetainedBronzePublicationError("repacked fragment changed schema")
    _verify_repack(source, normalized, rows)
    for fragment in normalized:
        yield fragment, _rows(fragment)


def _descriptor_by_path(inventory: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["path"]: item for item in inventory["objects"]}


def _verified_input(audit_dir: Path, descriptor: dict[str, Any]) -> Path:
    cache = (audit_dir / "verified-cache").resolve()
    path = (cache / descriptor["path"]).resolve()
    if path != cache and cache not in path.parents:
        raise RetainedBronzePublicationError("verified cache path escapes the audit directory")
    if path.is_symlink() or not path.is_file() or path.stat().st_size != descriptor["size"]:
        raise RetainedBronzePublicationError(f"verified cache identity changed: {descriptor['path']}")
    if file_sha(path) != descriptor["sha256"]:
        raise RetainedBronzePublicationError(f"verified cache digest changed: {descriptor['path']}")
    return path


def _publication_descriptor(value: Any, *, part: bool = False) -> dict[str, Any]:
    fields = {"id", "name", "size", "sha256", "row_count"} | ({"part"} if part else set())
    if not isinstance(value, dict) or set(value) != fields:
        raise RetainedBronzePublicationError("publication descriptor has invalid fields")
    if (not isinstance(value["id"], str) or not value["id"] or
        not re.fullmatch(r"fragment-[0-9a-f-]{36}\.parquet", str(value["name"])) or
        type(value["size"]) is not int or not 0 < value["size"] <= MAX_LANDING_FILE_BYTES or
        not SHA_RE.fullmatch(str(value["sha256"])) or
        type(value["row_count"]) is not int or value["row_count"] <= 0 or
        (part and (type(value["part"]) is not int or value["part"] <= 0))):
        raise RetainedBronzePublicationError("publication descriptor values are invalid")
    return dict(value)


def _taxonomy_entries(path: Path) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT indicator_id, indicator_name, indicator_name_en, thematic_area, domain, taxonomy_path "
            "FROM read_parquet(?) ORDER BY indicator_id", [str(path)]
        ).fetchall()
    finally:
        con.close()
    result = []
    for row in rows:
        if not isinstance(row[0], int) or not isinstance(row[1], str) or not row[1]:
            raise RetainedBronzePublicationError("taxonomy has an invalid indicator identity")
        result.append({
            "indicator_id": row[0], "indicator_name": row[1],
            "indicator_name_en": row[2] or "", "thematic_area": row[3] or "",
            "domain": row[4] or "", "taxonomy_path": row[5] or "",
            "status": "pending", "row_count": 0, "parts": [],
        })
    if len(result) != 1550 or len({item["indicator_id"] for item in result}) != 1550:
        raise RetainedBronzePublicationError("taxonomy does not expose exactly 1,550 indicators")
    return result


def _read_previous(store: Any) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    pointer = store.load_landing_pointer()
    if pointer is None:
        return None, None
    raw = store.read_landing_object({
        "id": pointer.get("manifest_file_id"), "name": pointer.get("manifest_file_name"),
        "size": pointer.get("manifest_size_bytes"), "sha256": pointer.get("manifest_sha256"),
    }, maximum_bytes=MAX_LANDING_MANIFEST_BYTES)
    manifest = json.loads(raw)
    expected_fields = {"format_version", "kind", "source_id", "snapshot_id", "created_at_utc",
        "code_sha", "status", "layer", "coverage_status", "lineage_status", "inventory_sha256",
        "audit_report_sha256", "audit_run_id", "indicator_count", "published_indicator_count",
        "pending_indicator_count", "indicator_index", "datasets", "observation_schema", "tests"}
    if (
        not isinstance(manifest, dict) or set(manifest) != expected_fields or manifest.get("format_version") != 1
        or manifest.get("kind") != KIND or manifest.get("status") != "validated"
        or manifest.get("layer") != "02_bronze"
        or manifest.get("coverage_status") != "incomplete_retained_inventory"
        or manifest.get("lineage_status") != "unresolved_native_to_bronze"
        or manifest.get("source_id") != SOURCE_ID or manifest.get("snapshot_id") != pointer.get("snapshot_id")
    ):
        raise RetainedBronzePublicationError("current retained Bronze manifest has invalid identity")
    return pointer, manifest


def _publish_retained_bronze(
    store: Any, audit_dir: Path, workspace: Path, code_sha: str, *, max_indicators: int = 8
) -> dict[str, Any]:
    """Publish one resumable indicator increment and promote only a verified candidate."""
    if not re.fullmatch(r"[0-9a-f]{40}", code_sha):
        raise RetainedBronzePublicationError("code_sha must be an exact Git commit")
    if not 1 <= max_indicators <= 64:
        raise RetainedBronzePublicationError("max_indicators must be between 1 and 64")
    audit_dir, workspace = audit_dir.resolve(), workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    with (workspace / "publisher.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RetainedBronzePublicationError("another retained Bronze publisher owns this workspace") from exc
        report, inventory, audit_sha = validate_audit(audit_dir)
        by_path = _descriptor_by_path(inventory)
        taxonomy_path = audit_dir / "verified-cache/taxonomy/br_dbw_indicators.parquet"
        entries = _taxonomy_entries(taxonomy_path)
        _, previous = _read_previous(store)
        datasets: dict[str, Any] = {}
        previous_index: dict[int, dict[str, Any]] = {}
        if previous is not None:
            if (
                previous.get("inventory_sha256") != inventory["inventory_sha256"]
                or previous.get("audit_report_sha256") != audit_sha
                or not isinstance(previous.get("datasets"), list)
            ):
                raise RetainedBronzePublicationError("current retained Bronze snapshot uses different audit evidence")
            if previous.get("indicator_count") != 1550 or (
                previous.get("published_indicator_count", -1) + previous.get("pending_indicator_count", -1) != 1550
            ):
                raise RetainedBronzePublicationError("current retained Bronze snapshot counts are invalid")
            if {item.get("name") for item in previous["datasets"] if isinstance(item, dict)} != {
                "observations", "dictionaries", "metadata", "taxonomy"
            }:
                raise RetainedBronzePublicationError("current retained Bronze datasets are invalid")
            datasets = {item["name"]: item for item in previous["datasets"] if item["name"] != "observations"}
            previous_descriptors: list[dict[str, Any]] = []
            for name, dataset in datasets.items():
                if dataset.get("columns") != [{"name": n, "type": t} for n,t in EXPECTED_SCHEMAS[name]]:
                    raise RetainedBronzePublicationError("current retained Bronze dataset schema changed")
                dataset["files"] = [_publication_descriptor(item) for item in dataset.get("files", [])]
                if sum(item["row_count"] for item in dataset["files"]) != dataset.get("row_count"):
                    raise RetainedBronzePublicationError("current retained Bronze dataset row counts changed")
                previous_descriptors.extend(dataset["files"])
            index_desc = previous.get("indicator_index")
            if (not isinstance(index_desc, dict) or set(index_desc) != {"id", "name", "size", "sha256"} or
                not re.fullmatch(r"fragment-[0-9a-f-]{36}\.json", str(index_desc.get("name", ""))) or
                not SHA_RE.fullmatch(str(index_desc.get("sha256", "")))):
                raise RetainedBronzePublicationError("current indicator index descriptor changed")
            index_raw = store.read_landing_object(index_desc, maximum_bytes=MAX_INDEX_BYTES)
            index = json.loads(index_raw)
            if (index.get("source_id") != SOURCE_ID or index.get("inventory_sha256") != inventory["inventory_sha256"]):
                raise RetainedBronzePublicationError("current indicator index audit binding changed")
            previous_index = {item["indicator_id"]: item for item in index.get("indicators", [])}
            if len(previous_index) != 1550:
                raise RetainedBronzePublicationError("current indicator index is incomplete")
            for item in entries:
                prior = previous_index.get(item["indicator_id"])
                if prior is None or any(prior.get(k) != item[k] for k in ("indicator_name", "indicator_name_en", "thematic_area", "domain", "taxonomy_path")):
                    raise RetainedBronzePublicationError("current indicator index taxonomy changed")
                status, row_count = prior.get("status"), prior.get("row_count")
                parts = [_publication_descriptor(value, part=True) for value in prior.get("parts", [])]
                if (status not in {"pending", "published"} or type(row_count) is not int or row_count < 0 or
                    (status == "pending" and (parts or row_count)) or
                    (status == "published" and (not parts or sum(p["row_count"] for p in parts) != row_count)) or
                    [part["part"] for part in parts] != list(range(1, len(parts)+1))):
                    raise RetainedBronzePublicationError("current indicator publication state changed")
                item.update({"status": status, "row_count": row_count, "parts": parts})
                previous_descriptors.extend(parts)
            store.verify_landing_objects_metadata(previous_descriptors)
            if previous["pending_indicator_count"] == 0:
                return previous

        with tempfile.TemporaryDirectory(dir=workspace) as tmp:
            temp = Path(tmp)
            # Fixed datasets are published once. Existing descriptors are reused after readback.
            for name, rel_path in (
                ("dictionaries", "dictionaries/br_dbw_dictionaries.parquet"),
                ("metadata", "metadata/br_dbw_metadata.parquet"),
                ("taxonomy", "taxonomy/br_dbw_indicators.parquet"),
            ):
                if name in datasets:
                    continue
                source = _verified_input(audit_dir, by_path[rel_path])
                files, row_count = [], 0
                for fragment, rows in bounded_fragments(source, EXPECTED_SCHEMAS[name], temp):
                    descriptor = store.put_landing_object(f"fragment-{uuid4()}.parquet", fragment.read_bytes())
                    descriptor["row_count"] = rows
                    files.append(descriptor); row_count += rows
                    if fragment.parent == temp: fragment.unlink()
                if row_count != report["measured_parquet_rows"][name]:
                    raise RetainedBronzePublicationError(f"{name} row count changed from audit")
                datasets[name] = {"name": name, "table_name": f"br_dbw_{'indicators' if name == 'taxonomy' else name}",
                    "row_count": row_count, "columns": [{"name": n, "type": t} for n,t in EXPECTED_SCHEMAS[name]], "files": files}

            completed = 0
            for item in entries:
                if item["status"] == "published" or completed >= max_indicators:
                    continue
                indicator_id = item["indicator_id"]
                rel_path = f"observations/part_{indicator_id}.parquet"
                if rel_path not in by_path:
                    raise RetainedBronzePublicationError("observation inventory is missing an indicator")
                source = _verified_input(audit_dir, by_path[rel_path])
                parts, row_count = [], 0
                for number, (fragment, rows) in enumerate(bounded_fragments(source, EXPECTED_SCHEMAS["observations"], temp), 1):
                    descriptor = store.put_landing_object(f"fragment-{uuid4()}.parquet", fragment.read_bytes())
                    descriptor.update({"row_count": rows, "part": number})
                    parts.append(descriptor); row_count += rows
                    if store.read_landing_object({k: descriptor[k] for k in ("id", "name", "size", "sha256")}) != fragment.read_bytes():
                        raise RetainedBronzePublicationError("uploaded observation fragment readback changed")
                    if fragment.parent == temp: fragment.unlink()
                item.update({"status": "published", "row_count": row_count, "parts": parts})
                completed += 1

        published = sum(item["status"] == "published" for item in entries)
        observation_rows = sum(item["row_count"] for item in entries)
        if published == 1550 and observation_rows != report["measured_parquet_rows"]["observations"]:
            raise RetainedBronzePublicationError("complete retained observation rows differ from the audit")
        index = {"format_version": 1, "kind": "retained_bronze_indicator_index", "source_id": SOURCE_ID,
            "inventory_sha256": inventory["inventory_sha256"], "indicator_count": 1550,
            "published_indicator_count": published, "pending_indicator_count": 1550-published,
            "indicators": entries}
        index_raw = canonical(index)
        if len(index_raw) > MAX_INDEX_BYTES:
            raise RetainedBronzePublicationError("indicator index exceeds 8 MiB")
        index_desc = store.put_landing_object(f"fragment-{uuid4()}.json", index_raw)
        index_desc["sha256"] = hashlib.sha256(index_raw).hexdigest()
        snapshot_id = str(uuid4())
        manifest = {
            "format_version": 1, "kind": KIND, "source_id": SOURCE_ID,
            "snapshot_id": snapshot_id, "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "code_sha": code_sha, "status": "validated", "layer": "02_bronze",
            "coverage_status": "incomplete_retained_inventory", "lineage_status": "unresolved_native_to_bronze",
            "inventory_sha256": inventory["inventory_sha256"], "audit_report_sha256": audit_sha,
            "audit_run_id": report["run_id"], "indicator_count": 1550,
            "published_indicator_count": published, "pending_indicator_count": 1550-published,
            "indicator_index": index_desc,
            "datasets": [datasets[name] for name in ("observations", "dictionaries", "metadata", "taxonomy") if name in datasets],
            "observation_schema": [{"name": n, "type": t} for n,t in EXPECTED_SCHEMAS["observations"]],
            "tests": {"passed": True, "rows_and_schemas_preserved": True},
        }
        manifest["datasets"].insert(0, {"name": "observations", "table_name": "br_dbw_observations",
            "row_count": observation_rows,
            "columns": manifest["observation_schema"], "files": []})
        raw = canonical(manifest)
        if len(raw) > MAX_LANDING_MANIFEST_BYTES:
            raise RetainedBronzePublicationError("retained Bronze manifest exceeds 1 MiB")
        descriptor = store.put_landing_object(f"manifest-{snapshot_id}.json", raw, maximum_bytes=MAX_LANDING_MANIFEST_BYTES)
        pointer = {"format_version": 1, "source_id": SOURCE_ID, "snapshot_id": snapshot_id,
            "manifest_file_id": descriptor["id"], "manifest_file_name": descriptor["name"],
            "manifest_sha256": descriptor["sha256"], "manifest_size_bytes": descriptor["size"]}
        if store.read_landing_object(descriptor, maximum_bytes=MAX_LANDING_MANIFEST_BYTES) != raw:
            raise RetainedBronzePublicationError("candidate manifest readback changed")
        store.read_landing_object(index_desc, maximum_bytes=MAX_INDEX_BYTES)
        store.promote_landing_pointer(pointer)
        if store.load_landing_pointer() != pointer:
            raise RetainedBronzePublicationError("promoted retained Bronze pointer readback changed")
        return manifest


def _with_owner(store: Any, operation: Any) -> dict[str, Any]:
    owner = str(uuid4())
    store.acquire_publication_owner(owner)
    primary: BaseException | None = None
    try:
        return operation()
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            store.release_publication_owner()
        except BaseException:
            if primary is None:
                raise


def publish_retained_bronze(
    store: Any, audit_dir: Path, workspace: Path, code_sha: str, *, max_indicators: int = 8
) -> dict[str, Any]:
    return _with_owner(store, lambda: _publish_retained_bronze(
        store, audit_dir, workspace, code_sha, max_indicators=max_indicators
    ))


def publish_retained_bronze_until_complete(
    store: Any, audit_dir: Path, workspace: Path, code_sha: str, *, max_indicators: int = 8,
    on_increment: Any | None = None,
) -> dict[str, Any]:
    """Continue finite increments while durable pending count strictly decreases."""
    def run() -> dict[str, Any]:
        previous_pending = 1551
        while True:
            result = _publish_retained_bronze(
                store, audit_dir, workspace, code_sha, max_indicators=max_indicators
            )
            if on_increment is not None:
                on_increment(result)
            pending = result["pending_indicator_count"]
            if pending == 0:
                return result
            if pending >= previous_pending:
                raise RetainedBronzePublicationError("publication continuation made no durable progress")
            previous_pending = pending
    return _with_owner(store, run)
