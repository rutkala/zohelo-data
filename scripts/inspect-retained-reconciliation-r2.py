#!/usr/bin/env python3
"""Read-only retained R2 inventories for source-specific reconciliation planning.

Writes only the requested local JSON artifact. No provider calls, remote writes,
Bronze acceptance claims, archival, or deletion. Full inventories are retained in
the artifact; bounded probes report schemas and descriptors, never data records.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path, PurePosixPath
import runpy
import zipfile

BASE = Path(__file__).resolve().parent
HELPERS = runpy.run_path(str(BASE / "reconcile-r2-medallion.py"))
SOURCES = ("opendata_org", "gus_bdl", "world_bank_wdi", "gus_dbw")
SUMMARY_FIELDS = {
    "format_version", "kind", "record_type", "source_id", "status", "complete",
    "coverage_status", "row_count", "total_rows", "accepted_distribution_count",
    "published_distribution_count", "pending_publication_count", "subgroup_id",
    "selection_id", "table_name", "layer", "snapshot_id", "transport",
    "content_validation", "landing_scope", "archive_readback_verified",
}
DESCRIPTOR_FIELDS = {
    "id", "file_id", "drive_id", "name", "file_name", "parquet_name", "key",
    "path", "member", "size", "bytes", "rows", "sha256", "md5Checksum",
    "raw_file_id", "raw_file_name", "raw_sha256", "raw_size_bytes", "category",
    "subgroup_id", "selection_id", "task_kind", "dataset_id", "archive_sha256",
}
SKIP_FIELDS = {
    "payload", "payload_utf8", "response", "results", "FEATURES", "features",
    "source_record_json", "record", "records", "request", "headers", "token",
    "access_token", "credentials", "authorization", "dimension_inventory",
}
SKIP_FIELDS_LOWER = {name.lower() for name in SKIP_FIELDS}


def inventory(s3, bucket, prefix):
    result = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/") and not obj.get("Size"):
                continue
            result.append({"key": key, "size": int(obj["Size"]), "etag": obj.get("ETag")})
    return sorted(result, key=lambda item: item["key"])


class R2Range(io.RawIOBase):
    """Small range cache for ZIP directories and Parquet footers in private R2."""

    def __init__(self, s3, bucket, obj):
        self.s3, self.bucket, self.obj = s3, bucket, obj
        self.pos, self.start, self.cache = 0, -1, b""

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, offset, whence=io.SEEK_SET):
        pos = offset + (self.pos if whence == io.SEEK_CUR else self.obj["size"] if whence == io.SEEK_END else 0)
        if pos < 0:
            raise ValueError("negative seek")
        self.pos = pos
        return pos

    def read(self, size=-1):
        remaining = max(0, self.obj["size"] - self.pos)
        size = remaining if size < 0 else min(size, remaining)
        if not size:
            return b""
        if self.start <= self.pos and self.pos + size <= self.start + len(self.cache):
            data = self.cache[self.pos-self.start:self.pos-self.start+size]
        else:
            end = min(self.obj["size"]-1, self.pos+max(size, 1024*1024)-1)
            args = {"Bucket": self.bucket, "Key": self.obj["key"], "Range": f"bytes={self.pos}-{end}"}
            if self.obj.get("etag"):
                args["IfMatch"] = self.obj["etag"]
            response = self.s3.get_object(**args)
            body = response["Body"]
            try:
                self.cache = body.read()
            finally:
                body.close()
            if len(self.cache) != end-self.pos+1:
                raise RuntimeError("short R2 range read")
            self.start = self.pos
            data = self.cache[:size]
        self.pos += len(data)
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)


def control_summary(value):
    """Expose state structure and file descriptors; omit payload and row values."""
    descriptors, counts = [], Counter()

    def walk(node, path="", depth=0):
        if depth > 16:
            counts["depth_limit_reached"] += 1
            return
        if isinstance(node, dict):
            for field in ("task_kind", "kind", "record_type"):
                if isinstance(node.get(field), str):
                    counts[f"{field}:{node[field]}"] += 1
            if any(k in node for k in ("sha256", "raw_sha256", "parquet_name", "raw_file_id", "archive_sha256")):
                descriptor = {k: v for k, v in node.items() if k in DESCRIPTOR_FIELDS and isinstance(v, (str, int, float, bool, type(None)))}
                descriptors.append({"json_path": path, **descriptor})
            for key, child in node.items():
                if key.lower() in SKIP_FIELDS_LOWER:
                    continue
                if isinstance(child, (dict, list)):
                    walk(child, f"{path}/{key}", depth+1)
        elif isinstance(node, list):
            for index, child in enumerate(node):
                if isinstance(child, (dict, list)):
                    walk(child, f"{path}/{index}", depth+1)

    walk(value)
    result = {"json_type": type(value).__name__, "descriptor_count": len(descriptors), "descriptors": descriptors, "kind_counts": dict(counts)}
    if isinstance(value, dict):
        result["top_level_keys"] = sorted(value)
        result["summary"] = {k: v for k, v in value.items() if k in SUMMARY_FIELDS and isinstance(v, (str, int, float, bool, type(None)))}
        result["container_sizes"] = {k: len(v) for k, v in value.items() if isinstance(v, (dict, list))}
    return result


def probe(s3, bucket, obj):
    stream = R2Range(s3, bucket, obj)
    magic = stream.read(4)
    result = {"key": obj["key"], "size": obj["size"]}
    stream.seek(0)
    if magic.startswith(b"PK"):
        with zipfile.ZipFile(stream) as archive:
            result.update(kind="zip", members=[{"name": member.filename, "size": member.file_size, "compressed_size": member.compress_size, "crc32": f"{member.CRC:08x}"} for member in archive.infolist()])
            result["uncompressed_bytes"] = sum(member["size"] for member in result["members"])
            result["member_count"] = len(result["members"])
            result["csv_headers"] = []
            for member in [m for m in archive.infolist() if m.filename.lower().endswith(".csv")][:3]:
                with archive.open(member) as body:
                    header = body.readline(65536)
                if len(header) >= 65536:
                    result["csv_headers"].append({"member": member.filename, "status": "header_exceeds_probe_limit"})
                else:
                    try:
                        decoded = header.decode("utf-8-sig")
                    except UnicodeDecodeError:
                        result["csv_headers"].append({"member": member.filename, "status": "header_not_utf8"})
                        continue
                    delimiter = ";" if decoded.count(";") > decoded.count(",") else ","
                    result["csv_headers"].append({"member": member.filename, "delimiter": delimiter, "columns": next(csv.reader([decoded], delimiter=delimiter))})
    elif magic == b"PAR1":
        import pyarrow.parquet as pq
        parquet = pq.ParquetFile(stream)
        result.update(kind="parquet", rows=parquet.metadata.num_rows, schema=str(parquet.schema_arrow))
    elif obj["size"] <= 1024*1024 and magic.lstrip().startswith((b"{", b"[")):
        value = json.loads(stream.read())
        result.update(kind="json", structure=control_summary(value))
    else:
        result["kind"] = "gzip" if magic.startswith(b"\x1f\x8b") else "not_inspected"
    return result


def table_inventory(cat, source, namespaces):
    result = []
    for namespace in ("bronze", "silver", "gold"):
        for identifier in namespaces[namespace]:
            if HELPERS["source_from_table"](namespace, identifier[-1]) != source:
                continue
            table = cat.load_table(identifier)
            snapshot = table.current_snapshot()
            files = list(table.scan().plan_files())
            result.append({"table": ".".join(identifier), "snapshot_id": str(snapshot.snapshot_id) if snapshot else None,
                "schema": [{"id": field.field_id, "name": field.name, "type": str(field.field_type), "required": field.required} for field in table.schema().fields],
                "data_files": [{"path": task.file.file_path, "rows": int(task.file.record_count), "bytes": int(task.file.file_size_in_bytes)} for task in files],
                "lineage_properties": {k: v for k, v in table.properties.items() if k.startswith("zohelo.lineage.")}})
    return result


def representative_objects(objects, limit):
    groups = {}
    for obj in objects:
        parts = PurePosixPath(obj["key"]).parts
        group = (parts[2] if len(parts) > 2 else "", PurePosixPath(obj["key"]).suffix)
        groups.setdefault(group, []).append(obj)
    selected = {}
    # Sample each path/kind first; then first/middle/last within each group.
    for position in (0, 1, 2):
        for group in groups.values():
            obj = group[(0, len(group)//2, len(group)-1)[position]]
            selected[obj["key"]] = obj
            if len(selected) >= limit:
                return list(selected.values())
    return list(selected.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=SOURCES, nargs="+", default=list(SOURCES))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-probes", type=int, default=12)
    parser.add_argument("--max-control-files", type=int, default=48)
    parser.add_argument("--max-control-bytes", type=int, default=16*1024*1024)
    args = parser.parse_args()
    if min(args.max_probes, args.max_control_files, args.max_control_bytes) < 0:
        parser.error("inspection limits must be nonnegative")
    s3, cat = HELPERS["s3_client"](), HELPERS["catalog"]()
    landing_bucket, lakehouse_bucket = HELPERS["req"]("R2_LANDING_BUCKET"), HELPERS["req"]("R2_LAKEHOUSE_BUCKET")
    namespaces = {ns: cat.list_tables(ns) for ns in ("bronze", "silver", "gold")}
    report = {"observed_at_utc": datetime.now(timezone.utc).isoformat(), "operation": "retained-reconciliation-inventory",
        "writes_performed": False, "provider_calls_performed": False, "coverage_proven": False,
        "landing_bucket": landing_bucket, "lakehouse_bucket": lakehouse_bucket, "sources": {}}
    for source in dict.fromkeys(args.source):
        landing = inventory(s3, landing_bucket, f"01_landing/{source}/")
        archive = inventory(s3, landing_bucket, f"05_archive/{source}/")
        control = inventory(s3, lakehouse_bucket, f"06_control/source_campaigns/{source}")
        control += inventory(s3, lakehouse_bucket, f"06_control/lineage/{source}/")
        counts = Counter(PurePosixPath(obj["key"]).name for obj in landing)
        item = {"landing": landing, "archive": archive, "archive_scope": f"05_archive/{source}/ only; alternate historical layouts are not ruled out", "control": control,
            "duplicate_landing_basenames": {name: count for name, count in counts.items() if count > 1},
            "tables": table_inventory(cat, source, namespaces), "probes": [], "control_probes": []}
        candidates = [(lakehouse_bucket, obj) for obj in control]
        candidates += [(landing_bucket, obj) for obj in landing if "/_control/" in obj["key"]]
        candidates = [(bucket, obj) for bucket, obj in candidates if obj["key"].endswith(".json")]
        candidates.sort(key=lambda pair: (not any(word in PurePosixPath(pair[1]["key"]).name for word in ("checkpoint", "state", "current", "complete")), pair[1]["key"]))
        used = 0
        for bucket, obj in candidates:
            if len(item["control_probes"]) >= args.max_control_files or used + obj["size"] > args.max_control_bytes:
                continue
            entry = {"bucket": bucket, "key": obj["key"], "size": obj["size"]}
            try:
                entry["structure"] = control_summary(json.loads(R2Range(s3, bucket, obj).read()))
            except Exception as exc:
                entry["error_type"] = type(exc).__name__
            item["control_probes"].append(entry)
            used += obj["size"]
        item["control_probe_limits"] = {"candidate_files": len(candidates), "inspected_files": len(item["control_probes"]), "read_bytes": used}
        for obj in representative_objects(landing, args.max_probes) if args.max_probes else []:
            try:
                item["probes"].append(probe(s3, landing_bucket, obj))
            except Exception as exc:
                item["probes"].append({"key": obj["key"], "error_type": type(exc).__name__})
        report["sources"][source] = item
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True)+"\n", encoding="utf-8")
        print(json.dumps({"source": source, "landing_objects": len(landing), "landing_bytes": sum(obj["size"] for obj in landing),
            "archive_objects": len(archive), "tables": len(item["tables"]), "probes": len(item["probes"]),
            "control_probe_limits": item["control_probe_limits"], "coverage_proven": False}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
