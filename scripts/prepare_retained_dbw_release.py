#!/usr/bin/env python3
"""Restore the reviewed DBW input package from Drive, without remote writes."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
import audit_retained_dbw_bronze as audit
from retained_dbw_publication import (
    REVIEWED_AUDIT_REPORT_SHA256, REVIEWED_INVENTORY_SHA256, validate_audit,
)

BASELINE = ROOT / "docs/audits/2026-09-21-dbw-retained-report.json"
SOURCE_FIELDS = (
    "format_version", "status", "source_id", "diagnostic_scope",
    "inventory_sha256", "expected_retained_indicator_inventory",
    "indicator_receipts", "observation_partitions", "remote_object_count",
    "remote_total_bytes", "receipt_reported_native_names",
    "measured_parquet_rows", "schemas", "remote_inventory_stable_after_restore",
)


class PreparationError(RuntimeError):
    """The reviewed input package cannot be reproduced safely."""


def reviewed_report(path: Path = BASELINE) -> tuple[bytes, dict]:
    if path.stat().st_size > 1024 * 1024:
        raise PreparationError("Reviewed audit report exceeds its size bound")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != REVIEWED_AUDIT_REPORT_SHA256:
        raise PreparationError("Reviewed audit report SHA-256 does not match")
    report = json.loads(raw)
    if (not isinstance(report, dict)
            or report.get("inventory_sha256") != REVIEWED_INVENTORY_SHA256
            or any(field not in report for field in SOURCE_FIELDS)):
        raise PreparationError("Reviewed audit report identity is invalid")
    return raw, report


def materialize_native_tree(audit_dir: Path, data_root: Path, inventory: dict) -> dict:
    """Hard-link every reviewed object into one immutable modeled-input tree."""
    data_root = data_root.absolute()
    if data_root.exists() or data_root.is_symlink():
        raise PreparationError("Use a new data root for each restore")
    objects = inventory.get("objects")
    if not isinstance(objects, list) or not objects:
        raise PreparationError("Reviewed descriptor inventory is empty")
    release_root = (
        data_root / "02_bronze" / "gus_dbw" / "releases"
        / REVIEWED_INVENTORY_SHA256
    )
    cache = audit_dir / "verified-cache"
    linked = 0
    total_bytes = 0
    try:
        for item in objects:
            relative = Path(item["path"])
            source = cache / relative
            target = release_root / relative
            if (
                source.is_symlink()
                or not source.is_file()
                or source.stat().st_size != item["size"]
            ):
                raise PreparationError(
                    f"Verified cache object is absent or changed: {relative.as_posix()}"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            os.link(source, target)
            linked += 1
            total_bytes += item["size"]
    except Exception:
        shutil.rmtree(data_root, ignore_errors=True)
        raise
    return {
        "data_root": str(data_root),
        "release_root": str(release_root),
        "materialized_objects": linked,
        "materialized_bytes": total_bytes,
        "materialization": "hard_links_to_verified_cache",
    }


def prepare(storage, output: Path, *, baseline: Path = BASELINE) -> dict:
    """Preserve historic audit bytes separately from fresh restoration evidence."""
    baseline_raw, original = reviewed_report(baseline)
    output = output.absolute()
    if output.exists() or output.is_symlink():
        raise PreparationError("Use a new output directory for each restore")
    output.mkdir(parents=True)
    receipt = {
        "format_version": 1, "source_id": "gus_dbw",
        "operation": "restore_reviewed_audit", "read_only": True,
        "status": "running", "original_audit_run_id": original["run_id"],
        "original_audited_at_utc": original["audited_at_utc"],
        "inventory_sha256": REVIEWED_INVENTORY_SHA256,
        "audit_report_sha256": REVIEWED_AUDIT_REPORT_SHA256,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "publication_performed": False,
    }
    evidence_path = output / "restore-evidence.json"
    audit._atomic_json(evidence_path, receipt)
    try:
        descriptors, _ = audit.validate_inventory(audit.discover(storage))
        observed = audit.inventory_document(descriptors)
        if (observed["inventory_sha256"] != REVIEWED_INVENTORY_SHA256
                or observed["object_count"] != original["remote_object_count"]
                or observed["total_bytes"] != original["remote_total_bytes"]):
            raise PreparationError("Drive inventory differs from the reviewed snapshot")
        package = output / "audit"
        fresh = audit.audit_retained_dbw(storage, package, workers=4)
        mismatches = [key for key in SOURCE_FIELDS if fresh.get(key) != original[key]]
        if mismatches:
            raise PreparationError("Restored evidence differs: " + ", ".join(mismatches))
        (package / "audit-report.json").rename(output / "fresh-audit-report.json")
        (package / "run-status.json").rename(output / "fresh-run-status.json")
        (package / "audit-report.json").write_bytes(baseline_raw)
        # This compatibility envelope identifies the historic reviewed snapshot;
        # it does not pretend that the original producer ran again.
        audit._atomic_json(package / "run-status.json", {
            "format_version": 1, "source_id": "gus_dbw", "status": "complete",
            "run_id": original["run_id"], "inventory_sha256": REVIEWED_INVENTORY_SHA256,
            "kind": "restored_reviewed_audit", "restore_run_id": fresh["run_id"],
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        })
        _accepted, inventory, _audit_sha = validate_audit(
            package, require_reviewed_snapshot=True
        )
        materialized = materialize_native_tree(
            package, output / "data-root", inventory
        )
        receipt.update(
            status="verified", restore_run_id=fresh["run_id"],
            verified_objects=fresh["remote_object_count"],
            verified_bytes=fresh["remote_total_bytes"],
            measured_parquet_rows=fresh["measured_parquet_rows"], package="audit",
            **materialized,
        )
    except Exception:
        receipt["status"] = "failed"
        package = output / "audit"
        if package.exists():
            audit._atomic_json(package / "run-status.json", {
                "format_version": 1, "source_id": "gus_dbw", "status": "failed",
                "operation": "restore_reviewed_audit",
            })
        raise
    finally:
        receipt["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        audit._atomic_json(evidence_path, receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--drive-root-id", required=True)
    args = parser.parse_args()
    reviewed_report()
    storage = audit.StorageManager(allow_interactive_auth=False, root_id=args.drive_root_id)
    print(json.dumps(prepare(storage, args.output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
