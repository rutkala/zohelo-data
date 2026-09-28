"""Resumably decode retained Eurostat distributions into immutable Bronze files.

This downstream stage never calls Eurostat and never mutates Landing. Each
bounded run pins the exact accepted-receipt prefix, restores one native object
at a time, invokes :mod:`eurostat_bulk_decode`, uploads a content-addressed
Parquet file, and only then advances its independent durable checkpoint.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
from typing import Any, Mapping

from eurostat_bulk_decode import decode_full_distribution


SOURCE_CAMPAIGN_ID = "eurostat_bulk"
BRONZE_CAMPAIGN_ID = "eurostat_bulk_bronze"
_STATE_VERSION = 1
_CODE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class EurostatBulkBronzeError(RuntimeError):
    """The retained-receipt Bronze campaign cannot advance safely."""


def run_bronze_batch(
    source_store: Any,
    source_raw_store: Any,
    bronze_store: Any,
    output_store: Any,
    workdir: Path,
    *,
    owner: str,
    code_sha: str,
    max_distributions: int = 64,
    max_seconds: int = 2400,
    clock=time.monotonic,
) -> dict[str, Any]:
    """Process a bounded accepted-receipt prefix and save after every output."""
    if type(max_distributions) is not int or not 1 <= max_distributions <= 256:
        raise EurostatBulkBronzeError("max_distributions must be between 1 and 256")
    if type(max_seconds) is not int or not 60 <= max_seconds <= 3300:
        raise EurostatBulkBronzeError("max_seconds must be between 60 and 3300")
    if not isinstance(code_sha, str) or _CODE_SHA_RE.fullmatch(code_sha) is None:
        raise EurostatBulkBronzeError("code_sha must be an exact Git commit SHA")
    workdir = Path(workdir)
    if workdir.is_symlink():
        raise EurostatBulkBronzeError("Bronze work directory cannot be a symlink")
    workdir.mkdir(parents=True, exist_ok=True)
    if not workdir.is_dir():
        raise EurostatBulkBronzeError("Bronze work directory is not a directory")

    source_state = source_store.load()
    source_receipts = _source_receipts(source_state)
    state = bronze_store.load()
    if state is None:
        state = _new_state()
    _validate_state(state, source_receipts)

    started = clock()
    decoded = 0
    skipped = 0
    bronze_store.acquire_publication_owner(owner)
    primary: BaseException | None = None
    try:
        while (
            state["source_receipt_count"] < len(source_receipts)
            and decoded < max_distributions
            and clock() - started < max_seconds
        ):
            index = state["source_receipt_count"]
            receipt_descriptor = source_receipts[index]
            receipt = source_store.read_receipt(receipt_descriptor)
            if not _is_data_receipt(receipt):
                state["source_receipt_count"] = index + 1
                state["source_receipt_checkpoint_sha256"] = _prefix_sha256(
                    source_receipts, index + 1
                )
                state["skipped_non_data_receipts"] += 1
                skipped += 1
                continue

            receipt_sha = _receipt_sha(receipt_descriptor)
            if receipt_sha in state["completed"]:
                raise EurostatBulkBronzeError(
                    "completed receipt appears at or beyond the saved source cursor"
                )
            _require_local_headroom(workdir, receipt)
            distribution = receipt.get("distribution", {})
            raw_descriptor = receipt.get("raw", {})
            print(
                json.dumps(
                    {
                        "status": "eurostat_bulk_bronze_distribution_started",
                        "source_receipt_index": index,
                        "source_receipt_sha256": receipt_sha,
                        "distribution_id": distribution.get("dataset_id"),
                        "raw_size_bytes": raw_descriptor.get("size_bytes"),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            distribution_dir = workdir / f"receipt-{receipt_sha}"
            if distribution_dir.exists():
                raise EurostatBulkBronzeError("Bronze receipt work path is not fresh")
            distribution_dir.mkdir()
            try:
                receipt_path = distribution_dir / "receipt.json"
                receipt_raw = _canonical_bytes(receipt)
                _verify_receipt_bytes(receipt_descriptor, receipt_raw)
                receipt_path.write_bytes(receipt_raw)
                source_path = distribution_dir / "distribution.tsv.gz"
                source_raw_store.read_to_file(receipt["raw"], source_path)
                output_path = distribution_dir / "observations.parquet"
                report = decode_full_distribution(
                    source_path,
                    receipt_path,
                    output_path,
                    receipt_descriptor=_decoder_receipt_descriptor(receipt_descriptor),
                )
                output = output_store.put_file(
                    output_path,
                    {
                        "source_id": "eurostat",
                        "layer": "02_bronze",
                        "dataset_id": report["dataset_id"],
                        "distribution_id": report["distribution_id"],
                        "source_receipt_sha256": receipt_sha,
                        "decoder_code_sha": code_sha,
                    },
                )
            finally:
                shutil.rmtree(distribution_dir, ignore_errors=True)

            candidate = deepcopy(state)
            candidate["completed"][receipt_sha] = {
                "source_receipt": _compact_receipt_descriptor(receipt_descriptor),
                "decoder_code_sha": code_sha,
                "dataset_id": report["dataset_id"],
                "distribution_id": report["distribution_id"],
                "partition_id": report["partition_id"],
                "empty_partition": report["empty_partition"],
                "observation_cells": report["observation_cells"],
                "output": _compact_output_descriptor(output),
            }
            candidate["source_receipt_count"] = index + 1
            candidate["source_receipt_checkpoint_sha256"] = _prefix_sha256(
                source_receipts, index + 1
            )
            candidate["processed_data_receipts"] += 1
            candidate["observation_cells"] += report["observation_cells"]
            candidate["output_bytes"] += output["size_bytes"]
            candidate["last_output_receipt_sha256"] = receipt_sha

            current_source = source_store.load_cached()
            current_receipts = _source_receipts(current_source)
            if (
                len(current_receipts) < index + 1
                or _prefix_sha256(current_receipts, index + 1)
                != candidate["source_receipt_checkpoint_sha256"]
            ):
                raise EurostatBulkBronzeError(
                    "Eurostat accepted-receipt prefix changed during Bronze decode"
                )
            bronze_store.guard_publication_owner()
            bronze_store.save(candidate)
            state = candidate
            decoded += 1
            print(
                json.dumps(
                    {
                        "status": "eurostat_bulk_bronze_distribution_completed",
                        "source_receipt_index": index,
                        "source_receipt_sha256": receipt_sha,
                        "distribution_id": report["distribution_id"],
                        "observation_cells": report["observation_cells"],
                        "output_bytes": output["size_bytes"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

        # Persist progress that advanced only across non-data receipts.
        persisted = bronze_store.load_cached()
        if persisted != state:
            bronze_store.guard_publication_owner()
            bronze_store.save(state)
    except BaseException as exc:
        primary = exc
        raise
    finally:
        try:
            bronze_store.release_publication_owner()
        except BaseException:
            # A cleanup failure is material when it is the only failure, but it
            # must not replace the decode/upload/checkpoint evidence that made
            # cleanup necessary in the first place.
            if primary is None:
                raise

    return _summary(state, source_state, decoded=decoded, skipped=skipped)


def verify_bronze_checkpoint(
    source_store: Any,
    bronze_store: Any,
    output_store: Any,
) -> dict[str, Any]:
    """Freshly bind the checkpoint to Landing and stream its newest output."""
    source_state = source_store.load()
    source_receipts = _source_receipts(source_state)
    state = bronze_store.load()
    if state is None:
        raise EurostatBulkBronzeError("Eurostat bulk Bronze checkpoint is absent")
    _validate_state(state, source_receipts)
    latest_sha = state.get("last_output_receipt_sha256")
    verified_output = None
    if latest_sha is not None:
        latest = state["completed"].get(latest_sha)
        if not isinstance(latest, dict):
            raise EurostatBulkBronzeError("latest Bronze output is absent from checkpoint")
        verified_output = output_store.verify(latest["output"])
    return {
        **_summary(state, source_state, decoded=0, skipped=0),
        "status": "fresh_eurostat_bulk_bronze_verified",
        "verification_scope": (
            "exact_checkpoint_and_latest_output_bytes"
            if verified_output is not None
            else "exact_empty_checkpoint"
        ),
        "verified_output_sha256": (
            verified_output.get("sha256") if verified_output else None
        ),
    }


def _new_state() -> dict[str, Any]:
    return {
        "format_version": _STATE_VERSION,
        "source_id": BRONZE_CAMPAIGN_ID,
        "upstream_source_id": SOURCE_CAMPAIGN_ID,
        "source_receipt_count": 0,
        "source_receipt_checkpoint_sha256": _prefix_sha256([], 0),
        "processed_data_receipts": 0,
        "skipped_non_data_receipts": 0,
        "observation_cells": 0,
        "output_bytes": 0,
        "last_output_receipt_sha256": None,
        "pending": [],
        "completed": {},
        "recent_roots": {},
        "receipts": [],
        "rejected_receipts": [],
    }


def _validate_state(state: Any, source_receipts: list[dict[str, Any]]) -> None:
    if not isinstance(state, dict) or state.get("format_version") != _STATE_VERSION:
        raise EurostatBulkBronzeError("Eurostat bulk Bronze checkpoint format is invalid")
    if (
        state.get("source_id") != BRONZE_CAMPAIGN_ID
        or state.get("upstream_source_id") != SOURCE_CAMPAIGN_ID
    ):
        raise EurostatBulkBronzeError("Eurostat bulk Bronze checkpoint identity is invalid")
    cursor = state.get("source_receipt_count")
    completed = state.get("completed")
    if type(cursor) is not int or cursor < 0 or cursor > len(source_receipts):
        raise EurostatBulkBronzeError("Eurostat bulk Bronze source cursor is invalid")
    if not isinstance(completed, dict):
        raise EurostatBulkBronzeError("Eurostat bulk Bronze completed map is invalid")
    if state.get("source_receipt_checkpoint_sha256") != _prefix_sha256(
        source_receipts, cursor
    ):
        raise EurostatBulkBronzeError(
            "Eurostat accepted-receipt prefix no longer matches the Bronze checkpoint"
        )
    if state.get("processed_data_receipts") != len(completed):
        raise EurostatBulkBronzeError("Eurostat bulk Bronze completed count is inconsistent")
    for key in ("skipped_non_data_receipts", "observation_cells", "output_bytes"):
        if type(state.get(key)) is not int or state[key] < 0:
            raise EurostatBulkBronzeError(f"Eurostat bulk Bronze {key} is invalid")
    if state["processed_data_receipts"] + state["skipped_non_data_receipts"] != cursor:
        raise EurostatBulkBronzeError("Eurostat bulk Bronze cursor totals are inconsistent")
    for receipt_sha, value in completed.items():
        if not isinstance(receipt_sha, str) or len(receipt_sha) != 64:
            raise EurostatBulkBronzeError("Eurostat bulk Bronze receipt key is invalid")
        if not isinstance(value, dict) or not isinstance(value.get("output"), dict):
            raise EurostatBulkBronzeError("Eurostat bulk Bronze output descriptor is invalid")
        if _CODE_SHA_RE.fullmatch(value.get("decoder_code_sha", "")) is None:
            raise EurostatBulkBronzeError("Eurostat bulk Bronze decoder commit is invalid")


def _source_receipts(state: Any) -> list[dict[str, Any]]:
    if (
        not isinstance(state, dict)
        or state.get("source_id") != SOURCE_CAMPAIGN_ID
        or state.get("provider_id") != "eurostat"
        or not isinstance(state.get("receipts"), list)
    ):
        raise EurostatBulkBronzeError("Eurostat full-distribution source state is invalid")
    return state["receipts"]


def _is_data_receipt(receipt: Any) -> bool:
    return (
        isinstance(receipt, dict)
        and receipt.get("schema_version") == 1
        and receipt.get("source_id") == "eurostat"
        and receipt.get("accepted") is True
        and receipt.get("kind") == "full_distribution"
        and isinstance(receipt.get("distribution"), dict)
        and receipt["distribution"].get("kind") == "eurostat_tsv_gzip"
    )


def _summary(
    state: Mapping[str, Any],
    source_state: Mapping[str, Any],
    *,
    decoded: int,
    skipped: int,
) -> dict[str, Any]:
    source_count = len(source_state["receipts"])
    caught_up = state["source_receipt_count"] == source_count
    return {
        "status": "eurostat_bulk_bronze_batch_complete",
        "source_id": "eurostat",
        "layer": "02_bronze",
        "decoded_in_batch": decoded,
        "skipped_in_batch": skipped,
        "processed_data_receipts": state["processed_data_receipts"],
        "processed_source_receipts": state["source_receipt_count"],
        "accepted_source_receipts": source_count,
        "pending_accepted_receipts": source_count - state["source_receipt_count"],
        "observation_cells": state["observation_cells"],
        "output_bytes": state["output_bytes"],
        "retained_receipt_prefix_complete": caught_up,
        "complete_official_catalogue": False,
        "coverage_status": (
            "retained_receipt_prefix_complete" if caught_up else "incomplete"
        ),
    }


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise EurostatBulkBronzeError("Eurostat receipt is not canonical JSON") from exc


def _prefix_sha256(receipts: list[dict[str, Any]], count: int) -> str:
    return sha256(_canonical_bytes({"receipts": receipts[:count]})).hexdigest()


def _receipt_sha(descriptor: Mapping[str, Any]) -> str:
    value = descriptor.get("sha256")
    if not isinstance(value, str) or len(value) != 64:
        raise EurostatBulkBronzeError("Eurostat receipt descriptor SHA-256 is invalid")
    return value


def _verify_receipt_bytes(descriptor: Mapping[str, Any], raw: bytes) -> None:
    expected_size = descriptor.get("size_bytes")
    if (
        type(expected_size) is not int
        or expected_size <= 0
        or len(raw) != expected_size
        or sha256(raw).hexdigest() != _receipt_sha(descriptor)
    ):
        raise EurostatBulkBronzeError(
            "Eurostat receipt canonical bytes do not match the durable descriptor"
        )


def _decoder_receipt_descriptor(descriptor: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": descriptor.get("id"),
        "sha256": descriptor.get("sha256"),
        "size_bytes": descriptor.get("size_bytes"),
    }


def _compact_receipt_descriptor(descriptor: Mapping[str, Any]) -> dict[str, Any]:
    result = _decoder_receipt_descriptor(descriptor)
    if not all(result.values()):
        raise EurostatBulkBronzeError("Eurostat receipt descriptor is incomplete")
    return result


def _compact_output_descriptor(descriptor: Mapping[str, Any]) -> dict[str, Any]:
    keys = ("id", "name", "sha256", "md5", "size_bytes", "metadata")
    result = {key: deepcopy(descriptor.get(key)) for key in keys}
    if not all(result.get(key) for key in keys[:-1]) or not isinstance(
        result["metadata"], dict
    ):
        raise EurostatBulkBronzeError("Eurostat Bronze output descriptor is incomplete")
    return result


def _require_local_headroom(workdir: Path, receipt: Mapping[str, Any]) -> None:
    raw = receipt.get("raw")
    raw_size = raw.get("size_bytes") if isinstance(raw, dict) else None
    if type(raw_size) is not int or raw_size <= 0:
        raise EurostatBulkBronzeError("Eurostat receipt raw size is invalid")
    required = raw_size * 4 + 1024 * 1024 * 1024
    if shutil.disk_usage(workdir).free < required:
        raise EurostatBulkBronzeError(
            "runner disk headroom is insufficient for this Eurostat distribution"
        )


def _resolve_nested(storage: Any, root_id: str, segments: list[str], *, create: bool) -> str:
    if create:
        return storage.get_or_create_nested_folder(segments, root_id=root_id)
    current = root_id
    for segment in segments:
        folders = storage._list_exact_folders(segment, parent_id=current)
        if len(folders) != 1:
            raise EurostatBulkBronzeError(
                f"Drive path {'/'.join(segments)!r} is absent or ambiguous"
            )
        current = folders[0]["id"]
    return current


def _production_stores(*, allow_write: bool):
    from ingestion.bulk_transport import BulkDriveRawStore
    from ingestion.drive_state_store import DriveStateStore
    from ingestion.source_campaign_store import _CampaignStore
    if allow_write:
        from source_campaign import production_storage
        storage = production_storage(True)
    else:
        from storage_manager import StorageManager
        storage = StorageManager(allow_interactive_auth=False)
        storage.resolve_root(create=False)
    root = storage.resolve_root(create=False)
    landing = storage.resolve_zone("landing", create=False)
    bronze = storage.resolve_zone("bronze", create=allow_write)
    source_control = _resolve_nested(
        storage, root, ["06_control", "source_campaigns", SOURCE_CAMPAIGN_ID],
        create=False,
    )
    source_raw_root = _resolve_nested(
        storage, landing, ["eurostat", "bulk"], create=False
    )
    bronze_control = _resolve_nested(
        storage, root, ["06_control", "source_campaigns", BRONZE_CAMPAIGN_ID],
        create=allow_write,
    )
    output_root = _resolve_nested(
        storage, bronze, ["eurostat", "full_distributions"], create=allow_write
    )
    source_store = _CampaignStore(
        DriveStateStore(storage, root, source_control),
        SOURCE_CAMPAIGN_ID,
        source_control,
        source_raw_root,
    )
    bronze_store = _CampaignStore(
        DriveStateStore(storage, root, bronze_control, allow_landing_pointer=True),
        BRONZE_CAMPAIGN_ID,
        bronze_control,
        output_root,
    )
    source_raw_store = BulkDriveRawStore(
        storage, "eurostat", responses_root_id=source_raw_root
    )
    output_store = BulkDriveRawStore(
        storage, BRONZE_CAMPAIGN_ID, responses_root_id=output_root
    )
    return source_store, source_raw_store, bronze_store, output_store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-production-write", action="store_true")
    parser.add_argument("--verify-current", action="store_true")
    parser.add_argument("--recover-owner")
    parser.add_argument("--recovery-identity")
    parser.add_argument("--max-distributions", type=int, default=64)
    parser.add_argument("--session-seconds", type=int, default=2400)
    args = parser.parse_args(argv)
    if args.verify_current and args.allow_production_write:
        parser.error("--verify-current is read-only and rejects write authorization")
    if args.recover_owner:
        if (
            args.verify_current
            or not args.allow_production_write
            or not args.recovery_identity
        ):
            parser.error(
                "--recover-owner requires write authorization and "
                "--recovery-identity, without --verify-current"
            )
    elif args.recovery_identity:
        parser.error("--recovery-identity requires --recover-owner")
    source_store, source_raw, bronze_store, output_store = _production_stores(
        allow_write=args.allow_production_write
    )
    if args.recover_owner:
        bronze_store.recover_publication_owner(
            args.recover_owner,
            args.recovery_identity,
        )
        report = {
            "status": "eurostat_bulk_bronze_owner_recovered",
            "expected_owner": args.recover_owner,
            "recovery_identity": args.recovery_identity,
        }
    elif args.verify_current:
        report = verify_bronze_checkpoint(source_store, bronze_store, output_store)
    else:
        if not args.allow_production_write:
            parser.error("Bronze publication requires --allow-production-write")
        run_id = os.environ.get("GITHUB_RUN_ID")
        attempt = os.environ.get("GITHUB_RUN_ATTEMPT")
        code_sha = os.environ.get("GITHUB_SHA")
        if not run_id or not attempt or not code_sha:
            raise EurostatBulkBronzeError(
                "GitHub run, attempt, and commit identity are required for publication"
            )
        owner = f"github-run-{run_id}-attempt-{attempt}"
        with tempfile.TemporaryDirectory(prefix="zohelo-eurostat-bronze-") as directory:
            report = run_bronze_batch(
                source_store,
                source_raw,
                bronze_store,
                output_store,
                Path(directory),
                owner=owner,
                code_sha=code_sha,
                max_distributions=args.max_distributions,
                max_seconds=args.session_seconds,
            )
    rendered = json.dumps(report, sort_keys=True)
    print(rendered, flush=True)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as handle:
            handle.write(
                f"## Eurostat full-distribution Bronze\n\n```json\n{rendered}\n```\n"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
