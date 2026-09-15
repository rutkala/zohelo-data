"""Resumable BDL Web-only discovery and extraction, independent of API campaigns.

A provider failure remains outstanding; it never becomes a completion marker and
never prevents other subgroups from being collected. Drive/upload errors stop the
writer, because an ambiguous write must not be retried as a provider failure.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PORTAL = ROOT / "portal"
WEB = "https://bdl.stat.gov.pl/bdl"
SEED_SHA256 = "8b4162d088bac8d3901d18c581d85cf9a0a0fcdc6c8ef3c4b9aa4a9ed969d248"
MAX_CONTROL_BYTES = 8 * 1024 * 1024


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def rendered(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def candidate_valid(item: dict) -> bool:
    keys = ("category_id", "group_id", "subgroup_id")
    if not isinstance(item, dict) or not all(
        isinstance(item.get(key), str) and re.fullmatch(prefix + r"[0-9]+", item[key])
        for key, prefix in zip(keys, "KGP")
    ):
        return False
    return item.get("url") == WEB + "/dane/podgrup/wymiary/" + "/".join(item[k][1:] for k in keys)


def new_state(seed_path: Path | None = None) -> dict:
    state = {
        "format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
        "started_at_utc": now(), "candidates": {}, "failures": {},
        "discovery_pending": [{"kind": "categories", "url": WEB + "/metadane/kategorie"}],
        "discovery_completed": {}, "catalogue_errors": {},
    }
    if seed_path and seed_path.is_file():
        raw = seed_path.read_bytes()
        if sha256(raw).hexdigest() != SEED_SHA256:
            raise ValueError("BDL recovery index does not match the inspected run-7 artifact")
        seed = json.loads(raw)
        for item in seed["remaining_candidates"]:
            if not candidate_valid(item):
                raise ValueError("Invalid candidate in BDL recovery index")
            state["candidates"][item["subgroup_id"]] = {**item, "web_catalogue_verified": False}
        state["recovery_seed"] = {"run_id": 34938114306, "sha256": SEED_SHA256, "complete": False}
        # This is a known failed request, not a skipped or completed subgroup.
        state["failures"]["P1313"] = {"attempts": 1, "error": "BDL result-table server error in run 34938114306"}
    return state


def apply_discovery(state: dict, task: dict, result: dict) -> dict:
    """Accept a fully paged Web table, then queue its children depth-first."""
    if result.get("url") != task["url"] or result.get("complete") is not True:
        raise ValueError("Web catalogue task did not prove table exhaustion")
    records = result.get("records")
    if not isinstance(records, list) or len(records) != result.get("expected_count") or not records:
        raise ValueError("Web catalogue row count is missing, empty or inconsistent")
    prefix = {"categories": "K", "groups": "G", "subgroups": "P"}[task["kind"]]
    ids = [r.get("id") for r in records if isinstance(r, dict)]
    if len(ids) != len(records) or len(set(ids)) != len(ids) or not all(
        isinstance(i, str) and re.fullmatch(prefix + r"[0-9]+", i) for i in ids
    ):
        raise ValueError("Web catalogue identities are invalid or duplicated")
    updated = deepcopy(state)
    children = []
    for record in records:
        identifier = record["id"]
        if task["kind"] == "categories":
            children.append({"kind": "groups", "category_id": identifier,
                             "url": WEB + "/metadane/grupy/" + identifier[1:]})
        elif task["kind"] == "groups":
            children.append({"kind": "subgroups", "category_id": task["category_id"],
                             "group_id": identifier, "url": WEB + "/metadane/podgrupy/" + identifier[1:]})
        else:
            cat, group = task["category_id"], task["group_id"]
            item = {"category_id": cat, "group_id": group, "subgroup_id": identifier,
                    "subgroup_name": record["name"], "path": [cat, group, identifier],
                    "url": WEB + f"/dane/podgrup/wymiary/{cat[1:]}/{group[1:]}/{identifier[1:]}",
                    "web_catalogue_verified": True, "web_metadata": record}
            previous = updated["candidates"].get(identifier)
            if previous and previous["url"] != item["url"]:
                raise ValueError("BDL subgroup changed hierarchy; explicit reconciliation required")
            updated["candidates"][identifier] = item
    updated["discovery_completed"][task["url"]] = {
        "kind": task["kind"], "row_count": len(records), "pages": result["pages"],
        "verified_at_utc": now(), "records_sha256": sha256(rendered(records)).hexdigest(),
    }
    pending = [t for t in updated["discovery_pending"] if t["url"] != task["url"]]
    existing = {t["url"] for t in pending} | set(updated["discovery_completed"])
    updated["discovery_pending"] = [t for t in children if t["url"] not in existing] + pending
    updated["catalogue_errors"].pop(task["url"], None)
    return updated


def next_candidate(state: dict, landed: set[str], attempted: set[str]) -> dict | None:
    eligible = [c for key, c in state["candidates"].items() if key not in landed | attempted]
    return min(eligible, key=lambda c: (
        state["failures"].get(c["subgroup_id"], {}).get("attempts", 0),
        int(c["subgroup_id"][1:]),
    ), default=None)


def progress(state: dict, landed: set[str], completed: int, rows: int) -> dict:
    known = set(state["candidates"])
    missing = known - landed
    unverified = [key for key, item in state["candidates"].items() if not item.get("web_catalogue_verified")]
    exhausted = not state["discovery_pending"] and bool(state["discovery_completed"]) and not unverified
    complete = exhausted and bool(known) and not missing and not state["catalogue_errors"]
    return {"status": "complete" if complete else "incomplete", "updated_at_utc": now(),
            "catalogue_exhausted": exhausted, "known_subgroups": len(known),
            "landed_known_subgroups": len(known & landed), "landed_total_subgroups": len(landed),
            "remaining_known_subgroups": len(missing), "unverified_index_subgroups": len(unverified),
            "pending_catalogue_tables": len(state["discovery_pending"]),
            "failed_subgroups": sorted(key for key in state["failures"] if key not in landed),
            "catalogue_errors": state["catalogue_errors"],
            "completed_this_run": completed, "rows_landed_this_run": rows}


class DriveControl:
    """One BDL-owned small checkpoint, with exact read-back and drift detection."""
    def __init__(self, storage: Any, parent: str, name: str):
        from bdl_bulk_ingest import _find_exact_file
        self.storage, self.parent, self.name = storage, parent, name
        matches = _find_exact_file(storage, name, parent)
        if len(matches) > 1:
            raise RuntimeError("Ambiguous BDL Web checkpoint")
        self.file_id = matches[0]["id"] if matches else None
        self.observed = self._read() if self.file_id else None
        if self.observed is not None and len(self.observed) > MAX_CONTROL_BYTES:
            raise RuntimeError("BDL checkpoint exceeds its bound")
        if matches and (matches[0].get("appProperties") or {}).get("sha256") != sha256(self.observed).hexdigest():
            raise RuntimeError("BDL checkpoint content does not match its recorded hash")

    def _read(self) -> bytes:
        return self.storage.drive_service.files().get_media(fileId=self.file_id).execute(num_retries=4)

    def load(self) -> dict | None:
        if self.observed is None:
            return None
        value = json.loads(self.observed)
        if value.get("source_id") != "gus_bdl" or value.get("format_version") != 1 or value.get("transport") != "web_ui":
            raise RuntimeError("Unexpected BDL Web checkpoint identity")
        if not all(candidate_valid(c) for c in value.get("candidates", {}).values()):
            raise RuntimeError("Invalid persisted BDL candidate")
        return value

    def save(self, value: dict) -> None:
        from googleapiclient.http import MediaInMemoryUpload
        raw = rendered(value)
        if len(raw) > MAX_CONTROL_BYTES:
            raise RuntimeError("BDL Web checkpoint exceeds its bound")
        files = self.storage.drive_service.files()
        props = {"source_id": "gus_bdl", "transport": "web_ui", "sha256": sha256(raw).hexdigest()}
        media = MediaInMemoryUpload(raw, mimetype="application/json", resumable=False)
        if self.file_id:
            if self._read() != self.observed:
                raise RuntimeError("BDL Web checkpoint changed outside the serialized writer")
            files.update(fileId=self.file_id, body={"appProperties": props}, media_body=media).execute(num_retries=0)
        else:
            answer = files.create(body={"name": self.name, "parents": [self.parent], "appProperties": props},
                                  media_body=media, fields="id").execute(num_retries=0)
            self.file_id = answer["id"]
        if self._read() != raw:
            raise RuntimeError("BDL Web checkpoint did not verify after publication")
        self.observed = raw


def invoke(script: str, env: dict, timeout: int, result_path: Path) -> dict:
    result_path.unlink(missing_ok=True)
    process = subprocess.run(["node", str(PORTAL / "scripts" / script)], cwd=PORTAL,
                             env=env, timeout=timeout, capture_output=True, text=True)
    value = json.loads(result_path.read_text()) if result_path.is_file() else {}
    if process.returncode:
        # The worker redacts its credentials; do not print raw subprocess environment/output.
        raise RuntimeError(str(value.get("error") or f"{script} exited {process.returncode}")[:1600])
    return value


def run(workspace: Path, max_seconds: int, seed: Path | None = None) -> dict:
    from bdl_bulk_ingest import ingest_archive, _hash_file
    from bdl_bulk_plan import _bulk_roots, _durable_status
    from bdl_web_bootstrap import _require_production_context, _clean_ephemeral
    from storage_manager import StorageManager
    _require_production_context()
    if max_seconds < 300:
        raise ValueError("Runtime must be at least 300 seconds")
    workspace = workspace.resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    storage = StorageManager(allow_interactive_auth=False)
    storage.resolve_root(create=False)
    bulk, control = _bulk_roots(storage)
    store = DriveControl(storage, control, "web-queue-v1.json")
    state = store.load() or new_state(seed)
    store.save(state)
    landed, _ = _durable_status(storage, bulk, control)  # Non-landed legacy markers are NOT completion.
    attempted: set[str] = set()
    discovery_attempted: set[str] = set()
    completed = rows = 0
    start = time.monotonic()

    def report() -> dict:
        value = progress(state, landed, completed, rows)
        (workspace / "bootstrap-summary.json").write_bytes(rendered(value))
        print(json.dumps(value, ensure_ascii=False), flush=True)
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_path:
            Path(summary_path).write_text(
                "# BDL Web historical ingestion\n\n"
                f"Campaign: **{value['status']}**. New verified subgroups this run: **{completed}**; rows: **{rows:,}**.\n\n"
                f"Known subgroups: {value['known_subgroups']}; landed: {value['landed_known_subgroups']}; "
                f"outstanding: {value['remaining_known_subgroups']}. Web catalogue tables still pending: "
                f"{value['pending_catalogue_tables']}. Catalogue exhausted: {value['catalogue_exhausted']}.\n\n"
                f"Failed and still pending: {', '.join(value['failed_subgroups']) or 'none'}.\n\n"
                "No API observations or medallion transformations are run. A checkpoint is not full-source acceptance.\n"
            )
        return value

    try:
        report()
        while max_seconds - (time.monotonic() - start) >= 300:
            task = next((t for t in state["discovery_pending"] if t["url"] not in discovery_attempted), None)
            if task:
                discovery_attempted.add(task["url"])
                env = {k: v for k, v in os.environ.items() if not any(x in k for x in ("TOKEN", "SECRET", "PASSWORD", "API_KEY"))}
                env.update(BDL_CATALOGUE_TASK=json.dumps(task), BDL_CATALOGUE_OUTPUT=str(workspace / "catalogue-task.json"))
                print(json.dumps({"status": "web_catalogue_table_started", "url": task["url"]}), flush=True)
                try:
                    result = invoke("bdl-web-catalogue.mjs", env, min(240, int(max_seconds - (time.monotonic() - start))), workspace / "catalogue-task.json")
                    state = apply_discovery(state, task, result)
                except (RuntimeError, subprocess.TimeoutExpired, ValueError, OSError) as exc:
                    state["catalogue_errors"][task["url"]] = str(exc)[:1600]
                store.save(state)
                report()
            item = next_candidate(state, landed, attempted)
            if item is None:
                if task:
                    continue
                break
            subgroup = item["subgroup_id"]
            attempted.add(subgroup)
            _clean_ephemeral(workspace)
            env = dict(os.environ)
            env.update(BDL_BULK_SUBGROUP_ID=subgroup, BDL_BULK_SUBGROUP_NAME=item.get("subgroup_name") or subgroup,
                       BDL_BULK_URL=item["url"], BDL_BULK_OUT_DIR=str(workspace))
            print(json.dumps({"status": "bdl_web_subgroup_started", "subgroup_id": subgroup}), flush=True)
            try:
                result = invoke("bdl-web-bulk-worker.mjs", env, min(900, int(max_seconds - (time.monotonic() - start))), workspace / "worker-result.json")
                if result.get("subgroupId") != subgroup or result.get("status") not in {"downloaded_relational_export", "downloaded_generated_export"}:
                    raise RuntimeError("BDL Web export was not complete for the requested subgroup")
                archives = list(workspace.glob("download-*.zip"))
                if len(archives) != 1 or _hash_file(archives[0], "sha256") != (result.get("archive") or {}).get("sha256"):
                    raise RuntimeError("BDL Web archive identity did not verify")
            except (RuntimeError, subprocess.TimeoutExpired, ValueError, OSError) as exc:
                previous = state["failures"].get(subgroup, {})
                state["failures"][subgroup] = {"attempts": previous.get("attempts", 0) + 1,
                                               "last_attempt_utc": now(), "error": str(exc)[:1600]}
                (workspace / f"failure-{subgroup}.json").write_bytes(rendered(state["failures"][subgroup]))
                store.save(state)
                print(json.dumps({"status": "bdl_web_subgroup_pending_failure", "subgroup_id": subgroup,
                                  "error": str(exc)[:1600]}), flush=True)
                report()
                continue
            # Do not swallow Drive, conversion or ambiguous checkpoint errors.
            receipt = ingest_archive(archives[0], subgroup, True)
            if receipt.get("status") != "bdl_web_bulk_landed":
                raise RuntimeError("BDL Landing upload did not verify")
            (workspace / f"landed-{subgroup}.json").write_bytes(rendered(receipt))
            landed.add(subgroup)
            completed += 1
            rows += int(receipt["row_count"])
            state["failures"].pop(subgroup, None)
            store.save(state)
            print(json.dumps({"status": "bdl_web_subgroup_landed", "subgroup_id": subgroup,
                              "row_count": receipt["row_count"], "completed_this_run": completed}), flush=True)
            report()
        return report()
    finally:
        report()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--max-seconds", type=int, default=18600)
    parser.add_argument("--seed", type=Path)
    args = parser.parse_args()
    value = run(args.workspace, args.max_seconds, args.seed)
    # Explicitly signal a stalled attempt, not a green no-op ingestion run.
    return 1 if value["status"] != "complete" and value["completed_this_run"] == 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
