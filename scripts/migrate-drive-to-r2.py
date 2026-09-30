#!/usr/bin/env python3
"""Copy retained Drive files to two R2 buckets, without ingestion or conversion.

01_landing and 05_archive go to the landing bucket; everything else goes to
lakehouse. Ordinary paths stay unchanged. Same-name files get a Drive-ID suffix
where necessary. A private index retains Drive IDs, folders and shortcut metadata.
Only native file bytes are copied; shortcuts are recorded, not followed.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import configparser
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2_migration import CONTROL, FOLDER, ROOT_ID, SHORTCUT, DriveSource, MigrationError, snapshot


def copy_plan(inventory, landing, lakehouse):
    nodes = sorted(inventory["nodes"], key=lambda n: n["id"])
    folder_counts = Counter(n["path"] for n in nodes if n["mimeType"] == FOLDER)
    file_counts = Counter(n["path"] for n in nodes if n["mimeType"] not in (FOLDER, SHORTCUT))
    original_paths = {n["path"] for n in nodes}
    used_keys, objects, folders, aliases = set(), {}, {}, {}
    for node in nodes:
        path = node["path"]
        if not path:
            continue
        if (node["name"] in (".", "..") or "/" in node["name"] or
                any(ord(c) < 32 or ord(c) == 127 for c in path) or
                len(path.encode("utf-8")) > 1024 or
                path.split("/", 1)[0] == "_drive_versions"):
            raise MigrationError("path_requires_explicit_mapping")
        bucket = landing if path.split("/", 1)[0] in ("01_landing", "05_archive") else lakehouse
        address = {"bucket": bucket, "key": path}
        kind = node["mimeType"]
        if kind == SHORTCUT:
            aliases[node["id"]] = dict(address, **node.get("shortcutDetails", {}))
            continue
        if kind == FOLDER:
            # Same-name directories merge; no contained file is discarded.
            folders[node["id"]] = address
            continue
        if kind.startswith("application/vnd.google-apps."):
            raise MigrationError("google_document_is_not_a_native_file")
        md5, size = node.get("md5Checksum", ""), int(node.get("size", -1))
        if not re.fullmatch(r"[0-9a-f]{32}", md5) or size < 0:
            raise MigrationError("missing_file_size_or_checksum")
        key = path
        if key in used_keys or key in folder_counts:
            key = path + "~drive-" + node["id"]
            if key in original_paths or key in used_keys or len(key.encode("utf-8")) > 1024:
                raise MigrationError("duplicate_suffix_requires_explicit_mapping")
        used_keys.add(key)
        parts = path.split("/")
        under_duplicate_folder = any(folder_counts["/".join(parts[:i])] > 1
                                     for i in range(1, len(parts)))
        objects[node["id"]] = dict(address, key=key, size=size, md5=md5,
            copy_by_id=file_counts[path] > 1 or under_duplicate_folder or key != path)
    return {"objects": objects, "folders": folders, "aliases": aliases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--confirm", required=True, choices=["copy-all-zohelo-data-preserve-drive"])
    args = parser.parse_args()
    started = time.monotonic()
    deadline = started + 330 * 60
    summary = {"format_version": 3, "engine": "rclone copy", "result": "starting",
               "drive_writes": False, "portal_cutover": False,
               "source_ingestion": False, "conversion": False,
               "code_sha": os.environ.get("GITHUB_SHA"), "run_id": os.environ.get("GITHUB_RUN_ID")}

    def record(**values):
        summary.update(values, updated_at_utc=datetime.now(timezone.utc).isoformat(),
                       elapsed_seconds=round(time.monotonic() - started, 1))
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(json.dumps(summary, sort_keys=True), flush=True)

    with tempfile.TemporaryDirectory(prefix="zohelo-rclone-") as temp:
        work = Path(temp)
        config_file = work / "rclone.conf"
        private_target = None

        def rclone(*command, cleanup=False):
            remaining = 600 if cleanup else int(deadline - time.monotonic())
            if remaining <= 0:
                raise MigrationError("copy_time_limit_rerun_to_continue")
            # Credentials stay in the private config; progress goes straight to Actions.
            subprocess.run(["rclone", "--config", str(config_file), *map(str, command),
                            "--max-duration", f"{remaining}s", "--cutoff-mode", "SOFT"],
                           timeout=remaining + 60, check=True)

        try:
            names = ("CLOUDFLARE_ACCOUNT_ID", "R2_S3_ENDPOINT", "R2_LANDING_BUCKET",
                     "R2_LAKEHOUSE_BUCKET", "CLOUDFLARE_R2_ACCESS_KEY_ID",
                     "CLOUDFLARE_R2_SECRET_ACCESS_KEY", "GOOGLE_OAUTH_CLIENT_ID",
                     "GOOGLE_OAUTH_CLIENT_SECRET", "GOOGLE_OAUTH_REFRESH_TOKEN")
            env = {key: os.environ.get(key, "").strip() for key in names}
            if not all(env.values()):
                raise MigrationError("required_migration_configuration_missing")
            landing, lakehouse = env["R2_LANDING_BUCKET"], env["R2_LAKEHOUSE_BUCKET"]
            account = env["CLOUDFLARE_ACCOUNT_ID"]
            if (landing != "zohelo-landing-prod" or lakehouse != "zohelo-lakehouse-prod" or
                    not re.fullmatch(r"[0-9a-f]{32}", account) or
                    env["R2_S3_ENDPOINT"].rstrip("/") != f"https://{account}.r2.cloudflarestorage.com"):
                raise MigrationError("unexpected_target_account_or_buckets")
            # rclone's legacy-token fallback drops refresh_token when access_token
            # is empty. Exchange the existing refresh token before writing config.
            credentials = Credentials(
                token=None, token_uri="https://oauth2.googleapis.com/token",
                client_id=env["GOOGLE_OAUTH_CLIENT_ID"],
                client_secret=env["GOOGLE_OAUTH_CLIENT_SECRET"],
                refresh_token=env["GOOGLE_OAUTH_REFRESH_TOKEN"])
            credentials.refresh(Request())
            if not credentials.token or credentials.expiry is None:
                raise MigrationError("google_token_refresh_incomplete")
            config = configparser.ConfigParser(interpolation=None)
            config["drive"] = {"type": "drive", "scope": "drive.readonly",
                "root_folder_id": ROOT_ID, "client_id": env["GOOGLE_OAUTH_CLIENT_ID"],
                "client_secret": env["GOOGLE_OAUTH_CLIENT_SECRET"], "skip_shortcuts": "true",
                "skip_gdocs": "true",
                "token": json.dumps({"access_token": credentials.token, "token_type": "Bearer",
                    "refresh_token": credentials.refresh_token,
                    "expiry": credentials.expiry.replace(tzinfo=timezone.utc).isoformat()})}
            config["r2"] = {"type": "s3", "provider": "Cloudflare", "region": "auto",
                "endpoint": env["R2_S3_ENDPOINT"], "no_check_bucket": "true",
                "access_key_id": env["CLOUDFLARE_R2_ACCESS_KEY_ID"],
                "secret_access_key": env["CLOUDFLARE_R2_SECRET_ACCESS_KEY"],
                "directory_markers": "true"}
            with config_file.open("w") as handle:
                config.write(handle)
            config_file.chmod(0o600)
            run = os.environ.get("GITHUB_RUN_ID", "local") + "-" + work.name
            private_target = f"r2:{lakehouse}/{CONTROL}/rclone/{run}"
            common = ["--checksum", "--fast-list", "--transfers", "24", "--checkers", "16",
                      "--create-empty-src-dirs", "--drive-pacer-min-sleep", "20ms",
                      "--tpslimit", "40", "--stats", "30s", "--stats-one-line",
                      "--stats-log-level", "NOTICE", "--retries", "3",
                      "--filter", "- /_drive_versions/**"]
            destinations = [
                (landing, ["--filter", "+ /01_landing/**", "--filter", "+ /05_archive/**", "--filter", "- **"]),
                (lakehouse, ["--filter", "- /01_landing/**", "--filter", "- /05_archive/**"]),
            ]
            # Both copies finish independently; an error in one does not cancel the other.
            with ThreadPoolExecutor(max_workers=2) as pool:
                copies = [pool.submit(rclone, "copy", "drive:", f"r2:{bucket}", *common,
                            *filters, "--backup-dir", f"r2:{bucket}/_drive_versions/rclone/{run}/bulk")
                          for bucket, filters in destinations]
                record(stage="copy_and_list_source", result="running", parallel_transfers=48)
                last_progress = started

                def progress(**counts):
                    nonlocal last_progress
                    # Do not finish a full inventory after a copy has already failed.
                    for result in copies:
                        if result.done():
                            result.result()
                    if time.monotonic() >= deadline:
                        raise MigrationError("source_listing_time_limit")
                    if time.monotonic() - last_progress >= 30:
                        record(**counts)
                        last_progress = time.monotonic()

                inventory = snapshot(DriveSource(), progress=progress)
                plan = copy_plan(inventory, landing, lakehouse)
                index = work / "inventory-and-map.json.gz"
                with gzip.open(index, "wt", encoding="utf-8") as handle:
                    json.dump({"inventory": inventory, "r2": plan}, handle, ensure_ascii=False)
                rclone("copyto", index, private_target + "/inventory-and-map.json.gz", "--checksum")
                direct = [(identity, address) for identity, address in plan["objects"].items()
                          if address["copy_by_id"]]
                record(stage="copy", source_files=len(plan["objects"]),
                       source_bytes=sum(n["size"] for n in plan["objects"].values()),
                       files_requiring_id_copy=len(direct), evidence_prefix=private_target,
                       parallel_transfers=48)
                for result in copies:
                    result.result()

            # rclone skips ambiguous names. Copy those files by exact Drive ID instead.
            # Checksums skip already-copied matches; differing destination bytes are backed up.
            def copy_by_id(item):
                identity, address = item
                rclone("backend", "copyid", "drive:", identity,
                       f"r2:{address['bucket']}/{address['key']}", "--checksum",
                       "--retries", "3", "--tpslimit", "4", "--filter", "- /_drive_versions/**",
                       "--backup-dir", f"r2:{address['bucket']}/_drive_versions/rclone/{run}/by-id")

            if direct:
                record(stage="copy_duplicate_paths_by_id")
                with ThreadPoolExecutor(max_workers=24) as pool:
                    for count, _ in enumerate(pool.map(copy_by_id, direct), 1):
                        if time.monotonic() - last_progress >= 30:
                            record(id_copies_completed=count)
                            last_progress = time.monotonic()
            parents_with_children = {n.get("parent_id") for n in inventory["nodes"]}
            empty = [a for identity, a in plan["folders"].items() if identity not in parents_with_children]
            if empty:
                record(stage="preserve_empty_folders", empty_folder_count=len(empty))
                with ThreadPoolExecutor(max_workers=8) as pool:
                    for _ in pool.map(lambda a: rclone("mkdir", f"r2:{a['bucket']}/{a['key']}"), empty):
                        pass
            record(result="copy_completed", stage="complete", errors=0,
                   verification="rclone_transfer_checksums; no separate full readback",
                   snapshot_guarantee=False, shortcuts_recorded=len(plan["aliases"]),
                   folder_ids_recorded=len(plan["folders"]))
            rclone("copyto", args.receipt, private_target + "/completed.json", "--checksum", cleanup=True)
            return 0
        except Exception as exc:
            record(result="incomplete", error_category=type(exc).__name__,
                   error_code=str(exc) if isinstance(exc, MigrationError) else "provider_or_runtime_error",
                   rclone_exit_code=getattr(exc, "returncode", None),
                   resume="Run the same copy again; matching files are skipped")
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
