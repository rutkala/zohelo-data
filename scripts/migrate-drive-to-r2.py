#!/usr/bin/env python3
"""Copy the retained project with rclone; no conversion, Drive writes or cutover.

Two ordinary folder copies do the transfer. Existing inventory code is reused
only to retain Drive IDs/shortcuts and to check that no source file was missed.
Run with the existing Drive/R2 Actions credentials and an installed rclone.
"""
import argparse
import configparser
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2_migration import (CONTROL, FOLDER, ROOT_ID, SHORTCUT, DriveSource,
                          MigrationError, inventory_summary, make_s3, snapshot)


def copy_plan(inventory, landing, lakehouse):
    """Preserve native files, folder metadata and internal aliases, not exports."""
    nodes = {n["id"]: n for n in inventory["nodes"]}
    paths, objects, folders, aliases = set(), {}, {}, {}
    for node in nodes.values():
        path = node["path"]
        if not path:
            continue
        if (node["name"] in (".", "..") or "/" in node["name"] or
                any(ord(c) < 32 or ord(c) == 127 for c in path) or
                len(path.encode("utf-8")) > 1024 or
                path.split("/", 1)[0] == "_drive_versions"):
            raise MigrationError("path_requires_explicit_mapping")
        kind = node["mimeType"]
        bucket = landing if path.split("/", 1)[0] == "01_landing" else lakehouse
        address = {"bucket": bucket, "key": path}
        if kind == SHORTCUT:
            target = node.get("shortcutDetails", {}).get("targetId")
            if target not in nodes:
                raise MigrationError("shortcut_target_outside_project")
            aliases[node["id"]] = dict(address, target_id=target)
            continue
        if path in paths:
            raise MigrationError("duplicate_drive_path_requires_mapping")
        paths.add(path)
        if kind == FOLDER:
            folders[node["id"]] = address
        else:
            md5, size = node.get("md5Checksum", ""), int(node.get("size", -1))
            if kind.startswith("application/vnd.google-apps."):
                raise MigrationError("google_document_is_not_a_native_file")
            if not re.fullmatch(r"[0-9a-f]{32}", md5) or size < 0:
                raise MigrationError("missing_file_size_or_checksum")
            objects[node["id"]] = dict(address, size=size, md5=md5)
    if sum(n["size"] for n in objects.values()) > 400 * 1024**3:
        raise MigrationError("inventory_exceeds_authorized_400_gib")
    return {"objects": objects, "folders": folders, "aliases": aliases}


def compare_objects(plan, listings, download_hash):
    """One size/hash reconciliation; download only objects lacking an R2 MD5."""
    for address in plan["folders"].values():
        row = listings[address["bucket"]].get(address["key"])
        if not row or not row.get("IsDir"):
            raise MigrationError("destination_folder_missing")
    for address in plan["objects"].values():
        row = listings[address["bucket"]].get(address["key"])
        if not row or row.get("IsDir") or row.get("Size") != address["size"]:
            raise MigrationError("destination_file_missing_or_wrong_size")
        md5 = row.get("Hashes", {}).get("MD5")
        if not md5:
            md5 = download_hash(address)
        if md5.lower() != address["md5"]:
            raise MigrationError("destination_checksum_mismatch")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--confirm", required=True,
                        choices=["copy-all-zohelo-data-preserve-drive"])
    args = parser.parse_args()
    started = time.monotonic()
    # Leave the 355-minute Actions job time for verification/receipt cleanup.
    deadline = started + 330 * 60
    summary = {"format_version": 2, "engine": "rclone copy", "result": "starting",
               "drive_writes": False, "portal_cutover": False,
               "iceberg_registration": "separate_pending_stage",
               "code_sha": os.environ.get("GITHUB_SHA"),
               "run_id": os.environ.get("GITHUB_RUN_ID")}

    def record(**values):
        summary.update(values, updated_at_utc=datetime.now(timezone.utc).isoformat(),
                       elapsed_seconds=round(time.monotonic() - started, 1))
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        print(json.dumps(summary, sort_keys=True), flush=True)

    with tempfile.TemporaryDirectory(prefix="zohelo-rclone-") as temp:
        work = Path(temp)
        evidence = work / "evidence"
        evidence.mkdir()
        config_file = work / "rclone.conf"
        private_target = None

        def rclone(*command, output=None, cleanup=False):
            timeout = 600 if cleanup else max(1, int(deadline - time.monotonic()))
            log_path = work / "private-publish.log" if cleanup else evidence / "rclone.log"
            with log_path.open("a") as log:
                kwargs = {"stdout": log, "stderr": log, "timeout": timeout, "check": True}
                cmd = ["rclone", "--config", str(config_file), *map(str, command)]
                if output is None:
                    subprocess.run(cmd, **kwargs)
                else:
                    with Path(output).open("w") as out:
                        kwargs["stdout"] = out
                        subprocess.run(cmd, **kwargs)

        try:
            env_names = ("CLOUDFLARE_ACCOUNT_ID", "R2_S3_ENDPOINT", "R2_LANDING_BUCKET",
                         "R2_LAKEHOUSE_BUCKET", "CLOUDFLARE_R2_ACCESS_KEY_ID",
                         "CLOUDFLARE_R2_SECRET_ACCESS_KEY", "GOOGLE_OAUTH_CLIENT_ID",
                         "GOOGLE_OAUTH_CLIENT_SECRET", "GOOGLE_OAUTH_REFRESH_TOKEN")
            env = {key: os.environ.get(key, "").strip() for key in env_names}
            if not all(env.values()):
                raise MigrationError("required_migration_configuration_missing")
            landing, lakehouse = env["R2_LANDING_BUCKET"], env["R2_LAKEHOUSE_BUCKET"]
            account = env["CLOUDFLARE_ACCOUNT_ID"]
            if (landing != "zohelo-landing-prod" or lakehouse != "zohelo-lakehouse-prod" or
                    not re.fullmatch(r"[0-9a-f]{32}", account) or
                    env["R2_S3_ENDPOINT"].rstrip("/") != f"https://{account}.r2.cloudflarestorage.com"):
                raise MigrationError("unexpected_target_account_or_buckets")
            config = configparser.ConfigParser(interpolation=None)
            # Obscure via stdin, never a credential in command arguments or logs.
            secret = subprocess.check_output(["rclone", "obscure", "-"],
                input=env["GOOGLE_OAUTH_CLIENT_SECRET"], text=True).strip()
            config["drive"] = {"type": "drive", "scope": "drive.readonly",
                "root_folder_id": ROOT_ID, "client_id": env["GOOGLE_OAUTH_CLIENT_ID"],
                "client_secret": secret, "skip_shortcuts": "true",
                "token": json.dumps({"access_token": "", "token_type": "Bearer",
                    "refresh_token": env["GOOGLE_OAUTH_REFRESH_TOKEN"],
                    "expiry": "2000-01-01T00:00:00Z"})}
            config["r2"] = {"type": "s3", "provider": "Cloudflare", "region": "auto",
                "endpoint": env["R2_S3_ENDPOINT"], "no_check_bucket": "true",
                "access_key_id": env["CLOUDFLARE_R2_ACCESS_KEY_ID"],
                "secret_access_key": env["CLOUDFLARE_R2_SECRET_ACCESS_KEY"],
                "directory_markers": "true"}
            with config_file.open("w") as handle:
                config.write(handle)
            config_file.chmod(0o600)
            run = (os.environ.get("GITHUB_RUN_ID", "local") + "-" +
                   os.environ.get("GITHUB_RUN_ATTEMPT", "1") + "-" + work.name)
            private_target = f"r2:{lakehouse}/{CONTROL}/rclone/{run}"
            record(stage="inventory", result="running")
            source = DriveSource()
            before = snapshot(source)
            plan = copy_plan(before, landing, lakehouse)
            with gzip.open(evidence / "inventory-and-map.json.gz", "wt", encoding="utf-8") as handle:
                json.dump({"inventory": before, "r2": plan}, handle, ensure_ascii=False)
            record(layers=inventory_summary(before),
                   total_files=len(plan["objects"]),
                   total_bytes=sum(n["size"] for n in plan["objects"].values()),
                   inventory_sha256=before["inventory_sha256"], stage="copy")
            common = ["--checksum", "--fast-list", "--transfers", "24", "--checkers", "32",
                      "--create-empty-src-dirs", "--drive-pacer-min-sleep", "20ms",
                      "--tpslimit", "80", "--stats", "1m", "--stats-one-line",
                      "--retries", "3", "--exclude", "/_drive_versions/**"]
            # Copy only: preserve destination-only files and back up changed R2 files.
            rclone("copy", "drive:01_landing", f"r2:{landing}/01_landing", *common,
                   "--backup-dir", f"r2:{landing}/_drive_versions/rclone/{run}")
            rclone("copy", "drive:", f"r2:{lakehouse}", *common,
                   "--exclude", "/01_landing/**",
                   "--backup-dir", f"r2:{lakehouse}/_drive_versions/rclone/{run}")
            record(stage="verify_all_files")
            listings = {}
            for bucket in (landing, lakehouse):
                listing_file = evidence / f"{bucket}.json"
                rclone("lsjson", f"r2:{bucket}", "--recursive", "--hash-type", "MD5",
                       "--no-modtime", "--no-mimetype", output=listing_file)
                listings[bucket] = {n["Path"]: n for n in json.loads(listing_file.read_text())}
            s3 = make_s3(env["R2_S3_ENDPOINT"], env["CLOUDFLARE_R2_ACCESS_KEY_ID"],
                         env["CLOUDFLARE_R2_SECRET_ACCESS_KEY"])

            def download_hash(address):
                checksum, size = hashlib.md5(), 0
                with s3.get_object(Bucket=address["bucket"], Key=address["key"])["Body"] as body:
                    for block in body.iter_chunks(1024 * 1024):
                        if time.monotonic() > deadline:
                            raise MigrationError("verification_time_limit")
                        checksum.update(block)
                        size += len(block)
                if size != address["size"]:
                    raise MigrationError("readback_size_mismatch")
                return checksum.hexdigest()

            compare_objects(plan, listings, download_hash)
            record(stage="final_source_reconciliation")
            after = snapshot(source)
            if before["inventory_sha256"] != after["inventory_sha256"]:
                raise MigrationError("source_changed_repeat_delta_copy")
            # One compact private index, not one new object per source file.
            rclone("copy", evidence, private_target, "--checksum", cleanup=True)
            record(result="copy_verified", stage="complete", errors=0,
                   verified_files=len(plan["objects"]),
                   verified_bytes=sum(n["size"] for n in plan["objects"].values()),
                   folders_preserved=len(plan["folders"]), aliases_preserved=len(plan["aliases"]),
                   evidence_prefix=private_target, immutable_reference_retained=True)
            rclone("copyto", args.receipt, private_target + "/verified.json", "--checksum", cleanup=True)
            return 0
        except Exception as exc:
            record(result="failed", error_category=type(exc).__name__,
                   error_code=str(exc) if isinstance(exc, MigrationError) else "provider_or_runtime_error",
                   rclone_exit_code=getattr(exc, "returncode", None))
            if private_target:
                try:
                    # No credentials are in this directory; errors stay in private R2.
                    rclone("copy", evidence, private_target, "--checksum", cleanup=True)
                except Exception:
                    record(private_evidence_upload_failed=True)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
