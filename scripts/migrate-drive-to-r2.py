#!/usr/bin/env python3
"""Migrate every retained object in the authorized zohelo-data Drive root to R2.

No source API ingestion, data transformations, Drive mutations, bucket deletion,
public access changes, or production portal cutover occur in this command.
"""
from datetime import datetime, timezone
import argparse
import json
import os
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from r2_migration import DriveSource, MigrationError, make_s3, migrate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--confirm", required=True, choices=["copy-all-zohelo-data-preserve-drive"])
    parser.add_argument("--workers", type=int, choices=range(1, 17), default=16,
                        help="Concurrent bulk transfers (1-16; default 16). Not a throughput guarantee.")
    args = parser.parse_args()
    summary = {"result": "starting", "drive_writes": False, "portal_cutover": False,
               "code_sha": os.environ.get("GITHUB_SHA"), "run_id": os.environ.get("GITHUB_RUN_ID")}
    started = time.monotonic()
    last_log = 0.0

    def record(update):
        nonlocal last_log
        summary.update(update, updated_at_utc=datetime.now(timezone.utc).isoformat(),
                       elapsed_seconds=round(time.monotonic() - started, 1))
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        now = time.monotonic()
        if update.get("stage") != "inventory" or now - last_log >= 15:
            print(json.dumps(summary, sort_keys=True), flush=True)
            last_log = now

    try:
        names = ("R2_S3_ENDPOINT", "R2_LANDING_BUCKET", "R2_LAKEHOUSE_BUCKET",
                 "CLOUDFLARE_R2_ACCESS_KEY_ID", "CLOUDFLARE_R2_SECRET_ACCESS_KEY",
                 "GOOGLE_OAUTH_CLIENT_ID", "GOOGLE_OAUTH_CLIENT_SECRET", "GOOGLE_OAUTH_REFRESH_TOKEN")
        config = {name: os.environ.get(name, "").strip() for name in names}
        if any(not value for value in config.values()):
            raise MigrationError("required_migration_configuration_missing")
        if (config["R2_LANDING_BUCKET"] != "zohelo-landing-prod" or
                config["R2_LAKEHOUSE_BUCKET"] != "zohelo-lakehouse-prod"):
            raise MigrationError("unexpected_target_buckets")
        account = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "").strip()
        if (not re.fullmatch(r"[0-9a-f]{32}", account) or
                config["R2_S3_ENDPOINT"].rstrip("/") != f"https://{account}.r2.cloudflarestorage.com"):
            raise MigrationError("target_account_mismatch")
        record({"stage": "configuration", "portal_deploy_token_configured": bool(os.environ.get("CLOUDFLARE_API_TOKEN"))})
        result = migrate(
            DriveSource,
            lambda: make_s3(config["R2_S3_ENDPOINT"], config["CLOUDFLARE_R2_ACCESS_KEY_ID"],
                            config["CLOUDFLARE_R2_SECRET_ACCESS_KEY"]),
            config["R2_LANDING_BUCKET"], config["R2_LAKEHOUSE_BUCKET"],
            workers=args.workers, seconds=5 * 3600, max_bytes=400 * 1024**3, progress=record,
        )
        record(result)
        return 0 if result["result"] == "copy_verified" else 2
    except Exception as exc:
        record({"result": "failed", "category": type(exc).__name__,
                "error_code": str(exc) if isinstance(exc, MigrationError) else "provider_or_runtime_error"})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
