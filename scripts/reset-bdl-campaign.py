#!/usr/bin/env python3
"""Safe administrative script to wipe legacy GUS BDL campaign state on Google Drive.

Confined strictly to:
- 01_landing/gus_bdl/
- 06_control/source_campaigns/gus_bdl/
- bdl-platform/

All other sources (world_bank_wdi, eurostat, opendata_org, nbp_platform) are strictly untouched.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from storage_manager import StorageManager


def _delete_folder_children(sm: StorageManager, folder_id: str, label: str) -> int:
    query = f"'{folder_id}' in parents and trashed=false"
    response = sm.drive_service.files().list(
        q=query,
        spaces="drive",
        fields="nextPageToken, files(id, name, mimeType)",
    ).execute()
    files = response.get("files", [])
    count = 0
    for file_info in files:
        file_id = file_info["id"]
        name = file_info["name"]
        print(f"[{label}] Deleting {name} ({file_id})...")
        sm.drive_service.files().delete(fileId=file_id).execute()
        count += 1
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Wipe legacy GUS BDL campaign state on Google Drive")
    parser.add_argument(
        "--allow-production-write",
        action="store_true",
        help="Explicitly authorize mutating the production Drive root",
    )
    args = parser.parse_args()

    if not args.allow_production_write:
        parser.error("--allow-production-write is required to wipe BDL data on Drive")

    os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"
    sm = StorageManager(backend="gdrive")
    root_id = sm.resolve_root()
    print(f"Connected to Google Drive root: {sm.root_name} ({root_id})")

    landing_id = sm.resolve_zone("landing")
    landing_bdl_folders = sm._list_exact_folders("gus_bdl", parent_id=landing_id)
    if not landing_bdl_folders:
        raise RuntimeError("01_landing/gus_bdl folder not found")
    landing_bdl_id = landing_bdl_folders[0]["id"]

    root_children = sm.drive_service.files().list(
        q=f"'{root_id}' in parents and trashed=false",
        spaces="drive",
        fields="files(id, name, mimeType)",
    ).execute().get("files", [])

    control_folders = [f for f in root_children if f["name"] == "06_control"]
    if not control_folders:
        raise RuntimeError("06_control folder not found")
    control_id = control_folders[0]["id"]

    sc_folders = sm._list_exact_folders("source_campaigns", parent_id=control_id)
    if not sc_folders:
        raise RuntimeError("06_control/source_campaigns folder not found")
    sc_id = sc_folders[0]["id"]

    bdl_ctrl_folders = sm._list_exact_folders("gus_bdl", parent_id=sc_id)
    if not bdl_ctrl_folders:
        raise RuntimeError("06_control/source_campaigns/gus_bdl folder not found")
    bdl_ctrl_id = bdl_ctrl_folders[0]["id"]

    bdl_plat_folders = sm._list_exact_folders("bdl-platform", parent_id=root_id)
    bdl_plat_id = bdl_plat_folders[0]["id"] if bdl_plat_folders else None

    print("\n--- BDL TARGET DIRECTORIES TO WIPE ---")
    print(f"1. Landing:  01_landing/gus_bdl              ({landing_bdl_id})")
    print(f"2. Control:  06_control/source_campaigns/gus_bdl ({bdl_ctrl_id})")
    if bdl_plat_id:
        print(f"3. Platform: bdl-platform                   ({bdl_plat_id})")
    print("--------------------------------------\n")

    c1 = _delete_folder_children(sm, landing_bdl_id, "01_landing/gus_bdl")
    c2 = _delete_folder_children(sm, bdl_ctrl_id, "06_control/source_campaigns/gus_bdl")
    c3 = 0
    if bdl_plat_id:
        c3 = _delete_folder_children(sm, bdl_plat_id, "bdl-platform")

    print(f"\nSuccessfully wiped GUS BDL on Google Drive:")
    print(f"  - Landing objects deleted:  {c1}")
    print(f"  - Control objects deleted:  {c2}")
    print(f"  - Platform objects deleted: {c3}")
    print(f"Total objects deleted: {c1 + c2 + c3}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
