#!/usr/bin/env python3
"""Stream the full OpenData.org Senzing zip archive directly from GCS to Google Drive.

Uses HTTP Range requests as a seekable stream into Google Drive's resumable upload API,
transferring the full 21.38 GB archive with ZERO local disk usage and a bounded memory footprint.
"""

import argparse
import io
import os
import sys
import time
import urllib.request
from googleapiclient.http import MediaIoBaseUpload

from src.storage_manager import StorageManager

OPENDATA_URL = "https://storage.googleapis.com/bq-public-datastore/opendata/ODO-SENZING-20260305/full/ODO_SENZING_20260305.zip"
ARCHIVE_FILENAME = "ODO_SENZING_20260305.zip"
TOTAL_SIZE_BYTES = 21378254323  # 21.38 GB
CHUNK_SIZE_BYTES = 32 * 1024 * 1024  # 32 MiB Drive upload chunks


class RemoteSeekableStream(io.RawIOBase):
    """Seekable stream that retrieves requested byte chunks over HTTP Range requests."""

    def __init__(self, url: str, total_length: int):
        self.url = url
        self.total_length = total_length
        self.pos = 0

    def seekable(self) -> bool:
        return True

    def readable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.total_length + offset
        else:
            raise ValueError(f"Invalid whence: {whence}")
        return self.pos

    def tell(self) -> int:
        return self.pos

    def readinto(self, b) -> int:
        if self.pos >= self.total_length:
            return 0
        end = min(self.pos + len(b) - 1, self.total_length - 1)
        req = urllib.request.Request(self.url, headers={"Range": f"bytes={self.pos}-{end}"})
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = resp.read()
                    b[:len(data)] = data
                    self.pos += len(data)
                    return len(data)
            except Exception as err:
                if attempt == 4:
                    raise
                time.sleep(2 ** attempt)
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-production-write", action="store_true", help="Authorize writes to zohelo-data Drive root")
    args = parser.parse_args()

    if args.allow_production_write:
        os.environ["ZOHELO_ALLOW_PRODUCTION_WRITES"] = "true"

    sm = StorageManager(allow_interactive_auth=False)
    if args.allow_production_write:
        sm.authorize_writes()

    root_id = sm.resolve_root(create=False)
    print(f"Resolved Drive Root ID: {root_id}")

    # Target folder: 01_landing / opendata_org
    landing_root = sm.resolve_zone("landing", create=True)
    target_folder_id = sm.get_or_create_nested_folder(["opendata_org"], root_id=landing_root)
    print(f"Target Drive folder (01_landing/opendata_org): {target_folder_id}")

    # Check if file already exists in target folder
    svc = sm.drive_service
    q = f"'{target_folder_id}' in parents and name = '{ARCHIVE_FILENAME}' and trashed = false"
    existing = svc.files().list(q=q, fields="files(id, name, size)").execute().get("files", [])
    if existing:
        f = existing[0]
        print(f"Archive already exists in Landing: ID={f['id']}, Size={int(f.get('size', 0))/(1024**3):.2f} GB")
        return 0

    print(f"Starting cloud-to-cloud streaming transfer into Landing layer:")
    print(f"  • Source: GCS ({OPENDATA_URL})")
    print(f"  • Total size: {TOTAL_SIZE_BYTES / (1024**3):.2f} GB ({TOTAL_SIZE_BYTES:,} bytes)")
    print(f"  • Destination: Google Drive 01_landing/opendata_org/{ARCHIVE_FILENAME}")
    print(f"  • Local disk used: 0 bytes (in-memory streaming)")

    stream = RemoteSeekableStream(OPENDATA_URL, TOTAL_SIZE_BYTES)
    media = MediaIoBaseUpload(
        io.BufferedReader(stream, buffer_size=CHUNK_SIZE_BYTES),
        mimetype="application/zip",
        chunksize=CHUNK_SIZE_BYTES,
        resumable=True
    )

    file_metadata = {
        "name": ARCHIVE_FILENAME,
        "parents": [target_folder_id],
        "description": "Full OpenData.org Senzing entity resolution native archive (2026-03-05 snapshot)"
    }

    request = svc.files().create(body=file_metadata, media_body=media, fields="id, name, size")

    start_time = time.time()
    response = None
    while response is None:
        try:
            status, response = request.next_chunk(num_retries=5)
        except Exception as e:
            print(f"Transient error during chunk upload: {e}, retrying in 5s...", flush=True)
            time.sleep(5)
            continue

        if status:
            progress = status.progress() * 100
            uploaded_mb = (status.resumable_progress or 0) / (1024 ** 2)
            total_mb = TOTAL_SIZE_BYTES / (1024 ** 2)
            elapsed = time.time() - start_time
            speed_mb = (uploaded_mb / elapsed) if elapsed > 0 else 0
            print(f"Uploaded: {uploaded_mb:>8.1f} MB / {total_mb:.1f} MB ({progress:5.1f}%) @ {speed_mb:4.1f} MB/s", flush=True)

    elapsed_total = time.time() - start_time
    file_id = response.get("id")
    print(f"\nTransfer to Landing complete in {elapsed_total / 60:.1f} minutes!")
    print(f"File ID: {file_id}")
    print(f"File Name: {response.get('name')}")

    # Write Landing snapshot receipt
    import json
    landing_manifest = {
        "source_id": "opendata_org",
        "snapshot_id": "opendata_org-20260305",
        "status": "landing_published",
        "format": "native_zip",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "table_name": "landing_opendata_org_native",
        "files": [
            {
                "id": file_id,
                "name": ARCHIVE_FILENAME,
                "size": TOTAL_SIZE_BYTES,
                "url": OPENDATA_URL,
                "inner_members": [
                    {"name": "organization.json", "format": "senzing_jsonl"},
                    {"name": "locations.json", "format": "senzing_jsonl"},
                    {"name": "peoplebusiness.json", "format": "senzing_jsonl"},
                ],
            }
        ],
    }
    manifest_bytes = json.dumps(landing_manifest, indent=2).encode("utf-8")
    manifest_media = MediaIoBaseUpload(io.BytesIO(manifest_bytes), mimetype="application/json")
    svc.files().create(
        body={"name": "landing-snapshot.json", "parents": [target_folder_id], "mimeType": "application/json"},
        media_body=manifest_media,
        fields="id, name"
    ).execute()
    print("Published landing-snapshot.json to 01_landing/opendata_org/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
