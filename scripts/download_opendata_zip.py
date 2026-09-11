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
        with urllib.request.urlopen(req) as resp:
            data = resp.read()
            b[:len(data)] = data
            self.pos += len(data)
            return len(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-production-write", action="store_true", help="Authorize writes to zohelo-data Drive root")
    args = parser.parse_args()

    sm = StorageManager()
    if args.allow_production_write:
        sm.authorize_writes()

    root_id = sm.resolve_root(create=False)
    print(f"Resolved Drive Root ID: {root_id}")

    # Target folder: 05_archive / opendata
    archive_root = sm.resolve_zone("archive", create=True)
    target_folder_id = sm.get_or_create_nested_folder(["opendata"], root_id=archive_root)
    print(f"Target Drive folder (05_archive/opendata): {target_folder_id}")

    # Check if file already exists in target folder
    svc = sm.drive_service
    q = f"'{target_folder_id}' in parents and name = '{ARCHIVE_FILENAME}' and trashed = false"
    existing = svc.files().list(q=q, fields="files(id, name, size)").execute().get("files", [])
    if existing:
        f = existing[0]
        print(f"Archive already exists on Drive: ID={f['id']}, Size={int(f.get('size', 0))/(1024**3):.2f} GB")
        return 0

    print(f"Starting cloud-to-cloud streaming transfer:")
    print(f"  • Source: GCS ({OPENDATA_URL})")
    print(f"  • Total size: {TOTAL_SIZE_BYTES / (1024**3):.2f} GB ({TOTAL_SIZE_BYTES:,} bytes)")
    print(f"  • Destination: Google Drive 05_archive/opendata/{ARCHIVE_FILENAME}")
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
        "description": "Full OpenData.org Senzing entity resolution archive (2026-03-05 snapshot)"
    }

    request = svc.files().create(body=file_metadata, media_body=media, fields="id, name, size")

    start_time = time.time()
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            progress = status.progress() * 100
            uploaded_mb = (status.resumable_progress or 0) / (1024 ** 2)
            total_mb = TOTAL_SIZE_BYTES / (1024 ** 2)
            elapsed = time.time() - start_time
            speed_mb = (uploaded_mb / elapsed) if elapsed > 0 else 0
            print(f"Uploaded: {uploaded_mb:>8.1f} MB / {total_mb:.1f} MB ({progress:5.1f}%) @ {speed_mb:4.1f} MB/s", flush=True)

    elapsed_total = time.time() - start_time
    print(f"\nTransfer complete in {elapsed_total / 60:.1f} minutes!")
    print(f"File ID: {response.get('id')}")
    print(f"File Name: {response.get('name')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
