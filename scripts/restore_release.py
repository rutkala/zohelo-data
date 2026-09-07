"""Download the current verified platform release for local SQL and MetricFlow."""
import argparse
import json
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from drive_release_store import DriveReleaseStore
from local_release import restore_local_release
from storage_manager import StorageManager


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="New local directory; existing directories are never overwritten")
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    storage = StorageManager(backend="gdrive", allow_interactive_auth=False)
    root = storage.resolve_root(create=False)
    result = restore_local_release(DriveReleaseStore(storage, root), root, args.output_dir)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
