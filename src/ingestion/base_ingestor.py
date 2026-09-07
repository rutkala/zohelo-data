"""Compatibility entrypoint for the consolidated NBP platform runner."""
import argparse
import os
from pathlib import Path
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="all")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    args = parser.parse_args()
    if args.source != "all":
        parser.error("Publication keeps all four NBP sources together. Use --source all.")
    runner = Path(__file__).resolve().parents[1] / "nbp_platform.py"
    os.execv(sys.executable, [sys.executable, str(runner), "--mode", args.mode])
