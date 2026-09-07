"""Compatibility entry point for the shared fail-closed offline process runner."""
from pathlib import Path
import runpy

runpy.run_path(str(Path(__file__).resolve().parents[2] / "src/offline_process.py"), run_name="__main__")
