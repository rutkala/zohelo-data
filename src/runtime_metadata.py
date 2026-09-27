"""Runtime metadata shared by current platform entrypoints."""

import os
from pathlib import Path
import re
import subprocess


REPO_ROOT = Path(__file__).resolve().parents[1]


def _code_sha() -> str:
    """Return the exact clean Git revision used for a publishable build."""
    sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True
    ).strip()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("Build code must identify a Git commit")
    if subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=REPO_ROOT, text=True
    ).strip():
        raise ValueError("Commit the working tree before publishing a data release")
    if os.environ.get("GITHUB_SHA", sha) != sha:
        raise ValueError("Build checkout does not match the workflow commit")
    return sha
