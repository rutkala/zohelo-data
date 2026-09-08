#!/usr/bin/env python3
"""Store an approved source credential as a GitHub Actions repository secret."""

import argparse
from getpass import GetPassWarning, getpass
from pathlib import Path
import subprocess
import sys
import warnings


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.source_credentials import source_access_status  # noqa: E402


APPROVED_SECRETS = {"GUS_BDL_API_KEY": "gus_bdl"}
GITHUB_REPOSITORY = "rutkala/zohelo-data"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "secret_name",
        nargs="?",
        choices=tuple(APPROVED_SECRETS),
        default="GUS_BDL_API_KEY",
    )
    args = parser.parse_args()

    try:
        with warnings.catch_warnings():
            # getpass otherwise falls back to echoed stdin when a hidden terminal
            # is unavailable. A credential setup helper must fail closed instead.
            warnings.simplefilter("error", GetPassWarning)
            value = getpass(f"Value for {args.secret_name}: ")
    except GetPassWarning:
        raise RuntimeError("Hidden credential input is unavailable") from None
    if not value:
        raise ValueError("A non-empty source credential is required")
    # Reuse the runtime validation without placing the value in process arguments.
    source_access_status(
        APPROVED_SECRETS[args.secret_name], environ={args.secret_name: value}
    )
    try:
        subprocess.run(
            [
                "gh",
                "secret",
                "set",
                args.secret_name,
                "--repo",
                GITHUB_REPOSITORY,
            ],
            input=value.encode("ascii"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        raise RuntimeError("Unable to store the GitHub Actions secret") from None
    finally:
        value = None
    print(f"Stored GitHub Actions secret {args.secret_name} for {GITHUB_REPOSITORY}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
