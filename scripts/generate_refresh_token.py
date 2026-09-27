#!/usr/bin/env python3
"""
Generate Google OAuth refresh token (file-based, robust).

Usage:
  python scripts/generate_refresh_token.py

Environment variables (optional):
  GOOGLE_OAUTH_CLIENT_SECRET_FILE  Path to OAuth client JSON
  GOOGLE_OAUTH_TOKEN_FILE          Path to output token JSON
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_CLIENT_SECRET = ROOT_DIR / "credentials" / "oauth_client_secret.json"
DEFAULT_TOKEN_FILE = ROOT_DIR / "credentials" / "token.json"


def load_client_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"OAuth client file not found: {path}\n"
            "Create a Desktop OAuth client in Google Cloud and save JSON to this path."
        )

    with path.open("r", encoding="utf-8") as f:
        config = json.load(f)

    if not isinstance(config, dict) or not ("installed" in config or "web" in config):
        raise ValueError("Client secrets must include a top-level 'installed' or 'web' object.")

    return config


def main() -> int:
    client_secret_path = Path(
        os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET_FILE", str(DEFAULT_CLIENT_SECRET))
    )
    token_path = Path(os.environ.get("GOOGLE_OAUTH_TOKEN_FILE", str(DEFAULT_TOKEN_FILE)))

    print("\n" + "=" * 80)
    print("GOOGLE OAUTH REFRESH TOKEN GENERATOR")
    print("=" * 80 + "\n")
    print(f"Using client secrets file: {client_secret_path}")

    try:
        config = load_client_config(client_secret_path)
        client_type = "installed" if "installed" in config else "web"
        print(f"✅ OAuth config loaded successfully ({client_type})\n")

        flow = InstalledAppFlow.from_client_config(config, scopes=SCOPES)
        creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")

        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(creds.to_json(), encoding="utf-8")

        print("=" * 80)
        print("✅ SUCCESS!")
        print("=" * 80)
        print(f"\nToken saved to: {token_path}\n")

        if creds.refresh_token:
            print("🔑 Refresh token generated and stored in token.json")
        else:
            print("⚠️ No refresh token returned.")
            print("   Revoke app access at https://myaccount.google.com/permissions and run again.")

        return 0

    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
