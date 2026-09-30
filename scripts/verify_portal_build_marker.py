#!/usr/bin/env python3
"""Verify that the deployed portal exposes the exact build marker."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any


EXPECTED_CAPABILITIES = {
    "supported_release_formats": [1, 2],
    "supported_drive_layouts": [],
    "data_source": "cloudflare-r2",
    "supported_retained_bronze_formats": [1, 2],
}


class MarkerVerificationError(RuntimeError):
    """Raised when the deployed marker cannot be accepted."""


def build_marker_url(origin: str, expected_git_commit: str) -> str:
    parsed = urllib.parse.urlsplit(origin)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("origin must be an absolute HTTP(S) URL")
    if parsed.query or parsed.fragment:
        raise ValueError("origin must not contain a query string or fragment")
    base_path = parsed.path.rstrip("/")
    marker_path = f"{base_path}/portal-build.json"
    query = urllib.parse.urlencode({"verification": expected_git_commit})
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, marker_path, query, "")
    )


def validate_marker(payload: Any, expected_git_commit: str) -> dict[str, Any]:
    expected = {"git_commit": expected_git_commit, **EXPECTED_CAPABILITIES}
    if payload != expected:
        raise MarkerVerificationError(
            "portal build marker does not match the exact deployed contract"
        )
    return payload


def read_marker(
    url: str,
    *,
    timeout_seconds: float,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Cache-Control": "no-cache",
            "User-Agent": "zohelo-portal-build-verifier/1",
        },
    )
    with opener(request, timeout=timeout_seconds) as response:
        status = getattr(response, "status", None)
        if status != 200:
            raise MarkerVerificationError(f"unexpected HTTP status: {status}")
        content_type = response.headers.get_content_type()
        if content_type != "application/json":
            raise MarkerVerificationError(
                f"unexpected content type: {content_type or 'missing'}"
            )
        body = response.read(16_385)
        if len(body) > 16_384:
            raise MarkerVerificationError("portal build marker exceeds 16 KiB")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MarkerVerificationError("portal build marker is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise MarkerVerificationError("portal build marker must be a JSON object")
    return payload


def verify_deployed_marker(
    origin: str,
    expected_git_commit: str,
    *,
    attempts: int = 12,
    delay_seconds: float = 5.0,
    timeout_seconds: float = 10.0,
    opener: Callable[..., Any] = urllib.request.urlopen,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if delay_seconds < 0 or timeout_seconds <= 0:
        raise ValueError("delay must be non-negative and timeout must be positive")
    url = build_marker_url(origin, expected_git_commit)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            payload = read_marker(
                url,
                timeout_seconds=timeout_seconds,
                opener=opener,
            )
            return validate_marker(payload, expected_git_commit)
        except (
            MarkerVerificationError,
            OSError,
            urllib.error.URLError,
        ) as exc:
            last_error = exc
            if attempt < attempts:
                sleeper(delay_seconds)
    raise MarkerVerificationError(
        f"deployed portal marker was not accepted after {attempts} attempts: "
        f"{last_error}"
    ) from last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--expected-git-commit", required=True)
    parser.add_argument("--attempts", type=int, default=12)
    parser.add_argument("--delay-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = verify_deployed_marker(
        args.origin,
        args.expected_git_commit,
        attempts=args.attempts,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
    )
    print(
        json.dumps(
            {
                "status": "verified",
                "origin": args.origin,
                "git_commit": payload["git_commit"],
                **EXPECTED_CAPABILITIES,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
