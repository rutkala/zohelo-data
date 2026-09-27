"""Bounded public NBP transport. Response shaping belongs to dbt."""
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import time
from urllib.parse import urlsplit

import requests


def _retry_delay(response, attempt, max_delay_seconds, now):
    fallback = min(2**attempt, max_delay_seconds)
    raw = response.headers.get("Retry-After")
    if not isinstance(raw, str) or not raw.strip():
        return fallback
    value = raw.strip()
    if value.isdecimal():
        return min(int(value), max_delay_seconds)
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return fallback
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    delay = max(0.0, (retry_at.astimezone(timezone.utc) - now()).total_seconds())
    return min(delay, max_delay_seconds)


def fetch_response(
    plan,
    *,
    session=None,
    max_bytes=12_000_000,
    attempts=4,
    timeout=(10, 45),
    max_retry_delay_seconds=8,
    sleep=time.sleep,
    now=lambda: datetime.now(timezone.utc),
):
    """Return (status, exact bytes, retry count); never print request credentials."""
    if not isinstance(attempts, int) or isinstance(attempts, bool) or not 1 <= attempts <= 8:
        raise ValueError("NBP HTTP attempts must be between 1 and 8")
    if (
        not isinstance(max_retry_delay_seconds, int)
        or isinstance(max_retry_delay_seconds, bool)
        or not 0 <= max_retry_delay_seconds <= 60
    ):
        raise ValueError("NBP retry delay limit must be between 0 and 60 seconds")
    if (
        not isinstance(timeout, tuple)
        or len(timeout) != 2
        or any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not 0 < value <= 120
            for value in timeout
        )
    ):
        raise ValueError("NBP connect/read timeouts must be between 0 and 120 seconds")
    url = plan.request_url
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.netloc != "api.nbp.pl" or not parts.path.startswith("/api/"):
        raise ValueError("NBP requests must use the configured public HTTPS API")
    client = session or requests.Session()
    owned = session is None
    try:
        for attempt in range(attempts):
            try:
                with client.get(
                    url,
                    params=plan.params,
                    timeout=timeout,
                    stream=True,
                    allow_redirects=False,
                ) as response:
                    status = response.status_code
                    if (
                        status in (408, 429) or 500 <= status <= 599
                    ) and attempt + 1 < attempts:
                        sleep(_retry_delay(response, attempt, max_retry_delay_seconds, now))
                        continue
                    chunks = []
                    length = 0
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        length += len(chunk)
                        if length > max_bytes:
                            raise ValueError("NBP response exceeds the configured size limit")
                        chunks.append(chunk)
                    return status, b"".join(chunks), attempt
            except (requests.Timeout, requests.ConnectionError, requests.exceptions.ChunkedEncodingError):
                if attempt + 1 == attempts:
                    raise RuntimeError("NBP request exhausted bounded network retries") from None
                sleep(min(2 ** attempt, 8))
        raise RuntimeError("NBP request did not finish")
    finally:
        if owned:
            client.close()
