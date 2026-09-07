"""Bounded public NBP transport. Response shaping belongs to dbt."""
import time
from urllib.parse import urlsplit

import requests


def fetch_response(plan, *, session=None, max_bytes=12_000_000, attempts=4, sleep=time.sleep):
    """Return (status, exact bytes, retry count); never print request credentials."""
    url = plan.request_url
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.netloc != "api.nbp.pl" or not parts.path.startswith("/api/"):
        raise ValueError("NBP requests must use the configured public HTTPS API")
    client = session or requests.Session()
    owned = session is None
    try:
        for attempt in range(attempts):
            try:
                with client.get(url, params=plan.params, timeout=(10, 45), stream=True,
                                allow_redirects=False) as response:
                    status = response.status_code
                    if status in (408, 429, 500, 502, 503, 504) and attempt + 1 < attempts:
                        sleep(min(2 ** attempt, 8))
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
