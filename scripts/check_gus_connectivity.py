"""Check live connectivity to Statistics Poland (GUS) endpoints."""

import os
import socket
import sys
import time
import urllib.request

ENDPOINTS = [
    ("GUS Portal (stat.gov.pl)", "194.165.48.116", "https://stat.gov.pl"),
    ("GUS API DBW (api-dbw.stat.gov.pl)", "194.165.48.123", "https://api-dbw.stat.gov.pl/api/version"),
    ("GUS DBW Web (dbw.stat.gov.pl)", "194.165.48.118", "https://dbw.stat.gov.pl/api_app/wsk/getIndicatorsTree"),
    ("GUS BDL Web (bdl.stat.gov.pl)", "194.165.48.118", "https://bdl.stat.gov.pl"),
]


def test_socket(ip: str, port: int = 443, timeout: float = 3.0) -> bool:
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((ip, port))
        return True
    except Exception:
        return False
    finally:
        s.close()


def test_http(url: str, timeout: float = 5.0) -> tuple[bool, str]:
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed = time.time() - t0
            return True, f"HTTP {resp.status} ({elapsed:.2f}s)"
    except Exception as exc:
        elapsed = time.time() - t0
        return False, f"{type(exc).__name__}: {exc} ({elapsed:.2f}s)"


def main():
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    print(f"Checking GUS connectivity (Proxy: {proxy or 'Direct / None'})...\n")
    try:
        req = urllib.request.Request("https://api.ipify.org", headers={"User-Agent": "curl/7.88.1"})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            outbound_ip = resp.read().decode().strip()
            print(f"Current Outbound IP: {outbound_ip}\n")
    except Exception as exc:
        print(f"Current Outbound IP: unknown ({exc})\n")

    all_ok = True
    for name, ip, url in ENDPOINTS:
        http_ok, http_msg = test_http(url, timeout=5.0)
        status_str = f"ONLINE: {http_msg}" if http_ok else f"ERROR: {http_msg}"
        if not http_ok:
            all_ok = False
        print(f"[{'OK' if http_ok else 'FAIL'}] {name}: {status_str}")

    print("\nSummary:")
    if all_ok:
        print("All GUS endpoints are reachable. Ingestion can proceed.")
        sys.exit(0)
    else:
        print("GUS Web endpoint is currently unreachable.")
        sys.exit(1)


if __name__ == "__main__":
    main()
