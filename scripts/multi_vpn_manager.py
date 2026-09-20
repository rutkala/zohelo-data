#!/usr/bin/env python3
"""Manage multiple userspace WireGuard proxies (wireproxy) across different ports and IPs."""

from __future__ import annotations

import argparse
import glob
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import json

RUNTIME_DIR = Path("/workspaces/zohelo-data/.wireguard/instances")


def clean_existing():
    subprocess.run(["pkill", "-f", "wireproxy"], check=False)
    time.sleep(1)


def setup_instance(conf_path: Path, index: int, base_http_port: int = 8081, base_socks_port: int = 1081) -> dict:
    http_port = base_http_port + index
    socks_port = base_socks_port + index
    
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    instance_conf = RUNTIME_DIR / f"wp_{http_port}.conf"
    instance_log = RUNTIME_DIR / f"wp_{http_port}.log"

    raw_conf = conf_path.read_text(encoding="utf-8")
    # Clean out any old http or socks sections if present
    clean_lines = []
    skip = False
    for line in raw_conf.splitlines():
        if re.match(r"^\[(http|socks5|HttpProxy|Socks5)\]", line.strip(), re.IGNORECASE):
            skip = True
            continue
        if skip and line.startswith("["):
            skip = False
        if not skip:
            clean_lines.append(line)

    custom_section = f"\n[http]\nBindAddress = 127.0.0.1:{http_port}\n\n[Socks5]\nBindAddress = 127.0.0.1:{socks_port}\n"
    instance_conf.write_text("\n".join(clean_lines) + custom_section, encoding="utf-8")
    instance_conf.chmod(0o600)

    # Launch wireproxy
    proc = subprocess.Popen(
        ["wireproxy", "-c", str(instance_conf)],
        stdout=open(instance_log, "w"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )

    return {
        "index": index,
        "conf": conf_path.name,
        "http_port": http_port,
        "socks_port": socks_port,
        "pid": proc.pid,
        "log": str(instance_log),
    }


def test_proxy(http_port: int, timeout: float = 5.0) -> str | None:
    proxy_handler = urllib.request.ProxyHandler({"https": f"http://127.0.0.1:{http_port}", "http": f"http://127.0.0.1:{http_port}"})
    opener = urllib.request.build_opener(proxy_handler)
    try:
        req = urllib.request.Request("https://api.ipify.org", headers={"User-Agent": "curl/7.88.1"})
        with opener.open(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8").strip()
    except Exception as exc:
        return None


def main():
    parser = argparse.ArgumentParser(description="Multi-WireGuard VPN proxy orchestrator")
    parser.add_argument("--stop", action="store_true", help="Stop all running wireproxy instances")
    parser.add_argument("--status", action="store_true", help="Check status of existing proxy ports")
    parser.add_argument("--max-tunnels", type=int, default=10, help="Maximum concurrent tunnels (default 10)")
    args = parser.parse_args()

    if args.stop:
        clean_existing()
        print("All wireproxy instances stopped.")
        return

    if args.status:
        print("\n--- Active Multi-IP VPN Cluster Status ---")
        print(f"{'Port':<8} | {'Assigned Target':<10} | {'Egress Public IP':<18} | {'Status'}")
        print("-" * 65)
        for port in range(8081, 8091):
            ip = test_proxy(port)
            status = "HEALTHY" if ip else "DOWN"
            print(f"127.0.0.1:{port:<4} | {'SHARED':<10} | {str(ip):<18} | {status}")
        return

    # Prefer worker1..worker10 (deduplicated by filename)
    conf_dirs = [Path("/workspaces/zohelo-data/.devcontainer/wireguard"), Path("/workspaces/zohelo-data/.wireguard")]
    seen = set()
    worker_confs = []
    for d in conf_dirs:
        if d.is_dir():
            for f in d.glob("zohelo-worker*.conf"):
                if f.name in seen:
                    continue
                seen.add(f.name)
                m = re.search(r"worker(\d+)", f.name)
                num = int(m.group(1)) if m else 999
                worker_confs.append((num, f))
    
    worker_confs.sort(key=lambda x: x[0])
    selected_confs = [f for _, f in worker_confs][:args.max_tunnels]
    print(f"Selected {len(selected_confs)} worker configs. Starting instances...")

    clean_existing()
    instances = []
    for idx, conf in enumerate(selected_confs):
        inst = setup_instance(conf, idx)
        instances.append(inst)

    print("Waiting 3s for handshakes to complete...")
    time.sleep(3)

    results = []
    print("\n--- Active Multi-IP VPN Cluster Status ---")
    print(f"{'Port':<8} | {'Assigned Target':<10} | {'Config Name':<30} | {'Egress Public IP':<18} | {'Status'}")
    print("-" * 85)

    for i, inst in enumerate(instances):
        target = "SHARED"
        ip = test_proxy(inst["http_port"])
        status = "HEALTHY" if ip else "UNREACHABLE"
        inst["ip"] = ip
        inst["target"] = target
        inst["status"] = status
        results.append(inst)
        print(f"127.0.0.1:{inst['http_port']:<4} | {target:<10} | {inst['conf'][:28]:<30} | {str(ip):<18} | {status}")

    # Write summary for ingestion scripts to consume
    summary_file = Path("/workspaces/zohelo-data/.wireguard/cluster.json")
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nCluster details saved to {summary_file}")


if __name__ == "__main__":
    main()
