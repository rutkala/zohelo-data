#!/usr/bin/env python3
"""Own ten local wireproxy processes for one BDL Actions invocation.

Secret values and wireproxy diagnostics never enter logs or artifacts. This process
supervises its children and removes its private runtime directory on exit.
"""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time

WORKERS = 10
INGESTION_CREDENTIALS = ('GUS_BDL_WEB_EMAIL', 'GUS_BDL_WEB_PASSWORD',
                         'GOOGLE_OAUTH_CLIENT_ID', 'GOOGLE_OAUTH_CLIENT_SECRET',
                         'GOOGLE_OAUTH_REFRESH_TOKEN')
LISTENER = {"http", "socks5", "httpproxy", "socks5proxy"}
SECTION = re.compile(r"^\s*\[([^\]]+)\]\s*(?:[#;].*)?$")
KEY = re.compile(r"^[A-Za-z0-9+/]{43}=$")


def route_environment() -> dict[str, str]:
    return {k: v for k, v in os.environ.items()
            if not re.fullmatch(r'ZOHELO_WORKER\d+', k)
            and not k.startswith(('GOOGLE_OAUTH_', 'GUS_BDL_WEB_'))
            and k not in ('GITHUB_TOKEN', 'GH_TOKEN')}


class RouteError(ValueError):
    pass


def _key(value: str) -> bool:
    if not KEY.fullmatch(value):
        return False
    try:
        return len(base64.b64decode(value, validate=True)) == 32
    except ValueError:
        return False


def validate_config(raw: str, worker: int) -> tuple[str, tuple[str, ...]]:
    """Return sanitized source sections and opaque duplicate-detection values."""
    if not raw or '***' in raw or 'REDACTED' in raw.upper():
        raise RouteError(f"ZOHELO_WORKER{worker}: empty or masked config")
    sections: dict[str, list[str]] = {}
    current = None
    for line in raw.splitlines():
        match = SECTION.fullmatch(line)
        if match:
            current = match.group(1).lower()
            if current in sections:
                raise RouteError(f"ZOHELO_WORKER{worker}: repeated section")
            sections[current] = []
            continue
        if current is None:
            if line.strip() and not line.lstrip().startswith(('#', ';')):
                raise RouteError(f"ZOHELO_WORKER{worker}: invalid config header")
            continue
        if current not in LISTENER:
            sections[current].append(line)
    if set(sections) - LISTENER != {'interface', 'peer'}:
        raise RouteError(f"ZOHELO_WORKER{worker}: expected Interface and Peer")
    parsed = {}
    for name in ('interface', 'peer'):
        values = {}
        for line in sections[name]:
            if not line.strip() or line.lstrip().startswith(('#', ';')):
                continue
            if '=' not in line:
                raise RouteError(f"ZOHELO_WORKER{worker}: malformed {name} field")
            k, v = (part.strip() for part in line.split('=', 1))
            k = k.lower()
            if k in values:
                raise RouteError(f"ZOHELO_WORKER{worker}: duplicate {name} field")
            values[k] = v.split('#', 1)[0].strip()
        parsed[name] = values
    interface, peer = parsed['interface'], parsed['peer']
    if not _key(interface.get('privatekey', '')) or not _key(peer.get('publickey', '')):
        raise RouteError(f"ZOHELO_WORKER{worker}: invalid 32-byte key")
    if 'presharedkey' in peer and not _key(peer['presharedkey']):
        raise RouteError(f"ZOHELO_WORKER{worker}: invalid preshared key")
    addresses = interface.get('address', '')
    try:
        address_set = [str(ipaddress.ip_interface(s.strip())) for s in addresses.split(',')]
        if not address_set or len(set(address_set)) != len(address_set):
            raise ValueError()
    except ValueError:
        raise RouteError(f"ZOHELO_WORKER{worker}: invalid interface address") from None
    endpoint = peer.get('endpoint', '')
    endpoint_match = re.fullmatch(r'(?:\[[0-9a-fA-F:]+\]|[A-Za-z0-9.-]+):([0-9]+)', endpoint)
    if not endpoint_match or not 1 <= int(endpoint_match.group(1)) <= 65535:
        raise RouteError(f"ZOHELO_WORKER{worker}: invalid endpoint")
    allowed = peer.get('allowedips', '')
    try:
        routes = [str(ipaddress.ip_network(s.strip(), strict=False)) for s in allowed.split(',')]
    except ValueError:
        raise RouteError(f"ZOHELO_WORKER{worker}: invalid allowed routes") from None
    if '0.0.0.0/0' not in routes or len(set(routes)) != len(routes):
        raise RouteError(f"ZOHELO_WORKER{worker}: full IPv4 tunnel required")
    # Preserve source Interface/Peer fields, never preserve caller-supplied listeners.
    clean = '\n'.join(f'[{name}]\n' + '\n'.join(sections[name]) for name in ('interface', 'peer'))
    # Provider profiles can share keys/inside addresses across endpoints.
    # Reject only an identical normalized tunnel; live egress establishes
    # whether nominally different profiles actually form distinct routes.
    return clean, (tuple(sorted(interface.items())), tuple(sorted(peer.items())))


def load_configs(environ: dict[str, str]) -> list[str]:
    missing = [f'ZOHELO_WORKER{i}' for i in range(1, WORKERS + 1) if not environ.get(f'ZOHELO_WORKER{i}', '').strip()]
    if missing:
        raise RouteError('Missing worker secrets: ' + ', '.join(missing))
    configs, tunnels = [], set()
    for i in range(1, WORKERS + 1):
        clean, identity = validate_config(environ[f'ZOHELO_WORKER{i}'], i)
        if identity in tunnels:
            raise RouteError(f'ZOHELO_WORKER{i}: duplicate tunnel identity')
        tunnels.add(identity)
        configs.append(clean)
    return configs


def listener_config(clean: str, index: int) -> str:
    return f'{clean}\n\n[http]\nBindAddress = 127.0.0.1:{8081 + index}\n'


def browser_probe(proxy: str | None, *, node: str = 'node') -> str:
    env = route_environment()
    for name in ('HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy', 'BDL_WEB_PROXY',
                 *[f'ZOHELO_WORKER{i}' for i in range(1, WORKERS + 1)]):
        env.pop(name, None)
    if proxy:
        env['BDL_WEB_PROXY'] = proxy
    else:
        env['BDL_PREFLIGHT_DIRECT'] = '1'
    process = None
    try:
        process = subprocess.Popen([node, 'portal/scripts/bdl-vpn-preflight.mjs'], env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                   text=True, start_new_session=True)
        output, _ = process.communicate(timeout=55)
        if process.returncode != 0:
            raise RouteError('Browser route probe failed')
        identity = output.strip()
        ipaddress.ip_address(identity)
        return identity
    except (subprocess.SubprocessError, ValueError, RouteError):
        raise RouteError('Browser route probe failed' + (' for a worker' if proxy else ' for direct baseline')) from None
    finally:
        if process is not None:
            # Chromium descendants are owned by this private probe process group.
            # Clean up even if Python receives a signal during the probe.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            if process.poll() is None:
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=3)


@contextmanager
def cluster(configs: list[str], *, binary: str, temp_parent: str | None = None):
    processes = []
    with tempfile.TemporaryDirectory(prefix='bdl-routes-', dir=temp_parent) as directory:
        root = Path(directory)
        os.chmod(root, 0o700)
        cluster_file = root / 'cluster.json'
        try:
            for index, clean in enumerate(configs):
                conf = root / f'worker-{index + 1}.conf'
                with open(conf, 'x', encoding='utf-8', opener=lambda p, flags: os.open(p, flags, 0o600)) as file:
                    file.write(listener_config(clean, index))
                with open(os.devnull, 'wb') as null:
                    process = subprocess.Popen([binary, '-c', str(conf)], stdout=null, stderr=null,
                                               env=route_environment())
                processes.append(process)
            routes = [{'http_port': 8081 + i, 'status': 'HEALTHY'} for i in range(WORKERS)]
            cluster_file.write_text(json.dumps(routes), encoding='utf-8')
            cluster_file.chmod(0o600)
            yield cluster_file, processes
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def preflight(processes: list, *, node: str = 'node') -> None:
    direct = browser_probe(None, node=node)
    seen = set()
    for i, process in enumerate(processes, 1):
        if process.poll() is not None:
            raise RouteError(f'Worker {i}: route process exited')
        route = f'http://127.0.0.1:{8080 + i}'
        for attempt in range(2):
            if process.poll() is not None:
                raise RouteError(f'Worker {i}: route process exited')
            try:
                identity = browser_probe(route, node=node)
                break
            except RouteError:
                if attempt:
                    raise RouteError(f'Worker {i}: browser route unavailable') from None
                time.sleep(2)
        if identity == direct or identity in seen:
            raise RouteError(f'Worker {i}: direct or duplicate browser egress')
        seen.add(identity)
        print(f'Worker {i}: distinct browser egress and BDL reachable', flush=True)
    if len(seen) != WORKERS:
        raise RouteError('Ten isolated routes required')


def bounded_budget(started: str, current: float) -> int:
    """Reserve 20 minutes to drain workers, commit control, and release ownership."""
    if not started.isdigit():
        raise RouteError('Missing Actions job start timestamp')
    remaining = int(started) + 350 * 60 - int(current) - 1200
    if remaining < 600:
        raise RouteError('Insufficient time for a bounded BDL ingestion run')
    return min(16200, remaining)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preflight', action='store_true')
    parser.add_argument('--binary', default='wireproxy')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not args.preflight and not command:
        parser.error('expected --preflight or a command')
    child = None
    stopping = False
    stop_deadline = None
    old_term, old_int = signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT)
    def shutdown(_signum, _frame):
        nonlocal stopping, stop_deadline
        stopping = True
        if child is None:
            raise RouteError('BDL route setup interrupted')
        if stop_deadline is None:
            stop_deadline = time.monotonic() + 900
            child.send_signal(signal.SIGINT)
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        if command:
            missing = [name for name in INGESTION_CREDENTIALS if not os.environ.get(name)]
            if missing:
                raise RouteError('Missing ingestion credentials: ' + ', '.join(missing))
        configs = load_configs(os.environ)
        with cluster(configs, binary=args.binary, temp_parent=os.environ.get('RUNNER_TEMP')) as (cluster_file, processes):
            preflight(processes)
            if args.preflight:
                print('BDL ten-route preflight passed; no Drive data written or ingestion started.')
                return 0
            env = {k: v for k, v in os.environ.items() if not re.fullmatch(r'ZOHELO_WORKER\d+', k)}
            env['BDL_PROXY_CLUSTER_FILE'] = str(cluster_file)
            if '@BDL_BUDGET@' in command:
                budget = bounded_budget(env.get('BDL_JOB_STARTED_EPOCH', ''), time.time())
                command = [str(budget) if item == '@BDL_BUDGET@' else item for item in command]
            child = subprocess.Popen(command, env=env)
            while True:
                try:
                    status = child.wait(timeout=2)
                    return 1 if stopping else status
                except subprocess.TimeoutExpired:
                    if any(process.poll() is not None for process in processes):
                        if not stopping:
                            stopping = True
                            stop_deadline = time.monotonic() + 900
                            child.send_signal(signal.SIGINT)
                    if stopping and time.monotonic() >= stop_deadline:
                        child.kill()
                        child.wait()
                        raise RouteError('BDL worker drain exceeded grace period; inspect active writer')
    except (RouteError, OSError) as exc:
        # Do not stringify system exceptions: they may embed configuration/paths.
        print(str(exc) if isinstance(exc, RouteError) else 'BDL route setup failed', file=sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGTERM, old_term)
        signal.signal(signal.SIGINT, old_int)


if __name__ == '__main__':
    sys.exit(main())
