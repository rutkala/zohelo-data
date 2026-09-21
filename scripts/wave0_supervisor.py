"""Observe retained Wave 0 progress without interpreting it as release acceptance.

This recovery supervisor does not launch downstream writers. BDL Bronze publication
still needs repair; DBW outputs need verified release reconciliation. A local flock
prevents duplicate monitors, not cross-host production writers.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def process_ids(script: Path, proc_root: Path = Path('/proc')) -> list[int]:
    """Match a Python script argument and its working directory, never a shell string."""
    found = []
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            args = (entry / 'cmdline').read_bytes().split(b'\0')
            if not args or not Path(os.fsdecode(args[0])).name.startswith('python'):
                continue
            cwd = (entry / 'cwd').resolve(strict=True)
            index = 1
            while index < len(args):
                arg = args[index]
                if not arg or arg == b'-' or arg.startswith((b'-c', b'-m')):
                    break
                if arg == b'--':
                    index += 1
                    break
                if arg in (b'-W', b'-X', b'--check-hash-based-pycs'):
                    index += 2
                elif arg.startswith(b'-'):
                    index += 1
                else:
                    break
            if index >= len(args) or not args[index] or args[index].startswith(b'-'):
                continue
            candidate = Path(os.fsdecode(args[index]))
            if (candidate if candidate.is_absolute() else cwd / candidate).resolve() == script.resolve():
                found.append(int(entry.name))
        except (OSError, ValueError):
            continue
    return sorted(found)


def read_checkpoint(path: Path) -> tuple[dict, str | None]:
    try:
        if path.stat().st_size > 1024 * 1024:
            return {}, 'checkpoint_exceeds_bound'
        value = json.loads(path.read_text())
        if not isinstance(value, dict):
            return {}, 'checkpoint_not_object'
        return value, None
    except FileNotFoundError:
        return {}, 'checkpoint_missing'
    except (OSError, ValueError):
        return {}, 'checkpoint_unreadable'


def observe(runtime_root: Path, find_processes=process_ids) -> dict:
    logs = runtime_root / 'portal/test-results'
    bdl, bdl_error = read_checkpoint(logs / 'bdl-web-bulk/bootstrap-summary.json')
    dbw, dbw_error = read_checkpoint(logs / 'dbw-bronze/checkpoint.json')
    bdl_pids = find_processes(runtime_root / 'src/bdl_web_adaptive.py')
    dbw_pids = find_processes(runtime_root / 'src/dbw_bronze_loader.py')
    return {
        'format_version': 1,
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'mode': 'monitor_only',
        'runtime_root': str(runtime_root),
        'bdl': {
            'process_ids': bdl_pids,
            'process_status': 'running' if bdl_pids else 'stopped',
            'duplicate_writers_detected': len(bdl_pids) > 1,
            'checkpoint_error': bdl_error,
            'checkpoint_updated_at_utc': bdl.get('updated_at_utc'),
            'reported_selection_complete_subgroups': bdl.get('selection_complete_subgroups'),
            'reported_remaining_subgroups': bdl.get('remaining_subgroups'),
            'reported_load_complete': bdl.get('load_complete') is True,
            'downstream_status': 'blocked',
            'downstream_reason': 'BDL Bronze publication must preserve and verify prior outputs before automatic launch',
        },
        'dbw': {
            'process_ids': dbw_pids,
            'process_status': 'running' if dbw_pids else 'stopped',
            'duplicate_writers_detected': len(dbw_pids) > 1,
            'checkpoint_error': dbw_error,
            'reported_bronze_status': dbw.get('status'),
            'reported_completed_indicators': dbw.get('completed_indicators'),
            'downstream_status': 'blocked',
            'downstream_reason': 'Preserve and reconcile existing Bronze outputs with the verified release contract before automatic launch',
        },
    }


def save_state(path: Path, value: dict) -> None:
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix='.supervisor-', delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-root', type=Path, default=ROOT)
    parser.add_argument('--state-dir', type=Path)
    parser.add_argument('--interval', type=float, default=60)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args(argv)
    if args.interval < 1:
        parser.error('--interval must be at least one second')
    runtime_root = args.runtime_root.resolve()
    if not (runtime_root / 'src/bdl_web_adaptive.py').is_file():
        parser.error('--runtime-root must be an existing zohelo-data checkout')
    lock_dir = runtime_root / '.local/wave0-supervisor'
    lock_dir.mkdir(parents=True, exist_ok=True)
    state_dir = args.state_dir or lock_dir
    state_dir.mkdir(parents=True, exist_ok=True)
    with (lock_dir / 'supervisor.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print('A recovery supervisor already owns this runtime root.', flush=True)
            return 2
        while True:
            state = observe(runtime_root)
            save_state(state_dir / 'status.json', state)
            print(json.dumps(state), flush=True)
            if args.once:
                return 0
            time.sleep(args.interval)


if __name__ == '__main__':
    raise SystemExit(main())
