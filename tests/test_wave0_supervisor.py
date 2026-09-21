"""Recovery must not turn old completion claims into automatic production writes."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
import fcntl

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/wave0_supervisor.py'
spec = importlib.util.spec_from_file_location('wave0_supervisor', SCRIPT)
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class RecoverySupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / 'src').mkdir()
        (self.root / 'src/bdl_web_adaptive.py').touch()

    def checkpoint(self, folder, filename, value):
        path = self.root / 'portal/test-results' / folder / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
        return path

    def test_completed_reports_never_authorize_downstream_publication(self):
        self.checkpoint('bdl-web-bulk', 'bootstrap-summary.json', {'load_complete': True})
        self.checkpoint('dbw-bronze', 'checkpoint.json', {'status': 'completed', 'completed_indicators': 1550})
        result = supervisor.observe(self.root, lambda _: [])
        self.assertTrue(result['bdl']['reported_load_complete'])
        self.assertEqual(result['dbw']['reported_completed_indicators'], 1550)
        for source in ('bdl', 'dbw'):
            self.assertEqual(result[source]['downstream_status'], 'blocked')
            self.assertEqual(result[source]['process_status'], 'stopped')

    def test_running_checkpoint_is_not_a_live_process(self):
        self.checkpoint('bdl-web-bulk', 'bootstrap-summary.json', {'run_stop_reason': 'running'})
        result = supervisor.observe(self.root, lambda _: [])
        self.assertEqual(result['bdl']['process_status'], 'stopped')

    def test_shell_entrypoint_runs_monitor_without_production_environment(self):
        result = subprocess.run(
            ['bash', str(SCRIPT.with_name('autonomous_wave0_supervisor.sh')),
             '--runtime-root', str(self.root), '--once'],
            env={'PATH': os.defpath, 'ZOHELO_SUPERVISOR_PYTHON': sys.executable},
            capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        state = json.loads(result.stdout)
        self.assertEqual(state['mode'], 'monitor_only')
        self.assertEqual(state['dbw']['downstream_status'], 'blocked')
        self.assertFalse((self.root / 'portal/test-results').exists())

    def test_duplicate_processes_are_visible(self):
        result = supervisor.observe(self.root, lambda _: [100, 200])
        self.assertTrue(result['bdl']['duplicate_writers_detected'])
        self.assertEqual(result['bdl']['process_status'], 'running')

    def test_corrupt_checkpoint_does_not_prevent_monitoring(self):
        path = self.checkpoint('bdl-web-bulk', 'bootstrap-summary.json', {})
        path.write_text('{')
        result = supervisor.observe(self.root, lambda _: [])
        self.assertEqual(result['bdl']['checkpoint_error'], 'checkpoint_unreadable')
        self.assertFalse(result['bdl']['reported_load_complete'])

    def test_second_monitor_cannot_overwrite_existing_status(self):
        state_dir = self.root / 'state'
        state_dir.mkdir()
        status = state_dir / 'status.json'
        status.write_text('retained')
        lock_dir = self.root / '.local/wave0-supervisor'
        lock_dir.mkdir(parents=True)
        with (lock_dir / 'supervisor.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(supervisor.main(['--runtime-root', str(self.root), '--state-dir', str(state_dir), '--once']), 2)
        self.assertEqual(status.read_text(), 'retained')

    def test_lock_released_after_successful_exit(self):
        with mock.patch.object(supervisor, 'observe', return_value={'mode': 'monitor_only'}):
            for name in ('first-state', 'second-state'):
                self.assertEqual(supervisor.main(['--runtime-root', str(self.root), '--state-dir', str(self.root / name), '--once']), 0)
                self.assertTrue((self.root / name / 'status.json').is_file())

    def test_failed_atomic_replace_preserves_previous_status(self):
        status = self.root / 'status.json'
        status.write_text('retained')
        with mock.patch.object(supervisor.os, 'replace', side_effect=OSError('fixture failure')):
            with self.assertRaises(OSError):
                supervisor.save_state(status, {'new': True})
        self.assertEqual(status.read_text(), 'retained')
        self.assertEqual(list(self.root.glob('.supervisor-*')), [])

    def test_process_identity_does_not_match_shell_command_strings(self):
        proc = self.root / 'proc'
        proc.mkdir()
        for pid, args in [('10', b'python\0src/bdl_web_adaptive.py\0'), ('11', b'bash\0-c\0python src/bdl_web_adaptive.py\0')]:
            entry = proc / pid
            entry.mkdir()
            (entry / 'cmdline').write_bytes(args)
            (entry / 'cwd').symlink_to(self.root)
        self.assertEqual(supervisor.process_ids(self.root / 'src/bdl_web_adaptive.py', proc), [10])

    def test_only_interpreter_script_operand_identifies_writer(self):
        proc = self.root / 'proc'
        proc.mkdir()
        target = str(self.root / 'src/bdl_web_adaptive.py').encode()
        other = self.root / 'other'
        other.mkdir()
        cases = [
            ('10', b'python\0-u\0' + target + b'\0', self.root),
            ('11', b'python\0other.py\0--input\0' + target + b'\0', self.root),
            ('12', b'python\0-m\0other\0' + target + b'\0', self.root),
            ('13', b'python\0-c\0print(1)\0' + target + b'\0', self.root),
            ('14', b'python\0src/bdl_web_adaptive.py\0', other),
            ('15', b'python\0-W\0ignore\0src/bdl_web_adaptive.py\0', self.root),
        ]
        for pid, args, cwd in cases:
            entry = proc / pid
            entry.mkdir()
            (entry / 'cmdline').write_bytes(args)
            (entry / 'cwd').symlink_to(cwd)
        self.assertEqual(supervisor.process_ids(self.root / 'src/bdl_web_adaptive.py', proc), [10, 15])


if __name__ == '__main__':
    unittest.main()
