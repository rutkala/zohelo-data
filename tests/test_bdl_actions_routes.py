"""Credential-free BDL Actions route and continuation contracts."""
import base64
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import multi_vpn_manager as vpn
import bdl_continuation


def key(number):
    return base64.b64encode(bytes([number]) * 32).decode()


def config(number):
    return (f'[Interface]\nPrivateKey = {key(number)}\nAddress = 10.2.0.{number}/32\n'
            f'\n[Peer]\nPublicKey = {key(99)}\nAllowedIPs = 0.0.0.0/0, ::/0\n'
            f'Endpoint = vpn.example.test:51820\n')


class RouteTests(unittest.TestCase):
    def test_missing_secret_names_only(self):
        env = {f'ZOHELO_WORKER{i}': config(i) for i in range(1, 9)}
        with self.assertRaisesRegex(vpn.RouteError, 'ZOHELO_WORKER9, ZOHELO_WORKER10') as err:
            vpn.load_configs(env)
        self.assertNotIn(key(1), str(err.exception))

    def test_bad_fields_and_masked_keys(self):
        for broken in [config(1).replace(key(1), '***'),
                       config(1).replace('[Peer]', '[Peeer]'),
                       config(1).replace('10.2.0.1/32', 'invalid'),
                       config(1).replace('vpn.example.test:51820', 'bad endpoint'),
                       config(1).replace('0.0.0.0/0, ::/0', '10.0.0.0/8'),
                       config(1).replace('PrivateKey', 'SecretKey')]:
            with self.subTest(broken=broken[:30]), self.assertRaises(vpn.RouteError):
                vpn.validate_config(broken, 1)

    def test_duplicate_key_or_route(self):
        env = {f'ZOHELO_WORKER{i}': config(i) for i in range(1, 11)}
        vpn.load_configs(env)
        env['ZOHELO_WORKER10'] = config(9)
        with self.assertRaisesRegex(vpn.RouteError, 'duplicate'):
            vpn.load_configs(env)
        # Shared inside addresses are common; browser egress is the route authority.
        env['ZOHELO_WORKER10'] = config(10).replace('10.2.0.10/32', '10.2.0.9/32')
        vpn.load_configs(env)

    def test_listener_replacement(self):
        original = config(1) + '\n[HttpProxy]\nBindAddress = 0.0.0.0:8080\n\n[socks5]\nBindAddress = 0.0.0.0:1080\n'
        cleaned, _ = vpn.validate_config(original, 1)
        result = vpn.listener_config(cleaned, 0)
        self.assertNotIn('0.0.0.0:8080', result)
        self.assertNotIn('0.0.0.0:1080', result)
        self.assertEqual(1, result.count('[http]'))
        self.assertIn('BindAddress = 127.0.0.1:8081', result)

    def test_owned_processes_only_cleanup_on_failure(self):
        class Process:
            def __init__(self):
                self.stopped = False
            def poll(self):
                return None if not self.stopped else 0
            def terminate(self):
                self.stopped = True
            def wait(self, timeout):
                return 0
        launched = []
        def start(*args, **kwargs):
            p = Process()
            launched.append(p)
            return p
        with tempfile.TemporaryDirectory() as td, patch.object(vpn.subprocess, 'Popen', side_effect=start):
            with self.assertRaisesRegex(RuntimeError, 'fixture stop'):
                with vpn.cluster([config(i) for i in range(1, 11)], binary='wireproxy', temp_parent=td) as (path, processes):
                    self.assertEqual(10, len(processes))
                    self.assertEqual(0o600, path.stat().st_mode & 0o777)
                    self.assertEqual(0o600, (path.parent / 'worker-1.conf').stat().st_mode & 0o777)
                    raise RuntimeError('fixture stop')
            self.assertEqual(10, len(launched))
            self.assertTrue(all(p.stopped for p in launched))
            self.assertEqual([], list(Path(td).iterdir()))

    def test_preflight_rejects_direct_or_duplicate_route(self):
        processes = [type('P', (), {'poll': lambda self: None})() for _ in range(10)]
        with patch.object(vpn, 'browser_probe', side_effect=['1.1.1.1'] + ['2.2.2.2'] * 10):
            with redirect_stdout(io.StringIO()), self.assertRaisesRegex(vpn.RouteError, 'duplicate'):
                vpn.preflight(processes)
        with patch.object(vpn, 'browser_probe', side_effect=['1.1.1.1'] * 11):
            with self.assertRaisesRegex(vpn.RouteError, 'direct'):
                vpn.preflight(processes)

    def test_job_deadline_reserves_drain_and_rejects_too_late_start(self):
        self.assertEqual(16200, vpn.bounded_budget('100000', 100000))
        self.assertEqual(700, vpn.bounded_budget('100000', 100000 + 350 * 60 - 1900))
        with self.assertRaises(vpn.RouteError):
            vpn.bounded_budget('100000', 100000 + 350 * 60 - 1700)

    def test_route_children_receive_no_worker_or_drive_credentials(self):
        with patch.dict(vpn.os.environ, {'ZOHELO_WORKER1': 'secret', 'GUS_BDL_WEB_PASSWORD': 'secret',
                                         'GOOGLE_OAUTH_REFRESH_TOKEN': 'secret', 'GITHUB_TOKEN': 'secret',
                                         'PATH': '/usr/bin'}):
            self.assertEqual({'PATH': '/usr/bin'}, {k: v for k, v in vpn.route_environment().items()
                                                   if k in {'ZOHELO_WORKER1', 'GUS_BDL_WEB_PASSWORD',
                                                    'GOOGLE_OAUTH_REFRESH_TOKEN', 'GITHUB_TOKEN', 'PATH'}})

    def test_timed_out_probe_cleans_its_owned_browser_group(self):
        class HangingProbe:
            pid = 8765
            returncode = None
            def communicate(self, timeout):
                raise subprocess.TimeoutExpired('node', timeout)
            def poll(self):
                return None
            def wait(self, timeout):
                self.returncode = -15
        with patch.object(vpn.subprocess, 'Popen', return_value=HangingProbe()) as popen, \
             patch.object(vpn.os, 'killpg') as killpg:
            with self.assertRaisesRegex(vpn.RouteError, 'probe failed'):
                vpn.browser_probe('http://127.0.0.1:8081')
            self.assertTrue(popen.call_args.kwargs['start_new_session'])
            killpg.assert_called_once_with(8765, vpn.signal.SIGTERM)


class ContinuationTests(unittest.TestCase):
    def test_only_saved_released_budget_with_work_continues(self):
        good = dict(status='interrupted', run_stop_reason='runtime_budget_reached',
                    checkpoint_saved=True, writer_released=True,
                    progress_counts_scope='durable_checkpoint_plus_current_run',
                    resumable_work=True, source_errors_require_review=False,
                    pass_complete=False, load_complete=False)
        self.assertTrue(bdl_continuation.should_continue(good))
        self.assertFalse(bdl_continuation.should_continue([]))
        for changes in ({'run_stop_reason': 'interrupted'}, {'status': 'pass_complete'},
                        {'checkpoint_saved': False}, {'writer_released': False},
                        {'resumable_work': False}, {'source_errors_require_review': True},
                        {'pass_complete': True}, {'progress_counts_scope': 'in_memory_last_observed_not_durable'}):
            with self.subTest(changes=changes):
                self.assertFalse(bdl_continuation.should_continue(good | changes))


if __name__ == '__main__':
    unittest.main()
