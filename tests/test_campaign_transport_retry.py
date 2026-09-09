"""Recovery must preserve failed attempts, quotas, budgets and source checkpoints."""
import json
from pathlib import Path
import ssl
import sys
import unittest
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ingestion import source_campaign as campaign
from ingestion.campaign_session import run_collection_session
from tests.test_source_campaign import Adapter, FakeClock, MemoryStore, TODAY, settings, task


class CampaignTransportRetryTests(unittest.TestCase):
    def test_timeout_recovery_reserves_every_attempt_and_accepts_once(self):
        clock, store, calls = FakeClock(), MemoryStore(), []

        def fetcher(request, hosts, **kwargs):
            calls.append((request['params']['task'], len(store.state['quota_attempts'])))
            if len(calls) == 1:
                raise TimeoutError('source read timed out')
            self.assertEqual(len(store.state['rejected_receipts']), 1)
            self.assertEqual(store.state['accepted_responses'], 0)
            return 200, b'{"ok":true}', {}

        report = campaign.run_campaign(
            store, Adapter([task('one')]), TODAY,
            settings(max_requests=2, transport_retries=1,
                     min_request_interval_seconds=2, max_inline_wait_seconds=5),
            fetcher=fetcher, clock=clock, sleeper=clock.sleep,
        )
        self.assertEqual(calls, [('one', 1), ('one', 2)])
        self.assertEqual(store.state['quota_attempts'], [1000.0, 1002.0])
        self.assertEqual(report['failed_attempts'], 1)
        self.assertEqual(report['failed_requests'], 0)
        self.assertEqual(report['transport_retry_attempts'], 1)
        self.assertEqual(report['recovered_transport_failures'], 1)
        self.assertEqual(report['errors'][0]['error_type'], 'TimeoutError')
        rejected = json.loads(store.objects[store.state['rejected_receipts'][0]['object_id']])
        self.assertFalse(rejected['accepted'])
        self.assertEqual(len(store.state['receipts']), 1)
        self.assertEqual(list(store.state['completed']), ['one'])
        self.assertEqual(store.state['pending'], [])

    def test_exhaustion_keeps_both_rejections_and_defers_provider_retry(self):
        clock, store, calls = FakeClock(), MemoryStore(), []

        def fetcher(request, hosts, **kwargs):
            calls.append(request['params']['task'])
            raise TimeoutError('still timed out')

        report = campaign.run_campaign(
            store, Adapter([task('one'), task('untouched', 'history')]), TODAY,
            settings(max_requests=3, transport_retries=1), fetcher=fetcher, clock=clock,
        )
        self.assertEqual(calls, ['one', 'one'])
        self.assertEqual(len(store.state['quota_attempts']), 2)
        self.assertEqual(report['reason'], 'provider_retry_after')
        self.assertGreater(report['retry_after_seconds'], 0)
        self.assertEqual(report['failed_attempts'], 2)
        self.assertEqual(report['failed_requests'], 0)
        self.assertEqual(report['deferred_transport_failures'], 1)
        self.assertEqual(report['recovered_transport_failures'], 0)
        self.assertEqual(len(store.state['rejected_receipts']), 2)
        self.assertEqual([t['id'] for t in store.state['pending']], ['one', 'untouched'])
        self.assertEqual(store.state['pending'][0]['failures'], 2)

    def test_request_time_and_quota_budgets_prevent_retry_and_restart_obeys_backoff(self):
        for cutoff in ('request', 'time', 'quota'):
            with self.subTest(cutoff=cutoff):
                clock, store, calls = FakeClock(), MemoryStore(), []
                configured = settings(max_requests=2, max_run_seconds=5, transport_retries=1)
                if cutoff == 'request':
                    configured['max_requests'] = 1
                elif cutoff == 'quota':
                    configured.update(quota_windows=[{'seconds': 100, 'requests': 1}],
                                      max_run_seconds=600)

                def fetcher(*args, **kwargs):
                    calls.append('attempt')
                    if cutoff == 'time':
                        clock.now += 5
                    raise TimeoutError('timed out')

                adapter = Adapter([task('one')])
                report = campaign.run_campaign(
                    store, adapter, TODAY, configured, fetcher=fetcher, clock=clock,
                )
                self.assertEqual(len(calls), 1)
                self.assertEqual(len(store.state['quota_attempts']), 1)
                self.assertEqual(report['failed_requests'], 0)
                self.assertEqual(report['transport_retry_attempts'], 0)
                self.assertGreater(store.state['pending'][0]['retry_at'], clock())
                restarted = campaign.run_campaign(
                    store, adapter, TODAY, configured, fetcher=fetcher, clock=clock,
                )
                self.assertEqual(restarted['requests'], 0)
                self.assertEqual(len(calls), 1)

    def test_http_parser_certificate_and_resource_errors_are_not_transport_retries(self):
        failures = ('parser', 'status_429', 'exception_429', 'resource', 'certificate')
        for failure in failures:
            with self.subTest(failure=failure):
                clock, store, calls = FakeClock(), MemoryStore(), []

                def fetcher(*args, **kwargs):
                    calls.append('attempt')
                    if failure == 'status_429':
                        return 429, b'rate limited', {'retry-after': '60'}
                    if failure == 'exception_429':
                        raise HTTPError('https://api.example.test', 429, 'rate limited', {}, None)
                    if failure == 'resource':
                        raise OSError('local resource failure')
                    if failure == 'certificate':
                        raise URLError(ssl.SSLCertVerificationError('invalid certificate'))
                    return 200, b'invalid', {}

                def interpret(*args):
                    raise ValueError('invalid source payload')

                report = campaign.run_campaign(
                    store, Adapter([task('one')], interpreter=interpret), TODAY,
                    settings(max_requests=3, transport_retries=2), fetcher=fetcher, clock=clock,
                )
                self.assertEqual(len(calls), 1)
                self.assertEqual(report['failed_requests'], 1)
                self.assertEqual(report['transport_retry_attempts'], 0)
                if failure == 'status_429':
                    self.assertGreater(store.state['provider_retry_at'], clock())

    def test_uncertain_storage_failure_stops_without_retry_or_rejection(self):
        class UncertainError(OSError):
            uncertain = True

        class UncertainStore(MemoryStore):
            def put_raw(self, *args):
                raise UncertainError('write outcome unknown')

        store, calls = UncertainStore(), []

        def fetcher(*args, **kwargs):
            calls.append('attempt')
            return 200, b'body', {}

        with self.assertRaisesRegex(UncertainError, 'outcome unknown'):
            campaign.run_campaign(
                store, Adapter([task('one')]), TODAY, settings(transport_retries=2),
                fetcher=fetcher, clock=FakeClock(),
            )
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(store.state['quota_attempts']), 1)
        self.assertEqual(store.state['rejected_receipts'], [])

    def test_session_reports_recovery_and_publishes_the_accepted_data(self):
        clock, store, calls, publications = FakeClock(), MemoryStore(), [], []

        def fetcher(*args, **kwargs):
            calls.append('attempt')
            if len(calls) == 1:
                raise ConnectionResetError('remote disconnected')
            return 200, b'body', {}

        report = run_collection_session(
            store, Adapter([task('one')]), settings(transport_retries=1),
            today_factory=lambda: TODAY, clock=clock, max_cycles=1,
            fetcher=fetcher,
            publish=lambda current, adapter, sha: publications.append(current.state['accepted_responses']),
        )
        self.assertEqual(report['failed_attempts'], 1)
        self.assertEqual(report['failed_requests'], 0)
        self.assertEqual(report['deferred_transport_failures'], 0)
        self.assertEqual(report['recovered_transport_failures'], 1)
        self.assertEqual(publications, [1])


if __name__ == '__main__':
    unittest.main()
