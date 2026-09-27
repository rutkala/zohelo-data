"""Credential-free proofs of complete UI-selection coverage and bounded recovery."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import bdl_web_partitions as p


def dimensions(years=31):
    return [{'id': 'years', 'year_axis': True, 'options': [{'value': str(1995+i), 'label': str(1995+i)} for i in range(years)]},
            {'id': 'sex', 'options': [{'value': str(i), 'label': str(i)} for i in range(3)]},
            {'id': 'age', 'options': [{'value': str(i), 'label': str(i)} for i in range(8)]}]


def plan(count=80):
    value = p.new_plan('P1313', 'https://bdl.stat.gov.pl/bdl/dane/podgrup/wymiary/2/2/1313')
    p.accept_dimensions(value, p.next_task(value, set()), dimensions())
    p.accept_layouts(value, p.next_task(value, set()), [{'id': 'layout', 'kind': 'select', 'value': '1', 'label': 'Układ administracyjny'}])
    p.accept_territories(value, p.next_task(value, set()), [{'value': str(i), 'label': 'Bolesławiec (1)' if i == 7 else str(i)} for i in range(count)], count)
    return value


def receipt(value, node, size=20):
    return {'subgroup_id': value['subgroup_id'], 'selection_id': node['id'], 'selection': node['scope'],
            'landing_scope': 'native_bytes_only', 'content_validation': 'not_performed',
            'archive_object': {'id': 'drive-'+node['id'], 'sha256': 'a'*64, 'md5': 'b'*32, 'size': size}}


class SelectionPartitionsTests(unittest.TestCase):
    def test_manual_reference_is_a_partition_not_full_subgroup(self):
        q = plan(4579)
        n = p.next_task(q, set())
        self.assertEqual(['7'], n['scope']['territories'])
        self.assertEqual(31, len(n['scope']['dimensions']['years']))
        self.assertEqual(3, len(n['scope']['dimensions']['sex']))
        self.assertEqual(8, len(n['scope']['dimensions']['age']))
        p.accept_download(q, n, receipt(q, n))
        self.assertFalse(p.summary(q)['complete'])
        self.assertEqual(25, len(p.next_task(q, set())['scope']['territories']))

    def test_all_4579_identifiers_are_covered_exactly_once(self):
        q = plan(4579)
        leaves = [n for n in q['nodes'].values() if n['status'] == 'pending']
        ids = [x for n in leaves for x in n['scope']['territories']]
        self.assertEqual(4579, len(ids))
        self.assertEqual(set(map(str, range(4579))), set(ids))

    def test_bounded_retry_then_binary_split_preserves_dimensions(self):
        q = plan()
        n = next(n for n in q['nodes'].values() if len(n['scope'].get('territories') or []) > 1)
        original = deepcopy(n['scope'])
        self.assertEqual('retry', p.failed(q, n, '500', 'provider_error'))
        self.assertEqual('split', p.failed(q, n, '500', 'provider_error'))
        children = [q['nodes'][k]['scope'] for k in n['children']]
        self.assertEqual(set(original['territories']), set(children[0]['territories']+children[1]['territories']))
        self.assertTrue(all(c['dimensions'] == original['dimensions'] for c in children))

    def test_single_territory_failure_splits_years_without_loss(self):
        q = plan(1)
        n = p.next_task(q, set())
        p.failed(q, n, '500', 'provider_error')
        p.failed(q, n, '500', 'provider_error')
        self.assertEqual('years', n['axis'])
        children = [q['nodes'][k] for k in n['children']]
        self.assertEqual(set(n['scope']['dimensions']['years']), set(v for c in children for v in c['scope']['dimensions']['years']))
        self.assertTrue(all(c['scope']['territories'] == n['scope']['territories'] for c in children))

    def test_irreducible_failure_is_outstanding_not_complete(self):
        q = p.new_plan('P1', 'test')
        p.accept_dimensions(q, p.next_task(q, set()), [{'id':'year','options':[{'value':'2025'}]}])
        p.accept_layouts(q, p.next_task(q, set()), [{'id':'l','kind':'select','value':'1'}])
        p.accept_territories(q, p.next_task(q, set()), [{'value':'1','label':'one'}], 1)
        n = p.next_task(q, set())
        p.failed(q, n, '500', 'provider_error')
        self.assertEqual('blocked', p.failed(q, n, '500', 'provider_error'))
        self.assertIsNone(p.next_task(q, set()))
        self.assertEqual(1, p.summary(q)['blocked_selections'])
        self.assertFalse(p.summary(q)['complete'])

    def test_authentication_and_protocol_errors_never_trigger_splits(self):
        for error in ['authentication_or_site', 'ui_protocol', 'rate_limit', 'storage_error']:
            q = plan()
            n = p.next_task(q, set())
            count = len(q['nodes'])
            self.assertEqual('stop', p.failed(q, n, 'error', error))
            self.assertEqual(count, len(q['nodes']))
            self.assertFalse(p.summary(q)['complete'])

    def test_restart_resumes_only_outstanding_partitions(self):
        q = plan()
        first = p.next_task(q, set())
        p.accept_download(q, first, receipt(q, first))
        restored = json.loads(json.dumps(q))
        self.assertNotEqual(first['id'], p.next_task(restored, set())['id'])
        self.assertEqual(1, p.summary(restored)['files'])

    def test_complete_requires_all_leaves_and_no_gaps(self):
        q = plan(4579)
        while (n := p.next_task(q, set())) is not None:
            p.accept_download(q, n, receipt(q, n))
        result = p.summary(q)
        self.assertTrue(result['complete'])
        self.assertEqual(result['files']*20, result['bytes'])
        self.assertEqual(0, result['outstanding_selections'])

    def test_wrong_subgroup_or_selection_receipt_is_rejected(self):
        for key, wrong in [('subgroup_id', 'P2'), ('selection_id', 'bad'), ('landing_scope', 'parsed')]:
            q = plan()
            n = p.next_task(q, set())
            r = receipt(q, n)
            r[key] = wrong
            with self.assertRaises(ValueError):
                p.accept_download(q, n, r)
            self.assertFalse(p.summary(q)['complete'])

    def test_zero_bytes_and_missing_native_reference_are_rejected(self):
        for obj in [{}, {'id':'x','sha256':'a'*64,'size':0}]:
            q = plan()
            n = p.next_task(q, set())
            r = receipt(q, n)
            r['archive_object'] = obj
            with self.assertRaises(ValueError):
                p.accept_download(q, n, r)

    def test_territory_count_mismatch_cannot_finish_inventory(self):
        q = p.new_plan('P1', 'test')
        p.accept_dimensions(q, p.next_task(q, set()), dimensions())
        p.accept_layouts(q, p.next_task(q, set()), [{'value':'1','id':'l','kind':'select'}])
        with self.assertRaisesRegex(ValueError, 'exhaustion'):
            p.accept_territories(q, p.next_task(q, set()), [{'value':'1','label':'one'}], 2)

    def test_duplicate_or_missing_provider_ids_fail_closed(self):
        for data in [[{'value':'1'},{'value':'1'}], [{'value':''}], []]:
            with self.assertRaises(ValueError):
                p.values(data)

    def test_large_dimension_selection_is_split_before_request(self):
        q = p.new_plan('P1', 'test')
        p.accept_dimensions(q, p.next_task(q, set()), dimensions(200))
        n = p.next_task(q, set())
        self.assertEqual('layouts', n['scope']['kind'])
        self.assertLessEqual(len(n['scope']['dimensions']['years'])*24,3500)
        p.validate(q)

    def test_all_layouts_must_be_planned(self):
        q = p.new_plan('P1', 'test')
        p.accept_dimensions(q, p.next_task(q, set()), dimensions())
        n = p.next_task(q, set())
        p.accept_layouts(q, n, [{'id':'l','value':'1','kind':'select'}, {'id':'l','value':'2','kind':'select'}])
        self.assertEqual(2, len(n['children']))
        self.assertFalse(p.summary(q)['complete'])
        n['children'].pop()
        with self.assertRaises(ValueError):
            p.validate(q)

    def test_orphaned_work_cannot_be_ignored_at_completion(self):
        q = plan()
        extra = p.task('download', {'x':['1']}, {'value':'1'}, ['orphan'])
        q['nodes'][extra['id']] = extra
        with self.assertRaisesRegex(ValueError, 'Unreachable'):
            p.summary(q)

    def test_partial_scope_tampering_breaks_identity(self):
        q = plan()
        p.next_task(q,set())['scope']['territories'].append('extra')
        with self.assertRaisesRegex(ValueError, 'identity|gaps|overlaps'):
            p.summary(q)


if __name__ == '__main__':
    unittest.main()
