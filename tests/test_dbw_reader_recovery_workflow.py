"""Reader-only recovery never republishes or recovers owners."""
from pathlib import Path
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]

class ReaderRecoveryWorkflowTests(unittest.TestCase):
    def test_reader_only_is_separate_from_production_writer(self):
        workflow = yaml.load((ROOT/'.github/workflows/dbw-bronze-release.yml').read_text(), Loader=yaml.BaseLoader)
        inputs = workflow['on']['workflow_dispatch']['inputs']
        self.assertEqual(inputs['operation']['options'], ['publish', 'verify_only'])
        self.assertEqual(inputs['operation']['default'], 'publish')
        writer = workflow['jobs']['publish']
        reader = workflow['jobs']['verify_readers']
        self.assertIn("inputs.operation == 'publish'", writer['if'])
        self.assertIn("github.ref == 'refs/heads/main'", reader['if'])
        self.assertIn("needs.validate.result == 'success'", reader['if'])
        self.assertEqual(reader['permissions'], {'contents': 'read'})
        self.assertEqual(reader['needs'], ['validate', 'publish'])
        steps = '\n'.join(step.get('run','') for step in reader['steps'])
        self.assertNotIn('publish_retained_dbw_bronze.py', steps)
        self.assertNotIn('--recover-stale-owner', steps)
        self.assertNotIn('ZOHELO_ALLOW_PRODUCTION_WRITES', str(reader))
        self.assertIn('--expected-snapshot "$EXPECTED_SNAPSHOT"', steps)
        self.assertIn('--expected-code-sha "$EXPECTED_PUBLICATION_CODE_SHA"', steps)
        validation = workflow['jobs']['validate']['steps']
        gate = next(step for step in validation if step.get('name') == 'Verify reviewed execution revision and operation inputs')
        self.assertIn("operation == 'verify_only'", gate['run'])
        self.assertIn("os.environ['RECOVER_DRIVE_OWNER']", gate['run'])
        full = next(step for step in validation if step.get('name') == 'Full credential-free data-platform checks before production')
        self.assertEqual(full['if'], "inputs.operation == 'publish'")

    def test_live_verifier_opens_the_home_query_before_waiting_for_editor(self):
        source = (ROOT/'portal/scripts/verify-dbw-live.mjs').read_text()
        self.assertLess(source.index("name: 'New SQL query'"), source.index("const editor ="))
        self.assertIn('evidence.pending_indicator_count !== 0', source)
        self.assertIn('manifestDigest', source)

if __name__ == '__main__':
    unittest.main()
