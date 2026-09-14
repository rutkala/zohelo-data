import copy
from hashlib import sha256
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import verify_migration_plan as verifier

class MigrationPlanProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.commit = "a" * 40
        self.root = "root-id"
        self.run_id = "12345"
        self.repo = "owner/repo"
        self.plan = {
            "status": "planned", "plan_id": "plan-one",
            "root_id": self.root, "expected_root_id": self.root,
            "steps": [], "pins": {},
        }
        self.digest = sha256(json.dumps(
            self.plan, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        self.plan["plan_sha256"] = self.digest
        self.identity = {
            "format_version": 1, "repository": self.repo,
            "workflow_path": verifier.WORKFLOW_PATH,
            "workflow_run_id": self.run_id, "workflow_commit": self.commit,
            "plan_id": "plan-one", "plan_sha256": self.digest,
            "root_id": self.root,
        }
        self.run = {
            "id": int(self.run_id), "repository": {"full_name": self.repo},
            "path": verifier.WORKFLOW_PATH, "event": "workflow_dispatch",
            "head_branch": "main", "status": "completed",
            "conclusion": "success", "head_sha": self.commit,
        }

    def verify(self, **changes):
        values = {
            "plan": copy.deepcopy(self.plan), "identity": copy.deepcopy(self.identity),
            "run_id": self.run_id, "expected_root_id": self.root, "expected_plan_id": "plan-one",
            "expected_plan_sha256": self.digest, "repository": self.repo,
            "executing_commit": self.commit, "token": "token",
        }
        values.update(changes)
        with mock.patch.object(verifier, "_json_request", return_value=copy.deepcopy(self.run)):
            return verifier.verify_provenance(**values)

    def test_exact_successful_workflow_identity_is_accepted(self):
        result = self.verify()
        self.assertEqual("reviewed_plan_verified", result["status"])
        self.assertEqual(self.digest, result["plan_sha256"])

    def test_rejects_malformed_run_sha_hash_and_plan_tampering_before_api(self):
        cases = {
            "run": {"run_id": "01x"},
            "sha": {"executing_commit": "A" * 40},
            "hash": {"expected_plan_sha256": "g" * 64},
            "plan": {"plan": {**self.plan, "root_id": "other"}},
        }
        for name, changes in cases.items():
            with self.subTest(name=name), mock.patch.object(verifier, "_json_request") as request:
                with self.assertRaises(verifier.ProvenanceError):
                    self.verify(**changes)
                request.assert_not_called()

    def test_rejects_any_identity_or_dispatch_mismatch(self):
        for field, value in (
            ("repository", "other/repo"), ("workflow_path", "deploy.yml"),
            ("workflow_run_id", "999"), ("workflow_commit", "b"*40),
            ("plan_id", "other"), ("plan_sha256", "b"*64), ("root_id", "other"),
        ):
            with self.subTest(field=field):
                identity = copy.deepcopy(self.identity)
                identity[field] = value
                with self.assertRaises(verifier.ProvenanceError):
                    self.verify(identity=identity)

    def test_rejects_non_main_unsuccessful_wrong_workflow_run(self):
        variants = {
            "id": 999, "repository": {"full_name": "other/repo"},
            "path": ".github/workflows/deploy.yml", "event": "push",
            "head_branch": "feature", "status": "in_progress",
            "conclusion": "failure", "head_sha": "b"*40,
        }
        for field, value in variants.items():
            with self.subTest(field=field):
                run = copy.deepcopy(self.run)
                run[field] = value
                with mock.patch.object(verifier, "_json_request", return_value=run):
                    with self.assertRaises(verifier.ProvenanceError):
                        verifier.verify_provenance(
                            plan=copy.deepcopy(self.plan), identity=copy.deepcopy(self.identity),
                            run_id=self.run_id, expected_root_id=self.root, expected_plan_id="plan-one",
                            expected_plan_sha256=self.digest, repository=self.repo,
                            executing_commit=self.commit, token="token",
                        )

    def test_api_errors_fail_closed(self):
        for error in (
            verifier.ProvenanceError("403"), verifier.ProvenanceError("404"),
            verifier.ProvenanceError("network"),
        ):
            with self.subTest(error=str(error)), mock.patch.object(
                verifier, "_json_request", side_effect=error
            ):
                with self.assertRaises(verifier.ProvenanceError):
                    verifier.verify_provenance(
                        plan=copy.deepcopy(self.plan), identity=copy.deepcopy(self.identity),
                        run_id=self.run_id, expected_root_id=self.root, expected_plan_id="plan-one",
                        expected_plan_sha256=self.digest, repository=self.repo,
                        executing_commit=self.commit, token="token",
                    )

if __name__ == "__main__":
    unittest.main()
