"""Policy checks for the manual serialized modeled-DBW release workflow."""
from pathlib import Path
import os
import sys
import unittest
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/dbw-modeled-release.yml"
sys.path.insert(0, str(ROOT / "src"))

import dbw_platform_release as release  # noqa: E402


class DBWModeledReleaseWorkflowTests(unittest.TestCase):
    def test_workflow_is_manual_main_only_and_serialized(self):
        value = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(value["on"]), {"workflow_dispatch"})
        self.assertEqual(value["permissions"], {"contents": "read"})
        publish = value["jobs"]["prepare_build_validate_and_publish"]
        self.assertIn("github.ref == 'refs/heads/main'", publish["if"])
        self.assertIn("inputs.code_sha == github.sha", publish["if"])
        self.assertEqual(
            publish["concurrency"]["group"], "zohelo-production-data"
        )
        self.assertEqual(publish["concurrency"]["cancel-in-progress"], "false")
        self.assertEqual(publish["concurrency"]["queue"], "max")

    def test_build_and_promotion_share_one_runner_but_consumer_is_fresh(self):
        value = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        jobs = value["jobs"]
        publish = jobs["prepare_build_validate_and_publish"]
        verify = jobs["verify_fresh_consumer"]
        publish_text = str(publish)
        self.assertIn("dbw_platform_release.py", publish_text)
        self.assertIn("--allow-production-write", publish_text)
        self.assertEqual(
            verify["needs"], "prepare_build_validate_and_publish"
        )
        self.assertIn("--verify-current", str(verify))
        self.assertNotIn("ZOHELO_ALLOW_PRODUCTION_WRITES", str(verify))

    def test_workflow_never_uploads_the_restored_cache_as_an_artifact(self):
        text = WORKFLOW.read_text()
        self.assertNotIn("actions/upload-artifact", text)
        self.assertNotIn("verified-cache", text)
        self.assertNotIn("descriptor-inventory", text)

    def test_write_guard_requires_exact_main_actions_checkout_and_opt_in(self):
        sha = "a" * 40
        clean_environment = {
            key: value
            for key, value in os.environ.items()
            if key not in {
                "GITHUB_ACTIONS",
                "GITHUB_REF",
                "ZOHELO_ALLOW_PRODUCTION_WRITES",
            }
        }
        with mock.patch.object(release, "_code_sha", return_value=sha):
            with mock.patch.dict(os.environ, clean_environment, clear=True):
                with self.assertRaisesRegex(PermissionError, "main-branch Actions"):
                    release._require_actions_main(
                        expected_code_sha=sha,
                        allow_production_write=True,
                        require_write=True,
                    )
            with mock.patch.dict(
                os.environ,
                {
                    **clean_environment,
                    "GITHUB_ACTIONS": "true",
                    "GITHUB_REF": "refs/heads/main",
                },
                clear=True,
            ):
                with self.assertRaisesRegex(PermissionError, "production-write opt-in"):
                    release._require_actions_main(
                        expected_code_sha=sha,
                        allow_production_write=True,
                        require_write=True,
                    )
            with mock.patch.dict(
                os.environ,
                {
                    **clean_environment,
                    "GITHUB_ACTIONS": "true",
                    "GITHUB_REF": "refs/heads/main",
                    "ZOHELO_ALLOW_PRODUCTION_WRITES": "true",
                },
                clear=True,
            ):
                self.assertEqual(
                    release._require_actions_main(
                        expected_code_sha=sha,
                        allow_production_write=True,
                        require_write=True,
                    ),
                    sha,
                )

    def test_fresh_consumer_requires_exact_reviewed_checkout_without_write_opt_in(self):
        sha = "b" * 40
        with mock.patch.object(release, "_code_sha", return_value=sha), mock.patch.dict(
            os.environ,
            {"GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/main"},
            clear=True,
        ):
            with self.assertRaisesRegex(PermissionError, "exact checked-out Git SHA"):
                release._require_actions_main(
                    expected_code_sha="c" * 40,
                    allow_production_write=False,
                    require_write=False,
                )
            self.assertEqual(
                release._require_actions_main(
                    expected_code_sha=sha,
                    allow_production_write=False,
                    require_write=False,
                ),
                sha,
            )


if __name__ == "__main__":
    unittest.main()
