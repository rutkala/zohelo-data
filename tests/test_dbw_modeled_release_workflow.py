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
        validation = value["jobs"]["validate_dispatch"]
        validation_text = str(validation)
        self.assertNotIn("if", validation)
        self.assertIn("refs/heads/main", validation_text)
        self.assertIn("REQUESTED_CODE_SHA", validation_text)
        self.assertIn("publish-reviewed-dbw", validation_text)
        self.assertIn("exit 1", validation_text)
        publish = value["jobs"]["prepare_build_validate_and_publish"]
        self.assertEqual(publish["needs"], "validate_dispatch")
        self.assertIn("needs.validate_dispatch.result == 'success'", publish["if"])
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
        self.assertEqual(
            publish["outputs"]["release_id"],
            "${{ steps.publish.outputs.release_id }}",
        )
        self.assertIn("--verify-current", str(verify))
        self.assertIn("--expected-release-id", str(verify))
        self.assertIn(
            "needs.prepare_build_validate_and_publish.outputs.release_id",
            str(verify),
        )
        self.assertNotIn("ZOHELO_ALLOW_PRODUCTION_WRITES", str(verify))

    def test_drive_credentials_are_scoped_to_execution_steps(self):
        value = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
        jobs = value["jobs"]
        publish = jobs["prepare_build_validate_and_publish"]
        verify = jobs["verify_fresh_consumer"]
        self.assertNotIn("env", publish)
        self.assertNotIn("env", verify)

        publish_steps = publish["steps"]
        verify_steps = verify["steps"]
        for step in [*publish_steps[:-1], *verify_steps[:-1]]:
            self.assertNotIn("GOOGLE_OAUTH_CLIENT_SECRET", str(step))
            self.assertNotIn("GOOGLE_OAUTH_REFRESH_TOKEN", str(step))

        publish_env = publish_steps[-1]["env"]
        verify_env = verify_steps[-1]["env"]
        self.assertEqual(publish_env["ZOHELO_ALLOW_PRODUCTION_WRITES"], "true")
        self.assertIn("secrets.GOOGLE_OAUTH_REFRESH_TOKEN", str(publish_env))
        self.assertIn("secrets.GOOGLE_OAUTH_REFRESH_TOKEN", str(verify_env))
        self.assertNotIn("ZOHELO_ALLOW_PRODUCTION_WRITES", verify_env)

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

    def test_existing_release_navigation_is_synchronized_then_finalized(self):
        storage = object()
        manifest = {"release_id": "00000000-0000-0000-0000-000000000001"}
        with mock.patch.object(
            release, "sync_source_medallion_navigation"
        ) as synchronize, mock.patch.object(
            release, "finalize_source_medallion_navigation"
        ) as finalize:
            release._finalize_existing_navigation(storage, "root", manifest)

        synchronize.assert_called_once_with(
            storage, "root", "dbw", manifest, finalize=False
        )
        finalize.assert_called_once_with(storage, "root", "dbw", manifest)

    def test_existing_release_root_without_pointer_is_retryable(self):
        store = mock.Mock(root_id="release-root")
        store.find.return_value = []
        with mock.patch.object(release, "read_current_release_manifest") as read:
            self.assertIsNone(release._read_optional_current_release(store))
        store.find.assert_called_once_with("current-release.json", "release-root")
        read.assert_not_called()

    def test_existing_pointer_errors_still_fail_closed(self):
        store = mock.Mock(root_id="release-root")
        store.find.return_value = ["pointer-id"]
        with mock.patch.object(
            release,
            "read_current_release_manifest",
            side_effect=release.ReleaseProtocolError("invalid current pointer"),
        ):
            with self.assertRaisesRegex(
                release.ReleaseProtocolError, "invalid current pointer"
            ):
                release._read_optional_current_release(store)

    def test_fresh_consumer_validates_the_pointer_whose_identity_was_checked(self):
        store = object()
        pointer = {
            "release_id": "00000000-0000-0000-0000-000000000001",
            "manifest_file_id": "manifest-id",
        }
        manifest = {
            "release_id": pointer["release_id"],
            "release_scope": release.DBW_RELEASE_SCOPE,
        }
        report = {"release_id": pointer["release_id"]}
        with mock.patch.object(
            release, "_current_pointer", return_value=pointer
        ) as current, mock.patch.object(
            release, "read_release_manifest", return_value=manifest
        ) as read, mock.patch.object(
            release, "validate_staged_dbw_release", return_value=report
        ) as validate:
            self.assertIs(
                release._validate_expected_current_release(
                    store, pointer["release_id"]
                ),
                report,
            )

        current.assert_called_once_with(store)
        read.assert_called_once_with(store, pointer)
        self.assertIs(validate.call_args.args[1], pointer)

    def test_fresh_consumer_rejects_another_workflows_current_release(self):
        manifest = {"release_id": "00000000-0000-0000-0000-000000000001"}
        release._require_expected_release(manifest, manifest["release_id"])
        with self.assertRaisesRegex(
            release.ReleaseProtocolError, "differs from the release published"
        ):
            release._require_expected_release(
                manifest, "00000000-0000-0000-0000-000000000002"
            )


if __name__ == "__main__":
    unittest.main()
