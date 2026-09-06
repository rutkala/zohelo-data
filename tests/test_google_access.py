import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError


spec = importlib.util.spec_from_file_location(
    "check_google_access", Path(__file__).resolve().parents[1] / "scripts" / "check_google_access.py"
)
diagnostic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostic)

SYNTHETIC_CONFIG = {
    "GOOGLE_OAUTH_CLIENT_ID": "synthetic-client-id",
    "GOOGLE_OAUTH_CLIENT_SECRET": "synthetic-client-secret",
    "GOOGLE_OAUTH_REFRESH_TOKEN": "synthetic-refresh-token",
}


class GoogleAccessDiagnosticTests(unittest.TestCase):
    def test_client_identity_without_consent_token_never_attempts_auth(self):
        factory = MagicMock()
        config = {k: v for k, v in SYNTHETIC_CONFIG.items() if k != "GOOGLE_OAUTH_REFRESH_TOKEN"}
        report = diagnostic.check_access(factory, config)
        self.assertEqual(report["status"], "missing_oauth_configuration")
        self.assertEqual(report["missing_variables"], ["GOOGLE_OAUTH_REFRESH_TOKEN"])
        factory.assert_not_called()

    def test_wrapped_refresh_error_is_reported_without_any_secret_or_exception_text(self):
        factory = MagicMock()
        cause = RefreshError("sensitive-provider-message", {"error": "invalid_grant", "detail": "private-detail"})
        failure = RuntimeError("sensitive-wrapper")
        failure.__cause__ = cause
        factory.side_effect = failure
        report = diagnostic.check_access(factory, SYNTHETIC_CONFIG)
        self.assertEqual(report["status"], "oauth_invalid_grant")
        serialized = json.dumps(report)
        for sensitive in [*SYNTHETIC_CONFIG.values(), "sensitive-provider-message", "private-detail", "sensitive-wrapper"]:
            self.assertNotIn(sensitive, serialized)

    def test_read_probe_does_not_invoke_any_storage_mutator(self):
        factory = MagicMock()
        manager = factory.return_value
        manager.auth_source = "oauth_environment"
        manager.master_folder_name = "zohelo-data"
        manager.drive_service.files().list().execute.return_value = {
            "files": [{"id": "private-file-id", "capabilities": {"canListChildren": True, "canAddChildren": True}}]
        }
        report = diagnostic.check_access(factory, SYNTHETIC_CONFIG)
        self.assertEqual(report["status"], "read_access_verified")
        self.assertTrue(report["can_add_children"])
        self.assertFalse(report["actual_writes_tested"])
        self.assertNotIn("private-file-id", json.dumps(report))
        factory.assert_called_once_with(backend="gdrive", allow_interactive_auth=False)
        manager.init_infrastructure.assert_not_called()
        manager._get_or_create_folder.assert_not_called()
        for method in ("create", "update", "delete"):
            getattr(manager.drive_service.files(), method).assert_not_called()

    def test_missing_or_ambiguous_folders_are_reported_without_creating_them(self):
        for response, status in (
            ({"files": []}, "platform_folder_not_visible"),
            ({"files": [{"id": "one"}, {"id": "two"}]}, "ambiguous_platform_folder"),
        ):
            with self.subTest(status=status):
                factory = MagicMock()
                factory.return_value.auth_source = "oauth_environment"
                factory.return_value.master_folder_name = "zohelo-data"
                factory.return_value.drive_service.files().list().execute.return_value = response
                report = diagnostic.check_access(factory, SYNTHETIC_CONFIG)
                self.assertEqual(report["status"], status)
                factory.return_value.drive_service.files().create.assert_not_called()

    def test_http_error_does_not_expose_response_body(self):
        response = MagicMock(status=403)
        error = HttpError(response, b'{"error":{"message":"private-account-detail"}}')
        self.assertEqual(diagnostic.classify_error(error), "drive_http_403")

    @patch.dict("os.environ", {
        "GCP_SERVICE_ACCOUNT_JSON": '{"web":{"client_id":"synthetic-id","client_secret":"synthetic-secret"}}',
    }, clear=True)
    @patch("storage_manager.InstalledAppFlow")
    def test_noninteractive_diagnostic_never_opens_browser_for_consent(self, flow):
        report = diagnostic.check_access()
        self.assertEqual(report["status"], "credential_configuration_error")
        flow.from_client_config.assert_not_called()


if __name__ == "__main__":
    unittest.main()
