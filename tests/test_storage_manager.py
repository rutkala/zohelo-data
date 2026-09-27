import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import storage_manager


class StorageManagerAuthTests(unittest.TestCase):
    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": '{"type":"service_account","client_email":"svc@example.com","private_key":"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n","token_uri":"https://oauth2.googleapis.com/token"}',
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.ServiceAccountCredentials.from_service_account_info")
    def test_uses_service_account_json_when_oauth_env_is_absent(
        self,
        from_service_account_info,
        user_credentials,
        build,
    ):
        credentials = object()
        from_service_account_info.return_value = credentials

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.drive_service, "drive-service")
        from_service_account_info.assert_called_once()
        user_credentials.assert_not_called()
        build.assert_called_once_with("drive", "v3", credentials=credentials)

    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": '{"client_email":"svc@example.com","private_key":"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n"}',
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.ServiceAccountCredentials.from_service_account_info")
    def test_uses_service_account_like_json_when_oauth_env_is_absent(
        self,
        from_service_account_info,
        user_credentials,
        build,
    ):
        credentials = object()
        from_service_account_info.return_value = credentials

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.drive_service, "drive-service")
        from_service_account_info.assert_called_once_with(
            {
                "client_email": "svc@example.com",
                "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
                "type": "service_account",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        user_credentials.assert_not_called()
        build.assert_called_once_with("drive", "v3", credentials=credentials)

    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": '{"type":"authorized_user","client_id":"json-client-id","client_secret":"json-client-secret","refresh_token":"json-refresh-token"}',
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.load_credentials_from_dict")
    def test_uses_typed_google_credentials_json_when_oauth_env_is_absent(
        self,
        load_credentials_from_dict,
        user_credentials,
        build,
    ):
        credentials = object()
        load_credentials_from_dict.return_value = (credentials, "project-id")

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.drive_service, "drive-service")
        load_credentials_from_dict.assert_called_once_with(
            {
                "type": "authorized_user",
                "client_id": "json-client-id",
                "client_secret": "json-client-secret",
                "refresh_token": "json-refresh-token",
            },
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        user_credentials.assert_not_called()
        build.assert_called_once_with("drive", "v3", credentials=credentials)

    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": '{"type":"service_account","client_email":"svc@example.com","private_key":"fake-key"}',
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "valid-refresh-token",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.HttpLib2Request", return_value=object())
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.ServiceAccountCredentials.from_service_account_info")
    def test_oauth_env_wins_over_service_account_json(
        self,
        from_service_account_info,
        user_credentials,
        _http_request,
        build,
    ):
        credentials = MagicMock()
        user_credentials.return_value = credentials

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.auth_source, "oauth_environment")
        credentials.refresh.assert_called_once()
        from_service_account_info.assert_not_called()
        build.assert_called_once_with("drive", "v3", credentials=credentials)

    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": '{"client_email":"svc@example.com","private_key":"fake-key"}',
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "valid-refresh-token",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.HttpLib2Request", return_value=object())
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.ServiceAccountCredentials.from_service_account_info")
    def test_oauth_env_wins_over_untyped_service_account_json(
        self,
        from_service_account_info,
        user_credentials,
        _http_request,
        build,
    ):
        credentials = MagicMock()
        user_credentials.return_value = credentials

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.auth_source, "oauth_environment")
        from_service_account_info.assert_not_called()
        build.assert_called_once_with("drive", "v3", credentials=credentials)

    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": '{"type":"authorized_user","client_id":"json-client-id","client_secret":"json-client-secret","refresh_token":"json-refresh-token"}',
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "valid-refresh-token",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.HttpLib2Request", return_value=object())
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.load_credentials_from_dict")
    def test_oauth_env_wins_over_typed_authorized_user_json(
        self,
        load_credentials_from_dict,
        user_credentials,
        _http_request,
        build,
    ):
        credentials = MagicMock()
        user_credentials.return_value = credentials

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.auth_source, "oauth_environment")
        load_credentials_from_dict.assert_not_called()
        build.assert_called_once_with("drive", "v3", credentials=credentials)

    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": "{unused malformed JSON",
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "valid-refresh-token",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.HttpLib2Request", return_value=object())
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.json.loads")
    def test_oauth_env_ignores_unused_malformed_json(
        self,
        json_loads,
        user_credentials,
        _http_request,
        build,
    ):
        credentials = MagicMock()
        user_credentials.return_value = credentials

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.auth_source, "oauth_environment")
        json_loads.assert_not_called()
        build.assert_called_once_with("drive", "v3", credentials=credentials)

    @patch.dict(os.environ, {"GCP_SERVICE_ACCOUNT_JSON": "{malformed JSON"}, clear=True)
    @patch("storage_manager.build")
    @patch("storage_manager.InstalledAppFlow.from_client_config")
    def test_malformed_json_still_fails_when_legacy_auth_is_selected(self, flow, build):
        with self.assertRaises(ValueError):
            storage_manager.StorageManager(allow_interactive_auth=False)
        flow.assert_not_called()
        build.assert_not_called()

    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": "{unused malformed JSON",
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "expired-refresh-token",
        },
        clear=True,
    )
    @patch("storage_manager.build")
    @patch("storage_manager.InstalledAppFlow.from_client_config")
    @patch("storage_manager.load_credentials_from_dict")
    @patch("storage_manager.ServiceAccountCredentials.from_service_account_info")
    @patch("storage_manager.json.loads")
    @patch("storage_manager.HttpLib2Request", return_value=object())
    @patch("storage_manager.UserCredentials")
    def test_oauth_refresh_failure_does_not_parse_or_fallback(
        self,
        user_credentials,
        _http_request,
        json_loads,
        from_service_account_info,
        load_credentials_from_dict,
        from_client_config,
        build,
    ):
        credentials = MagicMock()
        credentials.refresh.side_effect = ValueError("refresh failed")
        user_credentials.return_value = credentials

        with self.assertRaises(RuntimeError):
            storage_manager.StorageManager()

        json_loads.assert_not_called()
        from_service_account_info.assert_not_called()
        load_credentials_from_dict.assert_not_called()
        from_client_config.assert_not_called()
        build.assert_not_called()

    @patch.dict(
        os.environ,
        {
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "valid-refresh-token",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.HttpLib2Request", return_value=object())
    @patch("storage_manager.UserCredentials")
    def test_uses_explicit_oauth_env_vars_when_service_account_json_missing(
        self,
        user_credentials,
        _http_request,
        build,
    ):
        credentials = MagicMock()
        user_credentials.return_value = credentials

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.drive_service, "drive-service")
        credentials.refresh.assert_called_once()
        user_credentials.assert_called_once_with(
            token=None,
            refresh_token="valid-refresh-token",
            token_uri="https://oauth2.googleapis.com/token",
            client_id="oauth-client-id",
            client_secret="oauth-client-secret",
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        build.assert_called_once_with("drive", "v3", credentials=credentials)

    @patch.dict(
        os.environ,
        {
            "GCP_SERVICE_ACCOUNT_JSON": '{"installed":{"client_id":"json-client-id","token_uri":"https://oauth2.googleapis.com/token"}}',
            "GOOGLE_OAUTH_CLIENT_SECRET": "env-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "valid-refresh-token",
            "GITHUB_ACTIONS": "true",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.HttpLib2Request", return_value=object())
    @patch("storage_manager.UserCredentials")
    def test_fallback_oauth_path_uses_env_values_only_for_missing_json_fields(
        self,
        user_credentials,
        _http_request,
        build,
    ):
        credentials = MagicMock()
        user_credentials.return_value = credentials

        manager = storage_manager.StorageManager()

        self.assertEqual(manager.drive_service, "drive-service")
        credentials.refresh.assert_called_once()
        user_credentials.assert_called_once_with(
            token=None,
            refresh_token="valid-refresh-token",
            token_uri="https://oauth2.googleapis.com/token",
            client_id="json-client-id",
            client_secret="env-client-secret",
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        build.assert_called_once_with("drive", "v3", credentials=credentials)


if __name__ == "__main__":
    unittest.main()
