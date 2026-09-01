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
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "revoked-refresh-token",
            "GITHUB_ACTIONS": "true",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.ServiceAccountCredentials.from_service_account_info")
    def test_prefers_service_account_json_over_oauth_env_vars(
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
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "revoked-refresh-token",
            "GITHUB_ACTIONS": "true",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.ServiceAccountCredentials.from_service_account_info")
    def test_prefers_service_account_like_json_when_type_missing(
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
            "GOOGLE_OAUTH_CLIENT_ID": "oauth-client-id",
            "GOOGLE_OAUTH_CLIENT_SECRET": "oauth-client-secret",
            "GOOGLE_OAUTH_REFRESH_TOKEN": "revoked-refresh-token",
            "GITHUB_ACTIONS": "true",
        },
        clear=True,
    )
    @patch("storage_manager.build", return_value="drive-service")
    @patch("storage_manager.UserCredentials")
    @patch("storage_manager.load_credentials_from_dict")
    def test_prefers_typed_google_credentials_json_over_oauth_env_vars(
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
