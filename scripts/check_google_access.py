"""Check the production Python credential path using read-only Drive requests.

Output contains only fixed status codes and configuration-presence booleans.
Never print credentials, exception bodies, account identities or Drive file IDs.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from storage_manager import StorageManager


CREDENTIAL_VARIABLES = (
    "GOOGLE_OAUTH_CLIENT_ID",
    "GOOGLE_OAUTH_CLIENT_SECRET",
    "GOOGLE_OAUTH_REFRESH_TOKEN",
    "GCP_SERVICE_ACCOUNT_JSON",
)
SAFE_OAUTH_ERRORS = {
    "invalid_grant", "invalid_client", "unauthorized_client", "invalid_scope",
    "access_denied", "admin_policy_enforced", "temporarily_unavailable", "server_error",
}


def classify_error(exc):
    """Use typed/allowlisted error codes, never exception text or HTTP payloads."""
    current = exc
    for _ in range(6):
        if isinstance(current, RefreshError):
            for argument in current.args:
                if isinstance(argument, dict) and argument.get("error") in SAFE_OAUTH_ERRORS:
                    return "oauth_" + argument["error"]
            return "refresh_rejected"
        if isinstance(current, HttpError):
            status = getattr(current.resp, "status", None)
            if status in (400, 401, 403, 404, 429, 500, 502, 503, 504):
                return "drive_http_" + str(status)
            return "drive_api_error"
        if isinstance(current, (ValueError, TypeError)):
            return "credential_configuration_error"
        current = current.__cause__
        if current is None:
            break
    return "google_access_check_failed"


def check_access(storage_factory=StorageManager, environ=None):
    env = os.environ if environ is None else environ
    report = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": "github_actions" if env.get("GITHUB_ACTIONS") == "true" else (
            "codespaces" if env.get("CODESPACES") == "true" else "local"
        ),
        "configuration_present": {name: bool(env.get(name)) for name in CREDENTIAL_VARIABLES},
        "status": "not_checked",
        "read_only_diagnostic": True,
        "actual_writes_tested": False,
    }

    # Avoid guessing consent or starting a browser when only an application's ID/secret exist.
    if not env.get("GCP_SERVICE_ACCOUNT_JSON"):
        missing = [name for name in CREDENTIAL_VARIABLES[:3] if not env.get(name)]
        if missing:
            report.update(status="missing_oauth_configuration", missing_variables=missing)
            return report

    try:
        manager = storage_factory(backend="gdrive", allow_interactive_auth=False)
        report["credential_source"] = manager.auth_source
        # No account identity is returned or logged. This forces lazy credentials to authenticate.
        manager.drive_service.about().get(fields="kind").execute()
        report["drive_api_access"] = True

        folder_name = manager.master_folder_name.replace("\\", "\\\\").replace("'", "\\'")
        response = manager.drive_service.files().list(
            q=f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces="drive",
            fields="nextPageToken,files(id,capabilities(canListChildren,canAddChildren))",
            pageSize=2,
        ).execute()
        folders = response.get("files", [])
        if not folders:
            report["status"] = "platform_folder_not_visible"
            return report
        if len(folders) != 1 or response.get("nextPageToken"):
            report["status"] = "ambiguous_platform_folder"
            return report
        capabilities = folders[0].get("capabilities", {})
        report["platform_folder_visible"] = True
        report["can_list_children"] = capabilities.get("canListChildren") is True
        report["can_add_children"] = capabilities.get("canAddChildren") is True
        report["status"] = (
            "read_access_verified" if report["can_list_children"] else "folder_read_permission_missing"
        )
        return report
    except Exception as exc:
        report["status"] = classify_error(exc)
        return report


def main():
    # Keep third-party logging from emitting HTTP details if a caller enabled debug logging.
    logging.disable(logging.CRITICAL)
    report = check_access()
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        Path(summary_path).write_text(
            "### Google Drive access check\n\n```json\n" + rendered + "\n```\n\n"
            "This checks the selected Python credentials and folder visibility. "
            "It does not create, update or delete data, prove uploads, validate a browser login, "
            "or inspect another Codespace. See docs/google-authorization.md.\n"
        )
    return 0 if report["status"] == "read_access_verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
