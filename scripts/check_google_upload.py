"""Run a bounded, self-cleaning Google Drive upload probe.

The probe is deliberately opt-in.  Without ``--allow-write-test`` this module
does not construct a storage manager and therefore performs no authentication
or network requests.
"""

import argparse
import hashlib
import io
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_DIR = _REPO_ROOT / "src"
_SCRIPTS_DIR = Path(__file__).resolve().parent
for _path in (str(_SRC_DIR), str(_SCRIPTS_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from check_google_access import classify_error
from storage_manager import StorageManager


PROBE_NAME_PREFIX = "zohelo-upload-check-"
PROBE_MARKER = "zohelo-upload-check-v1"
NONCE_PROPERTY = "zohelo_probe_nonce"
MARKER_PROPERTY = "zohelo_probe_marker"
TEXT_MIME_TYPE = "text/plain"
_KNOWN_AUTH_SOURCES = {
    "oauth_environment",
    "service_account_json",
    "authorized_user_json",
    "typed_credentials_json",
    "oauth_client_json",
}


def _environment(env):
    if env.get("GITHUB_ACTIONS") == "true":
        return "github_actions"
    if env.get("CODESPACES") == "true":
        return "codespaces"
    return "local"


def _drive_query_name(name):
    return name.replace("\\", "\\\\").replace("'", "\\'")


def _is_not_found(exc):
    return isinstance(exc, HttpError) and getattr(exc.resp, "status", None) == 404


def _execute(request, *, retries=None):
    """Execute a Drive request with bounded retries where requested."""
    if retries is None:
        return request.execute()
    return request.execute(num_retries=retries)


def _identity_matches(metadata, *, file_id, root_id, probe_name, nonce):
    if not isinstance(metadata, dict):
        return False
    parents = metadata.get("parents")
    app_properties = metadata.get("appProperties")
    return (
        metadata.get("id") == file_id
        and metadata.get("name") == probe_name
        and isinstance(parents, list)
        and root_id in parents
        and metadata.get("mimeType") == TEXT_MIME_TYPE
        and metadata.get("ownedByMe") is True
        and isinstance(app_properties, dict)
        and app_properties.get(NONCE_PROPERTY) == nonce
        and app_properties.get(MARKER_PROPERTY) == PROBE_MARKER
    )


def _metadata_fields():
    return "id,name,parents,mimeType,ownedByMe,appProperties,trashed"


def check_upload(*, allow_write_test=False, storage_factory=StorageManager):
    """Check one Drive upload, readback and cleanup, only when explicitly enabled."""
    report = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": _environment(os.environ),
        "write_test_enabled": bool(allow_write_test),
        "upload_attempted": False,
        "upload_verified": False,
        "readback_verified": False,
        "cleanup_verified": False,
        "cleanup_required": False,
        "status": "write_test_not_enabled" if not allow_write_test else "not_checked",
    }
    if not allow_write_test:
        return report

    manager = None
    service = None
    root_id = None
    allocated_id = None
    probe_name = None
    nonce = None
    create_attempted = False
    primary_status = None

    def fail(status):
        nonlocal primary_status
        if primary_status is None:
            primary_status = status
        report["status"] = primary_status

    try:
        manager = storage_factory(backend="gdrive", allow_interactive_auth=False)
        auth_source = getattr(manager, "auth_source", None)
        if auth_source in _KNOWN_AUTH_SOURCES:
            report["credential_source"] = auth_source
        if auth_source != "oauth_environment":
            fail("oauth_required_for_upload_test")
            return report

        service = manager.drive_service
        folder_name = _drive_query_name(manager.master_folder_name)
        response = _execute(
            service.files().list(
                q=(
                    f"name='{folder_name}' and "
                    "mimeType='application/vnd.google-apps.folder' and trashed=false"
                ),
                spaces="drive",
                fields="nextPageToken,files(id,name,capabilities(canAddChildren))",
                pageSize=2,
            )
        )
        folders = response.get("files", []) if isinstance(response, dict) else []
        if not folders:
            fail("platform_folder_not_visible")
            return report
        if len(folders) != 1 or response.get("nextPageToken"):
            fail("ambiguous_platform_folder")
            return report
        folder = folders[0]
        root_id = folder.get("id") if isinstance(folder, dict) else None
        capabilities = folder.get("capabilities", {}) if isinstance(folder, dict) else {}
        if not isinstance(root_id, str) or not root_id:
            fail("platform_folder_not_visible")
            return report
        if capabilities.get("canAddChildren") is not True:
            fail("folder_write_permission_missing")
            return report

        nonce = uuid.uuid4().hex
        probe_name = f"{PROBE_NAME_PREFIX}{nonce}.txt"
        payload = f"{PROBE_MARKER}\n{nonce}\n".encode("utf-8")
        if len(payload) >= 1024:
            fail("probe_payload_invalid")
            return report
        report["payload_bytes"] = len(payload)
        report["sha256"] = hashlib.sha256(payload).hexdigest()

        id_response = _execute(
            service.files().generateIds(count=1, space="drive", type="files")
        )
        generated_ids = id_response.get("ids", []) if isinstance(id_response, dict) else []
        if len(generated_ids) != 1 or not isinstance(generated_ids[0], str) or not generated_ids[0]:
            fail("probe_id_allocation_failed")
            return report
        allocated_id = generated_ids[0]

        body = {
            "id": allocated_id,
            "name": probe_name,
            "parents": [root_id],
            "mimeType": TEXT_MIME_TYPE,
            "appProperties": {
                NONCE_PROPERTY: nonce,
                MARKER_PROPERTY: PROBE_MARKER,
            },
        }
        media = MediaIoBaseUpload(
            io.BytesIO(payload), mimetype=TEXT_MIME_TYPE, resumable=False
        )
        create_attempted = True
        report["upload_attempted"] = True
        created = _execute(
            service.files().create(body=body, media_body=media, fields="id")
        )
        returned_id = created.get("id") if isinstance(created, dict) else None
        if returned_id != allocated_id:
            fail("upload_id_mismatch")
        else:
            metadata = _execute(
                service.files().get(fileId=allocated_id, fields=_metadata_fields()),
                retries=2,
            )
            if not _identity_matches(
                metadata,
                file_id=allocated_id,
                root_id=root_id,
                probe_name=probe_name,
                nonce=nonce,
            ):
                fail("upload_metadata_mismatch")
            else:
                report["upload_verified"] = True
                downloaded = _execute(
                    service.files().get_media(fileId=allocated_id), retries=2
                )
                if not isinstance(downloaded, (bytes, bytearray)) or bytes(downloaded) != payload:
                    fail("upload_readback_mismatch")
                else:
                    report["readback_verified"] = True
                    report["status"] = "upload_readback_verified"
    except Exception as exc:
        fail(classify_error(exc))
    finally:
        if create_attempted and service is not None and allocated_id and root_id and nonce and probe_name:
            try:
                metadata = _execute(
                    service.files().get(fileId=allocated_id, fields=_metadata_fields()),
                    retries=2,
                )
            except Exception as exc:
                if _is_not_found(exc):
                    report["cleanup_verified"] = True
                    report["cleanup_status"] = "already_absent"
                    report["cleanup_required"] = False
                else:
                    report["cleanup_status"] = classify_error(exc)
                    report["cleanup_required"] = True
            else:
                if not _identity_matches(
                    metadata,
                    file_id=allocated_id,
                    root_id=root_id,
                    probe_name=probe_name,
                    nonce=nonce,
                ):
                    report["cleanup_status"] = "cleanup_identity_mismatch"
                    report["cleanup_required"] = True
                else:
                    try:
                        _execute(
                            service.files().delete(fileId=allocated_id), retries=2
                        )
                        try:
                            _execute(
                                service.files().get(
                                    fileId=allocated_id, fields="id,trashed"
                                ),
                                retries=2,
                            )
                        except Exception as exc:
                            if _is_not_found(exc):
                                report["cleanup_verified"] = True
                                report["cleanup_status"] = "deleted_and_absent"
                                report["cleanup_required"] = False
                            else:
                                report["cleanup_status"] = classify_error(exc)
                                report["cleanup_required"] = True
                        else:
                            report["cleanup_status"] = "cleanup_file_still_present"
                            report["cleanup_required"] = True
                    except Exception as exc:
                        report["cleanup_status"] = classify_error(exc)
                        report["cleanup_required"] = True

        if report["cleanup_required"]:
            report["probe_name"] = probe_name
            report["status"] = "cleanup_failed"
            if primary_status is not None:
                report["primary_status"] = primary_status
        elif report["cleanup_verified"]:
            if report["upload_verified"] and report["readback_verified"]:
                report["status"] = "upload_readback_cleanup_verified"
            elif primary_status is not None:
                report["status"] = primary_status
    return report


def main():
    # Suppress third-party HTTP logging before the opt-in path can construct auth.
    logging.disable(logging.CRITICAL)
    parser = argparse.ArgumentParser(description="Bounded Google Drive upload probe")
    parser.add_argument(
        "--allow-write-test",
        action="store_true",
        help="Explicitly authorize one temporary upload/readback/delete probe",
    )
    args = parser.parse_args()
    report = check_upload(allow_write_test=args.allow_write_test)
    rendered = json.dumps(report, indent=2, sort_keys=True)
    print(rendered)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            Path(summary_path).write_text(
                "### Google Drive upload check\n\n```json\n"
                + rendered
                + "\n```\n\n"
                "This is an opt-in, single-file probe scoped to the configured root. "
                "It does not test other credentials, folders, identities or runtime environments.\n",
                encoding="utf-8",
            )
        except OSError:
            pass
    return 0 if report["status"] == "upload_readback_cleanup_verified" else 1


if __name__ == "__main__":
    raise SystemExit(main())
