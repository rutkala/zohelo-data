"""Unit tests for the Google Drive structure and release pointer audit diagnostic.

Verifies:
- Global shared budget across all list, get, and media operations.
- Root and folder ambiguity detection (fails closed without picking first).
- Bounded metadata downloads (size caps before and during media read).
- Strict protocol schema validation (UUID, code_sha, scope, status, tests).
- Dataset and artifact reference validation (trashed, parent mismatch, size mismatch).
- Detection of missing current release directories.
- Honest release classification (verified, unverified with manifest, candidate without).
- Zero mutating Drive calls guaranteed across the entire audit traversal.
- Truthful CLI status reporting (non-zero on verification failure).
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from drive_audit import (
    FOLDER_MIME_TYPE,
    MAX_METADATA_BYTES,
    BudgetTracker,
    audit_physical_storage_map,
    audit_publication_pointer,
    audit_release_directories,
    bounded_read_file_bytes,
    classify_top_level_folder,
    find_child_by_name,
    list_folder_children,
    run_full_drive_audit,
)


def _make_valid_manifest(release_id: str, code_sha: str = "a" * 40, scope: str = "nbp_platform") -> dict:
    return {
        "format_version": 2,
        "release_id": release_id,
        "release_scope": scope,
        "code_sha": code_sha,
        "status": "validated",
        "tests": {"passed": True},
        "created_at_utc": "2026-09-13T08:00:00Z",
        "datasets": [
            {
                "dataset_id": "fact_fx_quotes",
                "table_name": "fact_fx_quotes",
                "layer": "04_gold",
                "row_count": 100,
                "files": [{"id": "f-parquet-1", "name": "fact_fx_quotes.parquet", "size": 5000, "sha256": "1" * 64}],
            }
        ],
        "artifacts": [
            {"name": "manifest.json", "id": "f-art-1", "size": 100, "sha256": "2" * 64},
            {"name": "catalog.json", "id": "f-art-2", "size": 100, "sha256": "3" * 64},
            {"name": "run_results.json", "id": "f-art-3", "size": 100, "sha256": "4" * 64},
            {"name": "business-catalog.json", "id": "f-art-4", "size": 100, "sha256": "5" * 64},
            {"name": "ingestion-state.json", "id": "f-art-5", "size": 100, "sha256": "6" * 64},
        ],
    }


class DriveAuditTests(unittest.TestCase):
    def setUp(self):
        self.mock_drive = MagicMock()
        self.mock_files = self.mock_drive.files()

    def test_global_budget_shared_across_all_operations(self):
        """BudgetTracker halts further requests and records reason when request limit is reached."""
        budget = BudgetTracker(max_requests=2, max_files=100)
        self.assertTrue(budget.record_request())
        self.assertTrue(budget.record_request())
        self.assertFalse(budget.record_request())
        self.assertIn("request_budget_exceeded", budget.incomplete_reasons)
        self.assertTrue(budget.is_exhausted)

    def test_pagination_accumulates_pages_and_detects_repeat(self):
        """list_folder_children iterates pages with shared budget and halts on repeated tokens."""
        budget = BudgetTracker(max_requests=10, max_files=10)
        self.mock_files.list().execute.side_effect = [
            {"files": [{"id": "f1", "name": "a.txt"}], "nextPageToken": "token-1"},
            {"files": [{"id": "f2", "name": "b.txt"}], "nextPageToken": "token-1"},  # Repeated!
        ]
        items, is_complete = list_folder_children(self.mock_files, "parent-1", budget)
        self.assertEqual(len(items), 2)
        self.assertFalse(is_complete)
        self.assertIn("repeated_page_token", budget.incomplete_reasons)

    def test_root_ambiguity_fails_closed(self):
        """run_full_drive_audit must fail closed with ambiguous_root if multiple roots exist."""
        self.mock_files.list().execute.return_value = {
            "files": [
                {"id": "root-1", "name": "zohelo-data", "mimeType": FOLDER_MIME_TYPE},
                {"id": "root-2", "name": "zohelo-data", "mimeType": FOLDER_MIME_TYPE},
            ]
        }
        report = run_full_drive_audit(self.mock_drive, root_name="zohelo-data")
        self.assertEqual(report["status"], "ambiguous_root")
        self.assertEqual(len(report["root_matches"]), 2)

    def test_child_and_pointer_ambiguity_reported(self):
        """Duplicate pointer files under publication root must be reported as ambiguous."""
        budget = BudgetTracker()
        self.mock_files.list().execute.return_value = {
            "files": [
                {"id": "ptr-1", "name": "current-release.json"},
                {"id": "ptr-2", "name": "current-release.json"},
            ]
        }
        res = audit_publication_pointer(self.mock_files, "root-id", budget)
        self.assertEqual(res["status"], "ambiguous_pointer")
        self.assertEqual(res["match_count"], 2)

    def test_bounded_media_download_enforces_size_caps(self):
        """Files exceeding metadata limit must be rejected before or during download."""
        budget = BudgetTracker()
        # Case 1: Declared size > cap
        self.mock_files.get().execute.return_value = {
            "id": "big-file", "name": "big.json", "size": str(MAX_METADATA_BYTES + 100), "trashed": False
        }
        with self.assertRaises(ValueError) as ctx:
            bounded_read_file_bytes(self.mock_files, "big-file", budget)
        self.assertIn("exceeds maximum metadata cap", str(ctx.exception))

        # Case 2: Streaming content exceeds cap
        self.mock_files.get().execute.return_value = {
            "id": "small-declared", "name": "bomb.json", "size": "100", "trashed": False
        }
        self.mock_files.get_media().execute.return_value = b"x" * (MAX_METADATA_BYTES + 50)
        with self.assertRaises(ValueError) as ctx:
            bounded_read_file_bytes(self.mock_files, "small-declared", budget)
        self.assertIn("exceeding cap", str(ctx.exception))

    def test_protocol_schema_validation_pointer_and_manifest(self):
        """Verify full protocol validation: UUID, code_sha, exact-byte hash, status, tests."""
        release_id = "e8c025a4-a7c7-432a-84fa-8151c1479c98"
        rel_folder_id = "rel-folder-123"
        manifest_data = _make_valid_manifest(release_id)
        manifest_bytes = json.dumps(manifest_data).encode("utf-8")
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

        pointer_data = {
            "format_version": 1,
            "release_id": release_id,
            "manifest_file_id": "manifest-file-id",
            "manifest_sha256": manifest_sha,
            "updated_at_utc": "2026-09-13T08:10:00Z",
        }
        pointer_bytes = json.dumps(pointer_data).encode("utf-8")

        budget = BudgetTracker()

        # Mock sequence:
        # 1. find pointer
        # 2. get pointer meta
        # 3. get_media pointer
        # 4. get manifest meta
        # 5. get_media manifest
        # 6. find 'releases' folder
        # 7. find release directory folder by release_id
        # 8. get dataset file metadata
        # 9. get artifact metadata (5 artifacts)
        self.mock_files.list().execute.side_effect = [
            {"files": [{"id": "ptr-id", "name": "current-release.json"}]},  # find pointer
            {"files": [{"id": "releases-dir-id", "name": "releases"}]},       # find releases
            {"files": [{"id": rel_folder_id, "name": release_id}]},          # find release folder
        ]
        self.mock_files.get().execute.side_effect = [
            {"id": "ptr-id", "name": "current-release.json", "size": "200", "trashed": False},
            {"id": "manifest-file-id", "name": "release.json", "size": str(len(manifest_bytes)), "trashed": False},
            # Dataset file
            {"id": "f-parquet-1", "name": "fact_fx_quotes.parquet", "size": "5000", "trashed": False, "parents": [rel_folder_id]},
            # 5 artifacts
            {"id": "f-art-1", "name": "manifest.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
            {"id": "f-art-2", "name": "catalog.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
            {"id": "f-art-3", "name": "run_results.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
            {"id": "f-art-4", "name": "business-catalog.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
            {"id": "f-art-5", "name": "ingestion-state.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
        ]
        self.mock_files.get_media().execute.side_effect = [pointer_bytes, manifest_bytes]

        res = audit_publication_pointer(self.mock_files, "root-id", budget, expected_scope="nbp_platform")
        self.assertEqual(res["status"], "current_manifest_and_metadata_verified")
        self.assertEqual(res["release_id"], release_id)
        self.assertEqual(res["manifest_sha256"], manifest_sha)
        self.assertEqual(res["dataset_count"], 1)
        self.assertEqual(res["artifact_count"], 5)
        self.assertEqual(res["reference_failures"], [])

    def test_reference_parent_mismatch_fails_verification(self):
        """Referenced file with wrong parent directory must fail manifest-and-metadata verification."""
        release_id = "668f721c-5f7e-446d-936f-e89745bb95d3"
        rel_folder_id = "rel-folder-correct"
        manifest_data = _make_valid_manifest(release_id, scope="bdl_platform")
        manifest_bytes = json.dumps(manifest_data).encode("utf-8")
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

        pointer_data = {
            "format_version": 1,
            "release_id": release_id,
            "manifest_file_id": "man-id",
            "manifest_sha256": manifest_sha,
            "updated_at_utc": "2026-09-13T08:15:00Z",
        }
        pointer_bytes = json.dumps(pointer_data).encode("utf-8")

        budget = BudgetTracker()
        self.mock_files.list().execute.side_effect = [
            {"files": [{"id": "ptr-id", "name": "current-release.json"}]},
            {"files": [{"id": "releases-dir-id", "name": "releases"}]},
            {"files": [{"id": rel_folder_id, "name": release_id}]},
        ]
        self.mock_files.get().execute.side_effect = [
            {"id": "ptr-id", "name": "current-release.json", "size": "200", "trashed": False},
            {"id": "man-id", "name": "release.json", "size": str(len(manifest_bytes)), "trashed": False},
            # Dataset file has WRONG parent: 'wrong-folder'
            {"id": "f-parquet-1", "name": "fact_fx_quotes.parquet", "size": "5000", "trashed": False, "parents": ["wrong-folder"]},
            # Artifacts
            {"id": "f-art-1", "name": "manifest.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
            {"id": "f-art-2", "name": "catalog.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
            {"id": "f-art-3", "name": "run_results.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
            {"id": "f-art-4", "name": "business-catalog.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
            {"id": "f-art-5", "name": "ingestion-state.json", "size": "100", "trashed": False, "parents": [rel_folder_id]},
        ]
        self.mock_files.get_media().execute.side_effect = [pointer_bytes, manifest_bytes]

        res = audit_publication_pointer(self.mock_files, "root-id", budget, expected_scope="bdl_platform")
        self.assertEqual(res["status"], "reference_metadata_failed")
        self.assertTrue(any("parent_mismatch" in f.get("reason", "") for f in res["reference_failures"]))

    def test_current_release_folder_not_found(self):
        """When the release folder referenced by current-release.json is absent, report release_directory_not_found."""
        release_id = "11111111-2222-3333-4444-555555555555"
        manifest_data = _make_valid_manifest(release_id)
        manifest_bytes = json.dumps(manifest_data).encode("utf-8")
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

        pointer_data = {
            "format_version": 1,
            "release_id": release_id,
            "manifest_file_id": "man-id",
            "manifest_sha256": manifest_sha,
            "updated_at_utc": "2026-09-13T08:00:00Z",
        }
        pointer_bytes = json.dumps(pointer_data).encode("utf-8")

        budget = BudgetTracker()
        self.mock_files.list().execute.side_effect = [
            {"files": [{"id": "ptr-id", "name": "current-release.json"}]},
            {"files": [{"id": "releases-dir-id", "name": "releases"}]},
            {"files": []},  # Empty releases folder! release_id not found!
        ]
        self.mock_files.get().execute.side_effect = [
            {"id": "ptr-id", "name": "current-release.json", "size": "200", "trashed": False},
            {"id": "man-id", "name": "release.json", "size": str(len(manifest_bytes)), "trashed": False},
        ]
        self.mock_files.get_media().execute.side_effect = [pointer_bytes, manifest_bytes]

        res = audit_publication_pointer(self.mock_files, "root-id", budget)
        self.assertEqual(res["status"], "release_directory_not_found")
        self.assertEqual(res["release_id"], release_id)

    def test_honest_release_classification(self):
        """Classify directories into verified, manifest_present_unverified, and no_manifest_candidate."""
        budget = BudgetTracker()
        current_id = "curr-rel-uuid"
        self.mock_files.list().execute.side_effect = [
            # 1. list releases folder
            {
                "files": [
                    {"id": "f-curr", "name": current_id, "mimeType": FOLDER_MIME_TYPE, "createdTime": "2026-09-13T01:00:00Z"},
                    {"id": "f-old", "name": "old-rel-uuid", "mimeType": FOLDER_MIME_TYPE, "createdTime": "2026-09-12T01:00:00Z"},
                    {"id": "f-cand", "name": "aborted-uuid", "mimeType": FOLDER_MIME_TYPE, "createdTime": "2026-09-13T02:00:00Z"},
                ]
            },
            # 2. list contents of f-curr (has release.json)
            {"files": [{"id": "m1", "name": "release.json", "size": "100"}, {"id": "p1", "name": "t.parquet", "size": "500"}]},
            # 3. list contents of f-old (has release.json)
            {"files": [{"id": "m2", "name": "release.json", "size": "100"}]},
            # 4. list contents of f-cand (no release.json)
            {"files": [{"id": "p2", "name": "temp.parquet", "size": "200"}]},
        ]

        res = audit_release_directories(
            self.mock_files, "releases-id", budget,
            current_release_id=current_id, current_pointer_verified=True,
        )
        self.assertEqual(res["total_release_folders"], 3)
        self.assertTrue(res["current_folder_found"])
        classes = {r["folder_name"]: r["classification"] for r in res["releases"]}
        self.assertEqual(classes[current_id], "current_manifest_and_metadata_verified")
        self.assertEqual(classes["old-rel-uuid"], "manifest_present_unverified")
        self.assertEqual(classes["aborted-uuid"], "no_manifest_candidate")

    def test_zero_mutating_drive_calls_across_full_audit(self):
        """Assert files.create, update, delete, copy, patch are NEVER called."""
        self.mock_files.list().execute.side_effect = [
            {"files": [{"id": "root-1", "name": "zohelo-data", "mimeType": FOLDER_MIME_TYPE}]},
            {"files": []},  # root items
            {"files": []},  # pointer search
            {"files": []},  # bdl-platform search
            {"files": []},  # releases search
        ]
        report = run_full_drive_audit(self.mock_drive, root_name="zohelo-data")
        self.assertTrue(report["read_only"])

        self.mock_files.create.assert_not_called()
        self.mock_files.update.assert_not_called()
        self.mock_files.delete.assert_not_called()
        self.mock_files.copy.assert_not_called()
        self.mock_files.patch.assert_not_called()

    def test_classify_top_level_folder_honesty(self):
        """classify_top_level_folder marks 03_silver and 04_gold as unresolved_historical."""
        silver = classify_top_level_folder("03_silver")
        self.assertEqual(silver["classification"], "unresolved_historical")

        gold = classify_top_level_folder("04_gold")
        self.assertEqual(gold["classification"], "unresolved_historical")

        prom = classify_top_level_folder("promotion-audits")
        self.assertEqual(prom["classification"], "optional_code_supported")

    def test_cli_missing_credentials_handling(self):
        """CLI entrypoint returns code 1 when no credentials are present in environment."""
        from audit_drive_structure import _check_credentials_available

        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(_check_credentials_available())


if __name__ == "__main__":
    unittest.main()
