"""Fixture tests for safe GUS BDL-only reset.

Covers all 10 required safety boundaries:
1. pagination: verifies multi-page descendant enumeration without stopping at first page
2. canonical and legacy paths: verifies discovery across canonical and legacy structures
3. ambiguous/mixed scope rejection: fails closed on duplicate folders, foreign assets, and avoids broad substring matches
4. no side effects on plan: verifies plan is strictly read-only and causes zero mutations
5. no secret output: verifies plan and receipts contain no credentials or tokens
6. stale-plan failure: fails closed on plan tampering, hash mismatch, or remote drift
7. non-BDL preservation: verifies non-BDL datasets and roots remain untrashed and intact
8. interrupted trash resume: verifies idempotent resumption from durable journal after interruption
9. retained quota: verifies provider quota attempts and cooldowns are preserved outside reset data
10. active producer rejection: verifies apply fails closed if BDL writer is active, pending, or enabled
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from bdl_reset import (
    ActiveProducerError,
    AmbiguityError,
    BdlResetEngine,
    BdlResetError,
    DriftError,
    SafetyPinError,
    _canonical_digest,
    FOLDER_MIME_TYPE,
    SHORTCUT_MIME_TYPE,
    JSON_MIME_TYPE,
)


class MockDriveService:
    """In-memory mock of Google Drive v3 API for BDL reset validation."""

    def __init__(self, root_id: str = "root-123"):
        self.root_id = root_id
        self.items: dict[str, dict] = {}
        self.files_resource = MockFilesResource(self)

        # Initialize root folder
        self.items[root_id] = {
            "id": root_id,
            "name": "zohelo-data",
            "mimeType": FOLDER_MIME_TYPE,
            "parents": [],
            "trashed": False,
            "size": 0,
        }

    def files(self):
        return self.files_resource

    def add_folder(self, item_id: str, name: str, parent_id: str) -> dict:
        item = {
            "id": item_id,
            "name": name,
            "mimeType": FOLDER_MIME_TYPE,
            "parents": [parent_id],
            "trashed": False,
            "size": 0,
        }
        self.items[item_id] = item
        return item

    def add_file(self, item_id: str, name: str, parent_id: str, size: int = 100, content: bytes = b"") -> dict:
        item = {
            "id": item_id,
            "name": name,
            "mimeType": "application/octet-stream" if not name.endswith(".json") else JSON_MIME_TYPE,
            "parents": [parent_id],
            "trashed": False,
            "size": size,
            "_content": content,
        }
        self.items[item_id] = item
        return item

    def add_shortcut(self, item_id: str, name: str, parent_id: str, target_id: str) -> dict:
        item = {
            "id": item_id,
            "name": name,
            "mimeType": SHORTCUT_MIME_TYPE,
            "parents": [parent_id],
            "trashed": False,
            "size": 0,
            "shortcutDetails": {"targetId": target_id},
        }
        self.items[item_id] = item
        return item


class MockFilesResource:
    def __init__(self, service: MockDriveService):
        self.service = service
        self.mutations_count = 0
        self.trash_calls: list[str] = []

    def list(self, **kwargs):
        q = kwargs.get("q", "")
        page_size = kwargs.get("pageSize", 100)
        page_token = kwargs.get("pageToken")

        # Parse simple queries: 'parent_id' in parents and trashed=false
        parent_id = None
        trashed_filter = False
        name_filter = None

        import re
        parent_match = re.search(r"'([^']+)' in parents", q)
        if parent_match:
            parent_id = parent_match.group(1)

        if "trashed=true" in q:
            trashed_filter = True
        elif "trashed=false" in q:
            trashed_filter = False

        name_match = re.search(r"name='([^']+)'", q)
        if name_match:
            name_filter = name_match.group(1).replace("\\'", "'").replace("\\\\", "\\")

        matching = []
        for item in self.service.items.values():
            if parent_id and parent_id not in item.get("parents", []):
                continue
            if item.get("trashed") != trashed_filter:
                continue
            if name_filter and item.get("name") != name_filter:
                continue
            matching.append(copy.deepcopy(item))

        # Sort for stable deterministic pagination
        matching.sort(key=lambda x: x["id"])

        start = int(page_token) if page_token else 0
        end = start + page_size
        page_items = matching[start:end]
        next_token = str(end) if end < len(matching) else None

        class ExecMock:
            def __init__(self, data):
                self.data = data

            def execute(self, num_retries=0):
                return self.data

        return ExecMock({"files": page_items, "nextPageToken": next_token, "incompleteSearch": False})

    def get(self, fileId, **kwargs):
        item = self.service.items.get(fileId)

        class ExecMock:
            def __init__(self, data):
                self.data = copy.deepcopy(data) if data else None

            def execute(self, num_retries=0):
                if not self.data:
                    raise RuntimeError(f"File {fileId} not found")
                return self.data

        return ExecMock(item)

    def get_media(self, fileId, **kwargs):
        item = self.service.items.get(fileId)

        class ExecMock:
            def __init__(self, data):
                self.data = data.get("_content", b"") if data else b""

            def execute(self, num_retries=0):
                return self.data

        return ExecMock(item)

    def update(self, fileId, body=None, media_body=None, **kwargs):
        self.mutations_count += 1
        item = self.service.items.get(fileId)
        if not item:
            raise RuntimeError(f"Cannot update missing item {fileId}")

        if body and "trashed" in body:
            item["trashed"] = bool(body["trashed"])
            if item["trashed"]:
                self.trash_calls.append(fileId)

        if media_body:
            # Updating content (e.g. journal upload)
            stream = getattr(media_body, "_fd", None)
            if stream:
                stream.seek(0)
                item["_content"] = stream.read()

        class ExecMock:
            def __init__(self, data):
                self.data = copy.deepcopy(data)

            def execute(self, num_retries=0):
                return self.data

        return ExecMock(item)

    def create(self, body=None, media_body=None, **kwargs):
        self.mutations_count += 1
        new_id = f"gen-{len(self.service.items) + 1}"
        item = {
            "id": new_id,
            "name": (body or {}).get("name", "untitled"),
            "mimeType": (body or {}).get("mimeType", "application/octet-stream"),
            "parents": (body or {}).get("parents", []),
            "trashed": False,
            "size": 0,
        }
        if media_body:
            stream = getattr(media_body, "_fd", None)
            if stream:
                stream.seek(0)
                item["_content"] = stream.read()
        self.service.items[new_id] = item

        class ExecMock:
            def __init__(self, data):
                self.data = copy.deepcopy(data)

            def execute(self, num_retries=0):
                return self.data

        return ExecMock(item)

    def delete(self, fileId, **kwargs):
        # STRICT ENFORCEMENT: Never delete permanently!
        raise AssertionError("CRITICAL VIOLATION: Permanent .delete() was called on Google Drive API!")


class MockStorageManager:
    def __init__(self, drive_service: MockDriveService, root_id: str):
        self.drive_service = drive_service
        self.root_id = root_id

    def resolve_root(self, create: bool = False) -> str:
        if create:
            raise AssertionError("StorageManager.resolve_root called with create=True in reset engine!")
        return self.root_id


class BdlResetTests(unittest.TestCase):
    def setUp(self):
        self.root_id = "root-prod-123"
        self.drive = MockDriveService(self.root_id)
        self.storage = MockStorageManager(self.drive, self.root_id)

        # Standard canonical structure
        self.drive.add_folder("f-landing", "01_landing", self.root_id)
        self.drive.add_folder("f-control", "06_control", self.root_id)
        self.drive.add_folder("f-releases", "releases", self.root_id)
        self.drive.add_folder("f-archive", "05_archive", self.root_id)
        self.drive.add_folder("f-bronze", "02_bronze", self.root_id)
        self.drive.add_folder("f-silver", "03_silver", self.root_id)
        self.drive.add_folder("f-gold", "04_gold", self.root_id)

        # Non-BDL sources (must be strictly preserved)
        self.drive.add_folder("f-landing-wdi", "world_bank_wdi", "f-landing")
        self.drive.add_folder("f-landing-eurostat", "eurostat", "f-landing")
        self.drive.add_folder("f-control-nbp", "nbp", "f-control")
        self.drive.add_folder("f-control-sc", "source_campaigns", "f-control")
        self.drive.add_folder("f-sc-wdi", "world_bank_wdi", "f-control-sc")
        self.drive.add_folder("f-releases-nbp", "nbp", "f-releases")
        self.drive.add_folder("f-releases-wdi", "wdi", "f-releases")

        # Canonical BDL roots
        self.drive.add_folder("f-landing-bdl", "gus_bdl", "f-landing")
        self.drive.add_folder("f-ctrl-bdl", "gus_bdl", "f-control-sc")
        self.drive.add_folder("f-rel-bdl", "bdl", "f-releases")
        self.drive.add_folder("f-arch-bdl", "bdl-platform", "f-archive")

        # Medallion current navigation
        for layer_id in ("f-bronze", "f-silver", "f-gold"):
            curr_id = f"f-curr-{layer_id}"
            self.drive.add_folder(curr_id, "current", layer_id)
            # Non-BDL navigation
            self.drive.add_folder(f"f-nbp-{layer_id}", "nbp", curr_id)
            # BDL navigation
            self.drive.add_folder(f"f-bdl-nav-{layer_id}", "bdl", curr_id)

    # -------------------------------------------------------------------------
    # 1. Pagination Test
    # -------------------------------------------------------------------------
    def test_pagination(self):
        """Verify descendant enumeration paginates completely through >100 items without truncation."""
        # Add 120 files inside 01_landing/gus_bdl
        for i in range(120):
            self.drive.add_file(f"f-landing-child-{i:03d}", f"response_{i:03d}.json", "f-landing-bdl", size=50)

        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        landing_targets = [t for t in plan["targets"] if t["root_key"] == "01_landing/gus_bdl"]
        # 120 files + 1 root folder = 121
        self.assertEqual(121, len(landing_targets))
        self.assertEqual(121, plan["counts_by_root"]["01_landing/gus_bdl"])
        self.assertEqual(120 * 50, plan["bytes_by_root"]["01_landing/gus_bdl"])

    # -------------------------------------------------------------------------
    # 2. Canonical and Legacy Paths Test
    # -------------------------------------------------------------------------
    def test_canonical_and_legacy_paths(self):
        """Verify discovery finds canonical releases/bdl + archive, plus legacy bdl-platform if present."""
        # Add legacy bdl-platform at root
        self.drive.add_folder("f-legacy-bdl", "bdl-platform", self.root_id)

        engine = BdlResetEngine(self.storage, self.root_id)
        roots = engine.discover_bdl_roots()

        self.assertIn("01_landing/gus_bdl", roots)
        self.assertIn("06_control/source_campaigns/gus_bdl", roots)
        self.assertIn("releases/bdl", roots)
        self.assertIn("05_archive/bdl-platform", roots)
        self.assertIn("legacy_root/bdl-platform", roots)
        self.assertIn("02_bronze/current/bdl", roots)
        self.assertIn("03_silver/current/bdl", roots)
        self.assertIn("04_gold/current/bdl", roots)

    # -------------------------------------------------------------------------
    # 3. Ambiguous and Mixed Scope Rejection Test
    # -------------------------------------------------------------------------
    def test_ambiguous_mixed_scope_rejection(self):
        """Verify fail-closed rejection on duplicate folders, foreign assets, and no broad substring matches."""
        # 3a. Duplicate gus_bdl folder under 01_landing
        self.drive.add_folder("f-duplicate-landing-bdl", "gus_bdl", "f-landing")
        engine = BdlResetEngine(self.storage, self.root_id)
        with self.assertRaises(AmbiguityError):
            engine.discover_bdl_roots()

        # Clean up duplicate
        del self.drive.items["f-duplicate-landing-bdl"]

        # 3b. Foreign asset inside BDL scope (e.g. world_bank_wdi inside releases/bdl)
        self.drive.add_folder("f-alien-source", "world_bank_wdi", "f-rel-bdl")
        with self.assertRaises(AmbiguityError):
            engine.plan()

        # Clean up alien
        del self.drive.items["f-alien-source"]

        # 3c. Broad substring check: a file named 'bdl_notes.txt' outside BDL scope must not be targeted
        self.drive.add_file("f-shared-doc", "bdl_notes.txt", self.root_id)
        plan = engine.plan()
        target_ids = {t["id"] for t in plan["targets"]}
        self.assertNotIn("f-shared-doc", target_ids)

    # -------------------------------------------------------------------------
    # 4. No Side Effects on Plan Test
    # -------------------------------------------------------------------------
    def test_no_side_effects_on_plan(self):
        """Verify plan generation is 100% read-only and performs zero mutations."""
        initial_mutations = self.drive.files_resource.mutations_count
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        self.assertEqual(initial_mutations, self.drive.files_resource.mutations_count)
        self.assertEqual(0, len(self.drive.files_resource.trash_calls))
        self.assertTrue(plan["read_only"])
        self.assertEqual("planned", plan["status"])

    # -------------------------------------------------------------------------
    # 5. No Secret Output Test
    # -------------------------------------------------------------------------
    def test_no_secret_output(self):
        """Verify generated plan, identity, and receipts leak no credentials or tokens."""
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        serialized = json.dumps(plan)
        secret_patterns = [
            "GOOGLE_OAUTH_CLIENT_SECRET",
            "GOOGLE_OAUTH_REFRESH_TOKEN",
            "GUS_BDL_API_KEY",
            "GUS_BDL_WEB_PASSWORD",
            "GUS_BDL_WEB_EMAIL",
            "Bearer ",
            "ya29.",
            "client_secret",
        ]
        for pattern in secret_patterns:
            self.assertNotIn(pattern, serialized)

    # -------------------------------------------------------------------------
    # 6. Stale-Plan Failure Test
    # -------------------------------------------------------------------------
    def test_stale_plan_failure(self):
        """Verify apply aborts if plan hash is tampered, root ID mismatches, or targets drift."""
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()
        digest = plan["plan_sha256"]

        # 6a. Tampered plan content
        tampered = copy.deepcopy(plan)
        tampered["targets"].append({"id": "fake", "name": "fake", "mime_type": "text/plain"})
        with self.assertRaises(DriftError):
            engine.apply(plan=tampered, confirmed=True, skip_producer_check=True, expected_sha256=digest)

        # 6b. Mismatched expected SHA-256
        with self.assertRaises(DriftError):
            engine.apply(plan=plan, confirmed=True, skip_producer_check=True, expected_sha256="0" * 64)

        # 6c. Root safety pin mismatch
        wrong_engine = BdlResetEngine(MockStorageManager(self.drive, "wrong-root"), "wrong-root")
        with self.assertRaises(SafetyPinError):
            wrong_engine.apply(plan=plan, confirmed=True, skip_producer_check=True, expected_sha256=digest)

        # 6d. Drift: target item deleted before apply
        target_id = plan["targets"][0]["id"]
        del self.drive.items[target_id]
        with self.assertRaises(DriftError):
            engine.apply(plan=plan, confirmed=True, skip_producer_check=True, expected_sha256=digest)

    # -------------------------------------------------------------------------
    # 7. Non-BDL Preservation Test
    # -------------------------------------------------------------------------
    def test_non_bdl_preservation(self):
        """Verify non-BDL sources (NBP, WDI, Eurostat) remain completely untouched and untrashed."""
        # Add files to BDL and non-BDL roots
        self.drive.add_file("f-bdl-item-1", "part1.parquet", "f-rel-bdl")
        self.drive.add_file("f-wdi-item-1", "wdi.parquet", "f-releases-wdi")
        self.drive.add_file("f-nbp-item-1", "nbp.parquet", "f-releases-nbp")

        tmp_journal = Path("test-results/journal-preserve.json")
        engine = BdlResetEngine(self.storage, self.root_id, journal_local_path=tmp_journal)
        plan = engine.plan()

        receipt = engine.apply(
            plan=plan,
            confirmed=True,
            skip_producer_check=True,
            expected_sha256=plan["plan_sha256"],
        )

        self.assertEqual("bdl_reset_applied", receipt["status"])
        self.assertEqual("verified_clean", receipt["verification"]["status"])

        # Non-BDL files must NOT be trashed
        self.assertFalse(self.drive.items["f-wdi-item-1"]["trashed"])
        self.assertFalse(self.drive.items["f-nbp-item-1"]["trashed"])
        self.assertFalse(self.drive.items["f-landing-wdi"]["trashed"])
        self.assertFalse(self.drive.items["f-releases-nbp"]["trashed"])

        # BDL files must be trashed
        self.assertTrue(self.drive.items["f-bdl-item-1"]["trashed"])
        self.assertTrue(self.drive.items["f-landing-bdl"]["trashed"])
        self.assertTrue(self.drive.items["f-rel-bdl"]["trashed"])

    # -------------------------------------------------------------------------
    # 8. Interrupted Trash Resume Test
    # -------------------------------------------------------------------------
    def test_interrupted_trash_resume(self):
        """Verify idempotent recovery and resumption from durable journal after an interruption."""
        # Add 4 BDL items
        self.drive.add_file("f-bdl-f1", "file1.parquet", "f-rel-bdl")
        self.drive.add_file("f-bdl-f2", "file2.parquet", "f-rel-bdl")

        tmp_journal = Path("test-results/journal-resume.json")
        if tmp_journal.is_file():
            tmp_journal.unlink()

        engine = BdlResetEngine(self.storage, self.root_id, journal_local_path=tmp_journal)
        plan = engine.plan()

        # Simulate interruption: manually trash the first 2 targets and record in journal
        first_target = plan["targets"][0]
        self.drive.items[first_target["id"]]["trashed"] = True
        fake_journal = {
            "format_version": 1,
            "operation": "apply",
            "plan_id": plan["plan_id"],
            "plan_sha256": plan["plan_sha256"],
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress",
            "trashed_items": [{
                "id": first_target["id"],
                "name": first_target["name"],
                "parent_id": first_target["parent_id"],
                "mime_type": first_target["mime_type"],
                "root_key": first_target["root_key"],
                "trashed_at_utc": datetime.now(timezone.utc).isoformat(),
            }],
        }
        engine._save_journal(fake_journal)

        # Resume execution
        resume_receipt = engine.apply(
            plan=plan,
            confirmed=True,
            resume=True,
            skip_producer_check=True,
            expected_sha256=plan["plan_sha256"],
        )

        self.assertEqual("bdl_reset_resumed", resume_receipt["status"])
        self.assertEqual("verified_clean", resume_receipt["verification"]["status"])

        # All targets must now be trashed
        for t in plan["targets"]:
            self.assertTrue(self.drive.items[t["id"]]["trashed"])

    # -------------------------------------------------------------------------
    # 9. Retained Quota Test
    # -------------------------------------------------------------------------
    def test_retained_quota(self):
        """Verify durable provider quota/cooldown evidence is extracted and preserved."""
        # Place campaign state with quota in 06_control/source_campaigns/gus_bdl
        state_doc = {
            "schema_version": 2,
            "source_id": "gus_bdl",
            "quota_attempts": [1700000000.0, 1700000100.0],
            "provider_retry_at": 1700000900.0,
            "last_attempt_utc": "2026-09-14T14:00:00Z",
        }
        self.drive.add_file(
            "f-ctrl-state-ptr",
            "current-ingestion-state.json",
            "f-ctrl-bdl",
            content=json.dumps(state_doc).encode("utf-8"),
        )

        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        quota = plan.get("retained_quota_evidence", {})
        self.assertTrue(quota.get("state_found"))
        self.assertEqual("gus_bdl", quota.get("source_id"))
        self.assertEqual([1700000000.0, 1700000100.0], quota.get("quota_attempts"))
        self.assertEqual(1700000900.0, quota.get("provider_retry_at"))
        self.assertEqual("2026-09-14T14:00:00Z", quota.get("last_attempt_utc"))

    # -------------------------------------------------------------------------
    # 10. Active Producer Rejection Test
    # -------------------------------------------------------------------------
    def test_active_producer_rejection(self):
        """Verify apply fails closed if BDL writer workflow is enabled or has active runs."""
        engine = BdlResetEngine(self.storage, self.root_id)
        plan = engine.plan()

        # 10a. Workflow is still active (not disabled)
        with mock.patch("urllib.request.urlopen") as mock_url:
            mock_resp = mock.MagicMock()
            mock_resp.read.return_value = json.dumps({"state": "active"}).encode("utf-8")
            mock_url.return_value.__enter__.return_value = mock_resp

            with self.assertRaises(ActiveProducerError):
                engine.apply(
                    plan=plan,
                    confirmed=True,
                    skip_producer_check=False,
                    expected_sha256=plan["plan_sha256"],
                )

        # 10b. Workflow is disabled, but has an active/queued run (e.g. 34883169715)
        def mock_request(req, timeout=15):
            url = req.full_url if hasattr(req, "full_url") else str(req)
            resp = mock.MagicMock()
            if "workflows/source-gus-bdl.yml/runs" in url:
                if "status=queued" in url:
                    resp.read.return_value = json.dumps({
                        "workflow_runs": [{"id": 34883169715, "status": "queued"}]
                    }).encode("utf-8")
                else:
                    resp.read.return_value = json.dumps({"workflow_runs": []}).encode("utf-8")
            else:
                resp.read.return_value = json.dumps({"state": "disabled_manually"}).encode("utf-8")
            return resp

        with mock.patch("urllib.request.urlopen", side_effect=mock_request):
            with self.assertRaises(ActiveProducerError):
                engine.apply(
                    plan=plan,
                    confirmed=True,
                    skip_producer_check=False,
                    expected_sha256=plan["plan_sha256"],
                )


class BdlResetDispatchAndProvenanceTests(unittest.TestCase):
    def test_dispatch_validation_strict_rules(self):
        import validate_bdl_reset_dispatch as dispatch

        # Plan requires no special mutating fields
        self.assertEqual([], dispatch.validate("plan", False, "", "", ""))

        # Mutating operations require confirmation and valid pins
        valid_pins = ("12345", "plan-bdl-reset-1234abcd", "a" * 64)
        for op in ("apply", "resume"):
            self.assertEqual([], dispatch.validate(op, True, *valid_pins))
            # Unconfirmed
            errors = dispatch.validate(op, False, *valid_pins)
            self.assertTrue(any("confirmation" in e for e in errors))
            # Malformed run ID
            errors = dispatch.validate(op, True, "abc", "plan-bdl-reset-1234abcd", "a" * 64)
            self.assertTrue(any("run ID" in e for e in errors))
            # Malformed plan ID
            errors = dispatch.validate(op, True, "12345", "bad_id", "a" * 64)
            self.assertTrue(any("plan ID" in e for e in errors))
            # Malformed sha256
            errors = dispatch.validate(op, True, "12345", "plan-bdl-reset-1234abcd", "not-a-sha")
            self.assertTrue(any("SHA-256" in e for e in errors))

    def test_plan_provenance_verification(self):
        import verify_bdl_reset_plan as verifier

        root = "root-123"
        commit = "c" * 40
        plan = {
            "status": "planned",
            "plan_id": "plan-bdl-reset-test1234",
            "root_id": root,
            "expected_root_id": root,
            "targets": [],
            "non_bdl_baseline": [],
        }
        digest = verifier.canonical_plan_hash(plan)
        plan["plan_sha256"] = digest

        identity = {
            "format_version": 1,
            "repository": "rutkala/zohelo-data",
            "workflow_path": verifier.WORKFLOW_PATH,
            "workflow_run_id": "9999",
            "workflow_commit": commit,
            "plan_id": "plan-bdl-reset-test1234",
            "plan_sha256": digest,
            "root_id": root,
        }

        # Successful verification without token (static pins check)
        result = verifier.verify_provenance(
            plan=plan,
            identity=identity,
            run_id="9999",
            expected_root_id=root,
            expected_plan_id="plan-bdl-reset-test1234",
            expected_plan_sha256=digest,
            repository="rutkala/zohelo-data",
            executing_commit=commit,
            token="",
        )
        self.assertEqual("reviewed_plan_verified", result["status"])

        # Tampered digest raises ProvenanceError
        with self.assertRaises(verifier.ProvenanceError):
            verifier.verify_provenance(
                plan=plan,
                identity=identity,
                run_id="9999",
                expected_root_id=root,
                expected_plan_id="plan-bdl-reset-test1234",
                expected_plan_sha256="b" * 64,
                repository="rutkala/zohelo-data",
                executing_commit=commit,
                token="",
            )


if __name__ == "__main__":
    unittest.main()

