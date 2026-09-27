"""Native Landing never needs to understand the contents it transfers."""
from contextlib import ExitStack
from hashlib import md5, sha256
import ast
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
from zipfile import ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import bdl_bulk_ingest as ingest


class NativeLandingTests(unittest.TestCase):
    def setup_transport(self, *, reused=False):
        stack = ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.dict(os.environ, {
            "GITHUB_ACTIONS": "true", "GITHUB_REF": "refs/heads/main",
        }))
        storage = Mock()
        storage.resolve_zone.return_value = "landing"
        storage.get_or_create_nested_folder.side_effect = ["snapshot", "control"] * 5
        stack.enter_context(patch.object(ingest, "StorageManager", return_value=storage))
        observed = {"uploads": [], "manifests": []}
        def upload(_storage, local_path, **kwargs):
            raw = local_path.read_bytes()
            self.assertEqual(sha256(raw).hexdigest(), kwargs["sha256_hex"])
            self.assertEqual(md5(raw).hexdigest(), kwargs["md5_hex"])
            observed["uploads"].append((raw, kwargs))
            return {"id": "native-file", "name": kwargs["name"], "size": len(raw),
                    "sha256": kwargs["sha256_hex"], "md5": kwargs["md5_hex"], "reused": reused}
        def manifest(_storage, value, *, parent_id):
            self.assertEqual("control", parent_id)
            observed["manifests"].append(value)
            return {"id": "manifest", "sha256": "a" * 64, "size": 500}
        published = stack.enter_context(patch.object(ingest, "_upload_file", side_effect=upload))
        stack.enter_context(patch.object(ingest, "_upload_manifest", side_effect=manifest))
        checkpoint = stack.enter_context(patch.object(ingest, "_upload_completion_marker", return_value={"id": "marker"}))
        return storage, observed, published, checkpoint

    def test_malformed_csv_and_multiple_archive_members_are_retained_without_opening(self):
        _, observed, upload, checkpoint = self.setup_transport()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "download.zip"
            with ZipFile(source, "w") as archive:
                archive.writestr("data.csv", b'broken;"quoting\r\n\xff\xfe')
                archive.writestr("provider-notes.txt", "retain all provider members")
            expected = source.read_bytes()
            with patch("zipfile.ZipFile", side_effect=AssertionError("Landing must not open ZIP")), patch(
                "duckdb.connect", side_effect=AssertionError("Landing must not process data")
            ):
                result = ingest.ingest_archive(source, "P1463", True, source_filename="CENY_1463_CREL.zip")
            self.assertEqual(expected, source.read_bytes())
        upload.assert_called_once()
        self.assertEqual(expected, observed["uploads"][0][0])
        self.assertEqual("CENY_1463_CREL.zip", observed["uploads"][0][1]["name"])
        self.assertEqual("native_bytes_only", result["landing_scope"])
        self.assertEqual(len(expected), result["archive_bytes"])
        self.assertEqual("not_performed", result["content_validation"])
        for value in (result, observed["manifests"][0]):
            self.assertNotIn("row_count", value)
            self.assertNotIn("source_columns", value)
            self.assertNotIn("parquet_object", value)
        checkpoint.assert_called_once()
        self.assertEqual(len(expected), checkpoint.call_args.kwargs["archive_bytes"])
        self.assertNotIn("row_count", checkpoint.call_args.kwargs)

    def test_native_writer_is_format_agnostic_and_never_reencodes_bytes(self):
        _, observed, _, _ = self.setup_transport()
        raw = bytes(range(256)) + b'\r\nopaque binary content\0'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "provider.xlsx"
            path.write_bytes(raw)
            result = ingest.ingest_archive(path, "P2", True)
        self.assertEqual(raw, observed["uploads"][0][0])
        self.assertEqual("provider.xlsx", result["archive_object"]["name"])
        self.assertEqual(sha256(raw).hexdigest(), result["archive_sha256"])

    def test_transfer_failure_never_creates_completion(self):
        _, observed, upload, checkpoint = self.setup_transport()
        upload.side_effect = RuntimeError("Drive verification failed")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.zip"
            path.write_bytes(b"opaque provider bytes")
            with self.assertRaisesRegex(RuntimeError, "Drive verification"):
                ingest.ingest_archive(path, "P3", True)
        checkpoint.assert_not_called()
        self.assertEqual([], observed["manifests"])

    def test_manifest_failure_leaves_native_object_and_no_false_completion(self):
        _, observed, upload, checkpoint = self.setup_transport()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.zip"
            path.write_bytes(b"opaque provider bytes")
            with patch.object(ingest, "_upload_manifest", side_effect=RuntimeError("ambiguous manifest write")):
                with self.assertRaisesRegex(RuntimeError, "ambiguous"):
                    ingest.ingest_archive(path, "P3", True)
        upload.assert_called_once()
        self.assertEqual(b"opaque provider bytes", observed["uploads"][0][0])
        checkpoint.assert_not_called()

    def test_retry_manifest_does_not_depend_on_processing_time_or_reuse_flag(self):
        _, observed, upload, _ = self.setup_transport()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.zip"
            path.write_bytes(b"unchanged native content")
            ingest.ingest_archive(path, "P4", True)
            original = upload.side_effect
            def reused(*args, **kwargs):
                return {**original(*args, **kwargs), "reused": True}
            upload.side_effect = reused
            ingest.ingest_archive(path, "P4", True)
        self.assertEqual(observed["manifests"][0], observed["manifests"][1])
        self.assertNotIn("reused", observed["manifests"][0]["archive_object"])

    def test_unsafe_filename_is_rejected_before_remote_writes(self):
        storage, _, upload, checkpoint = self.setup_transport()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.zip"
            path.write_bytes(b"native bytes")
            with self.assertRaisesRegex(ValueError, "filename"):
                ingest.ingest_archive(path, "P3", True, source_filename="../escape.zip")
        storage.begin_write_session.assert_not_called()
        upload.assert_not_called()
        checkpoint.assert_not_called()

    def test_landing_module_cannot_import_data_processing_engines(self):
        tree = ast.parse(Path(ingest.__file__).read_text(encoding="utf-8"))
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split('.')[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split('.')[0])
        self.assertFalse(imports & {"duckdb", "pandas", "pyarrow", "csv", "zipfile", "tarfile", "gzip"})


if __name__ == "__main__":
    unittest.main()
