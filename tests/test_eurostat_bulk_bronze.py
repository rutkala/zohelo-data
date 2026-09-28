from __future__ import annotations

from contextlib import redirect_stdout
from copy import deepcopy
import gzip
from hashlib import md5, sha256
from io import StringIO
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
CODE_SHA = "a" * 40

import eurostat_bulk_bronze as bronze_module  # noqa: E402
from eurostat_bulk_bronze import (  # noqa: E402
    EurostatBulkBronzeError,
    run_bronze_batch,
    verify_bronze_checkpoint,
)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class StateStore:
    def __init__(self, state=None, receipts=None):
        self.state = deepcopy(state)
        self.receipt_values = receipts or {}
        self.owner = None
        self.release_error = None
        self.recovery = None

    def load(self):
        return deepcopy(self.state)

    def load_cached(self):
        return deepcopy(self.state)

    def save(self, state):
        self.state = deepcopy(state)

    def read_receipt(self, descriptor):
        return deepcopy(self.receipt_values[descriptor["sha256"]])

    def acquire_publication_owner(self, owner):
        if self.owner is not None:
            raise RuntimeError("held")
        self.owner = owner

    def guard_publication_owner(self):
        if self.owner is None:
            raise RuntimeError("not held")

    def release_publication_owner(self):
        self.owner = None
        if self.release_error is not None:
            raise self.release_error

    def recover_publication_owner(self, expected_owner, recovery_identity):
        if self.owner != expected_owner:
            raise RuntimeError("unexpected owner")
        self.owner = None
        self.recovery = (expected_owner, recovery_identity)


class RawStore:
    def __init__(self):
        self.values = {}

    def add(self, raw):
        descriptor = {
            "id": "raw-" + sha256(raw).hexdigest()[:20],
            "name": "raw-" + sha256(raw).hexdigest() + ".bin",
            "sha256": sha256(raw).hexdigest(),
            "md5": md5(raw, usedforsecurity=False).hexdigest(),
            "size_bytes": len(raw),
            "metadata": {},
        }
        self.values[descriptor["sha256"]] = raw
        return descriptor

    def read_to_file(self, descriptor, path):
        Path(path).write_bytes(self.values[descriptor["sha256"]])
        return deepcopy(descriptor)


class OutputStore(RawStore):
    def __init__(self):
        super().__init__()
        self.put_calls = 0

    def put_file(self, path, metadata):
        self.put_calls += 1
        descriptor = self.add(Path(path).read_bytes())
        descriptor["metadata"] = deepcopy(metadata)
        return descriptor

    def verify(self, descriptor):
        raw = self.values[descriptor["sha256"]]
        if (
            len(raw) != descriptor["size_bytes"]
            or sha256(raw).hexdigest() != descriptor["sha256"]
        ):
            raise AssertionError("descriptor mismatch")
        return deepcopy(descriptor)


class EurostatBulkBronzeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.raw_store = RawStore()
        self.output_store = OutputStore()

    def tearDown(self):
        self.temporary.cleanup()

    def _receipt(self, dataset, body, *, kind="eurostat_tsv_gzip", empty=False):
        raw = body if empty else gzip.compress(body, mtime=0)
        raw_descriptor = self.raw_store.add(raw)
        distribution = {
            "dataset_id": dataset,
            "kind": kind,
            "version": "2026-09-27",
            "url": f"https://ec.europa.eu/eurostat/{dataset}",
            "params": {},
        }
        inspection = {"status": "complete"}
        if empty:
            distribution.update({
                "dataset_id": dataset + "::partition::empty",
                "original_dataset_id": dataset,
                "partition_id": "partition:empty",
                "partition_selection": {"geo": ["ZZ"]},
            })
            inspection["empty_partition"] = True
        value = {
            "schema_version": 1,
            "source_id": "eurostat",
            "accepted": True,
            "kind": "full_distribution",
            "distribution": distribution,
            "retrieved_at_utc": "2026-09-27T05:00:00+00:00",
            "raw": raw_descriptor,
            "inspection": inspection,
        }
        encoded = canonical(value)
        descriptor = {
            "task_id": "task:" + dataset,
            "id": "receipt-" + sha256(encoded).hexdigest()[:20],
            "sha256": sha256(encoded).hexdigest(),
            "size_bytes": len(encoded),
        }
        return descriptor, value

    def _stores(self, entries):
        descriptors = [item[0] for item in entries]
        receipts = {item[0]["sha256"]: item[1] for item in entries}
        source = StateStore({
            "source_id": "eurostat_bulk",
            "provider_id": "eurostat",
            "receipts": descriptors,
        }, receipts)
        return source, StateStore()

    def test_processes_data_receipt_and_verifies_fresh_output(self):
        entry = self._receipt(
            "demo", b"freq,unit,geo\\TIME_PERIOD\t2025 \nA,NR,PL\t7 p\n"
        )
        source, bronze = self._stores([entry])
        report = run_bronze_batch(
            source, self.raw_store, bronze, self.output_store, self.root / "work",
            owner="run-one", code_sha=CODE_SHA,
            max_distributions=2, max_seconds=60,
        )
        self.assertEqual(report["processed_data_receipts"], 1)
        self.assertEqual(report["pending_accepted_receipts"], 0)
        self.assertTrue(report["retained_receipt_prefix_complete"])
        self.assertFalse(report["complete_official_catalogue"])
        completed = next(iter(bronze.state["completed"].values()))
        self.assertEqual(completed["decoder_code_sha"], CODE_SHA)
        self.assertEqual(completed["output"]["metadata"]["decoder_code_sha"], CODE_SHA)
        parquet = self.root / "published.parquet"
        parquet.write_bytes(self.output_store.values[completed["output"]["sha256"]])
        with duckdb.connect() as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT dataset_id, value_numeric, status_code FROM read_parquet(?)",
                    [str(parquet)],
                ).fetchone(),
                ("demo", 7.0, "p"),
            )
        verified = verify_bronze_checkpoint(source, bronze, self.output_store)
        self.assertEqual(verified["status"], "fresh_eurostat_bulk_bronze_verified")
        self.assertEqual(
            verified["verification_scope"],
            "exact_checkpoint_and_latest_output_bytes",
        )

    def test_skips_non_data_and_resumes_without_duplicate_output(self):
        inventory = self._receipt("inventory", b"x", kind="eurostat_inventory")
        data = self._receipt(
            "demo", b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\t1\n"
        )
        source, bronze = self._stores([inventory, data])
        first = run_bronze_batch(
            source, self.raw_store, bronze, self.output_store, self.root / "first",
            owner="run-one", code_sha=CODE_SHA,
            max_distributions=1, max_seconds=60,
        )
        self.assertEqual(first["skipped_in_batch"], 1)
        self.assertEqual(first["processed_source_receipts"], 2)
        self.assertEqual(self.output_store.put_calls, 1)
        second = run_bronze_batch(
            source, self.raw_store, bronze, self.output_store, self.root / "second",
            owner="run-two", code_sha=CODE_SHA,
            max_distributions=1, max_seconds=60,
        )
        self.assertEqual(second["decoded_in_batch"], 0)
        self.assertEqual(self.output_store.put_calls, 1)

    def test_appended_receipt_continues_from_exact_prefix(self):
        first_entry = self._receipt(
            "one", b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\t1\n"
        )
        source, bronze = self._stores([first_entry])
        run_bronze_batch(
            source, self.raw_store, bronze, self.output_store, self.root / "one",
            owner="run-one", code_sha=CODE_SHA,
            max_distributions=1, max_seconds=60,
        )
        second_entry = self._receipt(
            "two", b"freq,geo\\TIME_PERIOD\t2025 \nA,DE\t2\n"
        )
        source.state["receipts"].append(second_entry[0])
        source.receipt_values[second_entry[0]["sha256"]] = second_entry[1]
        report = run_bronze_batch(
            source, self.raw_store, bronze, self.output_store, self.root / "two",
            owner="run-two", code_sha=CODE_SHA,
            max_distributions=1, max_seconds=60,
        )
        self.assertEqual(report["processed_data_receipts"], 2)
        self.assertEqual(report["processed_source_receipts"], 2)

    def test_rejects_reordered_upstream_prefix(self):
        one = self._receipt(
            "one", b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\t1\n"
        )
        two = self._receipt(
            "two", b"freq,geo\\TIME_PERIOD\t2025 \nA,DE\t2\n"
        )
        source, bronze = self._stores([one, two])
        run_bronze_batch(
            source, self.raw_store, bronze, self.output_store, self.root / "one",
            owner="run-one", code_sha=CODE_SHA,
            max_distributions=1, max_seconds=60,
        )
        source.state["receipts"] = [two[0], one[0]]
        with self.assertRaisesRegex(EurostatBulkBronzeError, "prefix"):
            run_bronze_batch(
                source, self.raw_store, bronze, self.output_store, self.root / "two",
                owner="run-two", code_sha=CODE_SHA,
                max_distributions=1, max_seconds=60,
            )

    def test_accepted_empty_partition_is_durable_typed_output(self):
        entry = self._receipt("demo", b"<Fault>NO_RESULTS</Fault>", empty=True)
        source, bronze = self._stores([entry])
        report = run_bronze_batch(
            source, self.raw_store, bronze, self.output_store, self.root / "empty",
            owner="run-one", code_sha=CODE_SHA,
            max_distributions=1, max_seconds=60,
        )
        self.assertEqual(report["observation_cells"], 0)
        completed = next(iter(bronze.state["completed"].values()))
        self.assertTrue(completed["empty_partition"])

    def test_failure_releases_durable_writer_owner_without_advancing_state(self):
        entry = self._receipt(
            "broken", b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\tnot-a-number\n"
        )
        source, bronze = self._stores([entry])
        with self.assertRaisesRegex(ValueError, "non-numeric"):
            run_bronze_batch(
                source, self.raw_store, bronze, self.output_store,
                self.root / "broken", owner="run-one",
                code_sha=CODE_SHA,
                max_distributions=1, max_seconds=60,
            )
        self.assertIsNone(bronze.owner)
        self.assertIsNone(bronze.state)

    def test_cleanup_failure_does_not_replace_primary_decode_evidence(self):
        entry = self._receipt(
            "broken", b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\tnot-a-number\n"
        )
        source, bronze = self._stores([entry])
        bronze.release_error = RuntimeError("owner release failed")
        with self.assertRaisesRegex(ValueError, "non-numeric"):
            run_bronze_batch(
                source, self.raw_store, bronze, self.output_store,
                self.root / "broken-cleanup", owner="run-one",
                code_sha=CODE_SHA,
                max_distributions=1, max_seconds=60,
            )
        self.assertIsNone(bronze.state)

    def test_rejects_unpinned_decoder_identity_before_acquiring_owner(self):
        entry = self._receipt(
            "demo", b"freq,geo\\TIME_PERIOD\t2025 \nA,PL\t1\n"
        )
        source, bronze = self._stores([entry])
        with self.assertRaisesRegex(EurostatBulkBronzeError, "code_sha"):
            run_bronze_batch(
                source, self.raw_store, bronze, self.output_store,
                self.root / "unpinned", owner="run-one", code_sha="main",
                max_distributions=1, max_seconds=60,
            )
        self.assertIsNone(bronze.owner)
        self.assertEqual(self.output_store.put_calls, 0)

    def test_cli_recovers_only_the_exact_named_owner(self):
        source = StateStore()
        bronze = StateStore()
        bronze.owner = "github-run-123-attempt-1"
        stdout = StringIO()
        with patch.object(
            bronze_module,
            "_production_stores",
            return_value=(source, None, bronze, None),
        ):
            with redirect_stdout(stdout):
                result = bronze_module.main([
                    "--allow-production-write",
                    "--recover-owner", "github-run-123-attempt-1",
                    "--recovery-identity", "github-recovery-run-456-attempt-1",
                ])

        self.assertEqual(result, 0)
        self.assertEqual(
            bronze.recovery,
            (
                "github-run-123-attempt-1",
                "github-recovery-run-456-attempt-1",
            ),
        )
        self.assertEqual(
            json.loads(stdout.getvalue())["status"],
            "eurostat_bulk_bronze_owner_recovered",
        )

    def test_workflow_serializes_writer_and_uses_fresh_read_only_verifier(self):
        workflow = yaml.load(
            (ROOT / ".github/workflows/source-eurostat.yml").read_text(),
            Loader=yaml.BaseLoader,
        )
        writer = workflow["jobs"]["full_distribution_bronze"]
        recovery = workflow["jobs"][
            "recover_cancelled_full_distribution_bronze_owner"
        ]
        verifier = workflow["jobs"]["verify_full_distribution_bronze"]
        self.assertEqual(writer["needs"], "collect_and_publish")
        self.assertIn("always()", writer["if"])
        self.assertNotIn(
            "needs.collect_and_publish.result == 'success'", writer["if"]
        )
        self.assertEqual(writer["concurrency"]["group"], "zohelo-production-data")
        writer_command = writer["steps"][-1]["run"]
        self.assertIn("timeout --signal=INT --kill-after=60s 2700s", writer_command)
        self.assertIn("--allow-production-write", writer_command)
        self.assertIn("--max-distributions 64", writer_command)
        owner_step = writer["steps"][-2]
        self.assertEqual(
            owner_step["if"],
            "inputs.bronze_recovery_owner != ''",
        )
        self.assertIn("--recover-owner", owner_step["run"])
        self.assertEqual(recovery["needs"], "full_distribution_bronze")
        self.assertIn(
            "needs.full_distribution_bronze.result == 'cancelled'",
            recovery["if"],
        )
        self.assertEqual(
            recovery["concurrency"]["group"],
            "zohelo-production-data",
        )
        self.assertIn(
            '"github-run-${GITHUB_RUN_ID}-attempt-${GITHUB_RUN_ATTEMPT}"',
            recovery["steps"][-1]["run"],
        )
        self.assertEqual(verifier["needs"], "full_distribution_bronze")
        self.assertEqual(
            verifier["steps"][-1]["run"],
            "python src/eurostat_bulk_bronze.py --verify-current",
        )
        self.assertNotIn("--allow-production-write", verifier["steps"][-1]["run"])
        self.assertNotIn("env", writer)
        self.assertNotIn("env", verifier)


if __name__ == "__main__":
    unittest.main()
