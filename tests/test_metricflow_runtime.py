"""Execute a synthetic MetricFlow query under kernel-enforced offline mode."""

import csv
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "metricflow"
OFFLINE_RUNNER = REPO_ROOT / "tests" / "helpers" / "run_offline.py"
MF_CLI = "from dbt_metricflow.cli.main import cli\ncli()"


class MetricFlowRuntimeTests(unittest.TestCase):
    """This fixture is technical only and defines no production business metric."""

    @classmethod
    def setUpClass(cls):
        # The workspace parent may be read-only inside Codespaces. Use the
        # existing ignored working directory so generated files cannot dirty Git.
        cls.temp_root = REPO_ROOT / ".local" / "test-tmp"
        cls.temp_root.mkdir(parents=True, exist_ok=True)
        cls.temp = tempfile.TemporaryDirectory(prefix="zohelo-metricflow-", dir=cls.temp_root)
        cls.addClassCleanup(cls.temp.cleanup)
        cls.workspace = Path(cls.temp.name)
        cls.project = cls.workspace / "project"
        shutil.copytree(FIXTURE_ROOT, cls.project)
        cls.database = cls.workspace / "fixture.duckdb"
        cls.target = cls.project / "target"
        cls.logs = cls.workspace / "logs"
        cls.tmpdir = cls.workspace / "tmp"
        cls.tmpdir.mkdir()
        cls.dbt = Path(sys.executable).parent / "dbt"
        if not cls.dbt.is_file():
            raise RuntimeError("dbt is missing from this Python environment; install requirements.txt")
        try:
            __import__("dbt_metricflow")
        except ImportError as exc:
            raise RuntimeError("dbt-metricflow is missing from this Python environment; install requirements.txt") from exc

        cls.env = dict(os.environ)
        for key in tuple(cls.env):
            if key.startswith(("GOOGLE_", "GCP_")):
                cls.env.pop(key)
        cls.env.update(
            DBT_PROFILES_DIR=str(cls.project),
            DBT_PROJECT_DIR=str(cls.project),
            METRICFLOW_FIXTURE_DUCKDB_PATH=str(cls.database),
            DBT_SEND_ANONYMOUS_USAGE_STATS="false",
            DO_NOT_TRACK="1",
            TMPDIR=str(cls.tmpdir),
        )
        cls._assert_offline_filter_fails_closed()
        for command in ("build", "parse"):
            result = cls._run_dbt(command)
            if result.returncode:
                raise AssertionError(f"Synthetic dbt {command} failed:\n{result.stdout}")

    @classmethod
    def _run_dbt(cls, command):
        return subprocess.run(
            [
                str(cls.dbt), command, "--project-dir", str(cls.project),
                "--profiles-dir", str(cls.project), "--log-path", str(cls.logs),
                "--no-partial-parse",
            ],
            cwd=cls.project,
            env=cls.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=120,
            check=False,
        )

    @classmethod
    def _assert_offline_filter_fails_closed(cls):
        probe = (
            "import errno, socket\n"
            "try:\n socket.socket()\n"
            "except OSError as exc:\n"
            "  raise SystemExit(0 if exc.errno == errno.ENETUNREACH else 2)\n"
            "raise SystemExit(3)\n"
        )
        result = subprocess.run(
            [sys.executable, str(OFFLINE_RUNNER), sys.executable, "-c", probe],
            cwd=cls.project,
            env=cls.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(
                "Linux libseccomp offline filter did not deny socket creation "
                f"with ENETUNREACH (exit {result.returncode}): {result.stdout}"
            )

    def _run_metricflow(self, *arguments):
        result = subprocess.run(
            [
                sys.executable, str(OFFLINE_RUNNER), sys.executable, "-c", MF_CLI,
                *arguments,
            ],
            cwd=self.project,
            env=self.env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=120,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        return result

    def _query_rows(self, filename, start_date, end_date):
        output = self.workspace / filename
        self._run_metricflow(
            "query", "--metrics", "fixture_value_total",
            "--group-by", "metric_time,fixture__category",
            "--start-time", start_date, "--end-time", end_date,
            "--csv", str(output),
        )
        with output.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(
            set(rows[0]) if rows else set(),
            {"metric_time__day", "fixture__category", "fixture_value_total"},
        )
        return sorted(
            (
                row["metric_time__day"].split(" ", 1)[0],
                row["fixture__category"],
                Decimal(row["fixture_value_total"]),
            )
            for row in rows
        )

    def test_native_metricflow_queries_exact_values_and_time_filters_offline(self):
        self._run_metricflow("validate-configs", "--skip-dw", "--show-all")

        full_rows = self._query_rows("full.csv", "2024-01-01", "2024-01-03")
        self.assertEqual(len(full_rows), 4)
        self.assertEqual(full_rows, [
            ("2024-01-01", "A", Decimal("2.00")),
            ("2024-01-01", "B", Decimal("3.00")),
            ("2024-01-02", "A", Decimal("5.00")),
            ("2024-01-03", "A", Decimal("7.00")),
        ])

        ranged_rows = self._query_rows("ranged.csv", "2024-01-02", "2024-01-03")
        self.assertEqual(len(ranged_rows), 2)
        self.assertEqual(ranged_rows, [
            ("2024-01-02", "A", Decimal("5.00")),
            ("2024-01-03", "A", Decimal("7.00")),
        ])

        filtered_rows = self._query_rows("filtered.csv", "2024-01-02", "2024-01-02")
        self.assertEqual(len(filtered_rows), 1)
        self.assertEqual(filtered_rows, [("2024-01-02", "A", Decimal("5.00"))])


if __name__ == "__main__":
    unittest.main()
