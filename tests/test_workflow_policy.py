import importlib.util
import tempfile
import unittest
from pathlib import Path

import yaml


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "check-workflows.py"
SPEC = importlib.util.spec_from_file_location("check_workflows", SCRIPT_PATH)
assert SPEC and SPEC.loader
CHECK_WORKFLOWS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK_WORKFLOWS)


def parse_workflow(source: str):
    return yaml.load(source, Loader=yaml.BaseLoader)


class WorkflowPolicyTests(unittest.TestCase):
    def test_accepts_bounded_pinned_main_guarded_sensitive_job(self):
        document = parse_workflow(
            """
on:
  workflow_dispatch:
permissions:
  contents: read
jobs:
  publish:
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@1111111111111111111111111111111111111111
        with:
          persist-credentials: false
      - env:
          TOKEN: ${{ secrets.TOKEN }}
        run: python publish.py
"""
        )
        self.assertEqual([], CHECK_WORKFLOWS.validate_workflow(Path("valid.yml"), document))

    def test_rejects_mutable_action_missing_timeout_and_implicit_permissions(self):
        document = parse_workflow(
            """
on:
  pull_request:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"""
        )
        errors = "\n".join(
            CHECK_WORKFLOWS.validate_workflow(Path("unsafe.yml"), document)
        )
        self.assertIn("no explicit effective permissions", errors)
        self.assertIn("must set timeout-minutes", errors)
        self.assertIn("full 40-character commit SHA", errors)
        self.assertIn("must disable persisted credentials", errors)

    def test_rejects_manual_secret_job_without_main_guard(self):
        document = parse_workflow(
            """
on:
  workflow_dispatch:
permissions:
  contents: read
jobs:
  publish:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - env:
          TOKEN: ${{ secrets.TOKEN }}
        run: python publish.py
"""
        )
        errors = "\n".join(
            CHECK_WORKFLOWS.validate_workflow(Path("unsafe.yml"), document)
        )
        self.assertIn("must require the main ref", errors)

    def test_rejects_inherited_secrets_and_non_mapping_job_permissions(self):
        document = parse_workflow(
            """
on:
  workflow_dispatch:
permissions:
  contents: read
jobs:
  call:
    uses: owner/repository/.github/workflows/task.yml@1111111111111111111111111111111111111111
    permissions: write-all
    secrets: inherit
"""
        )
        errors = "\n".join(
            CHECK_WORKFLOWS.validate_workflow(Path("unsafe.yml"), document)
        )
        self.assertIn("permissions must be an explicit mapping", errors)
        self.assertIn("must require the main ref", errors)

    def test_repository_check_reports_yaml_parse_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broken.yml"
            path.write_text("jobs: [")
            errors = CHECK_WORKFLOWS.check_repository(Path(directory))
        self.assertEqual(1, len(errors))
        self.assertIn("cannot parse workflow", errors[0])


if __name__ == "__main__":
    unittest.main()
