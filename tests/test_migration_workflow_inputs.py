import argparse
import importlib.util
from pathlib import Path
import sys
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"scripts"))
import validate_migration_dispatch as dispatch

def load(name, path):
    spec=importlib.util.spec_from_file_location(name, path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

migration_cli=load("migration_cli", ROOT/"scripts"/"migrate_drive_layout.py")

class MigrationWorkflowInputTests(unittest.TestCase):
    def test_mutations_require_complete_strict_reviewed_inputs(self):
        valid=("a"*40, "123", "plan-migrate-1234abcd", "b"*64)
        for operation in ("apply", "resume", "rollback"):
            self.assertEqual([], dispatch.validate(operation, True, *valid))
        self.assertTrue(dispatch.validate("resume", False, *valid))
        self.assertTrue(dispatch.validate("apply", True, "A"*40, "01", "bad", "g"*64))
        self.assertEqual([], dispatch.validate("plan", False, "", "", "", ""))

    def test_resume_and_rollback_bind_exact_remote_journal_hash(self):
        class Engine:
            def __init__(self, journal): self.journal=journal
            def _load_journal(self): return self.journal
        for operation in ("resume", "rollback"):
            args=argparse.Namespace(operation=operation, plan_sha256="a"*64)
            migration_cli.require_remote_journal_plan_hash(
                Engine({"plan_sha256": "a"*64}), args
            )
            for supplied, journal in (
                ("", {"plan_sha256": "a"*64}),
                ("A"*64, {"plan_sha256": "A"*64}),
                ("a"*64, None),
                ("a"*64, {"plan_sha256": "b"*64}),
            ):
                args.plan_sha256=supplied
                with self.assertRaises(migration_cli.MigrationError):
                    migration_cli.require_remote_journal_plan_hash(Engine(journal), args)

if __name__=="__main__":
    unittest.main()
