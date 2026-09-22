"""Credential-free installer contract tests; never contact npm or start an agent."""
from pathlib import Path
import os
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / ".devcontainer/install-codex.sh"


class CodexInstallerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        binary = self.root / "fake-bin"
        prefix = self.root / "standalone prefix"
        binary.mkdir()
        (prefix / "bin").mkdir(parents=True)
        self.log = self.root / "calls.log"
        npm = binary / "npm"
        npm.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALL_LOG"\ncase "$1" in\ninstall) exit "${INSTALL_EXIT:-0}";;\nprefix) printf "%s\\n" "$FAKE_PREFIX";;\n*) exit 91;;\nesac\n')
        npm.chmod(0o755)
        codex = prefix / "bin/codex"
        codex.write_text('#!/bin/sh\nprintf "codex %s\\n" "$*" >> "$CALL_LOG"\n[ "$*" = "--version" ] || exit 92\nprintf "codex-cli fixture\\n"\nexit "${VERSION_EXIT:-0}"\n')
        codex.chmod(0o755)
        self.env = {"PATH": str(binary) + os.pathsep + os.defpath, "HOME": str(self.root),
                    "CALL_LOG": str(self.log), "FAKE_PREFIX": str(prefix)}

    def run_installer(self, **overrides):
        return subprocess.run(["bash", str(INSTALLER)], cwd=self.root,
                              env={**self.env, **overrides}, text=True,
                              capture_output=True, timeout=10)

    def test_pinned_install_and_version_check_only(self):
        result = self.run_installer()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.log.read_text().splitlines(), [
            "install --global --ignore-scripts --no-audit --no-fund @openai/codex@0.155.1",
            "prefix --global", "codex --version"])

    def test_repeat_install_preserves_command_contract(self):
        self.assertEqual(self.run_installer().returncode, 0)
        self.assertEqual(self.run_installer().returncode, 0)
        self.assertEqual(self.log.read_text().splitlines().count("codex --version"), 2)

    def test_install_failure_never_starts_binary(self):
        self.assertEqual(self.run_installer(INSTALL_EXIT="17").returncode, 17)
        self.assertNotIn("codex --version", self.log.read_text())

    def test_version_failure_is_visible(self):
        self.assertEqual(self.run_installer(VERSION_EXIT="23").returncode, 23)


if __name__ == "__main__":
    unittest.main()
