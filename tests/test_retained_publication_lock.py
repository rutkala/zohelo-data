"""Real local-Git tests; no credentials, network, or production mutations."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from retained_publication_lock import GitPublicationLock, LOCK_REF, PublicationLockError


class PublicationLockTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.remote = self.root / "remote.git"
        subprocess.run(["git", "init", "--bare", str(self.remote)], check=True, capture_output=True)
        subprocess.run(["git", "init", str(self.repo)], check=True, capture_output=True)
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                 "commit", "--allow-empty", "-m", "fixture")
        self.git("remote", "add", "origin", str(self.remote))
        self.sha = self.git("rev-parse", "HEAD")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, text=True, stderr=subprocess.PIPE).strip()

    def test_exclusion_release_and_reacquisition(self):
        with GitPublicationLock(self.repo, self.sha, "fixture-root") as one:
            one.guard()
            with self.assertRaises(PublicationLockError):
                GitPublicationLock(self.repo, self.sha, "other-root").__enter__()
        with GitPublicationLock(self.repo, self.sha, "fixture-root") as two:
            two.guard()
        self.assertEqual(self.git("ls-remote", "origin", LOCK_REF), "")
        self.assertEqual(self.git("rev-parse", "HEAD"), self.sha)

    def test_simultaneous_empty_observations_have_exactly_one_winner(self):
        barrier = threading.Barrier(2)
        owners = [GitPublicationLock(self.repo, self.sha, "fixture-root") for _ in range(2)]
        original = GitPublicationLock._remote_claim
        first_seen = set()
        mutex = threading.Lock()
        def racing_read(owner):
            result = original(owner)
            with mutex:
                first = id(owner) not in first_seen
                first_seen.add(id(owner))
            if first:
                barrier.wait(timeout=10)
            return result
        def attempt(owner):
            try:
                return owner.__enter__()
            except PublicationLockError:
                return None
        with patch.object(GitPublicationLock, "_remote_claim", racing_read):
            with ThreadPoolExecutor(max_workers=2) as pool:
                winners = [owner for owner in pool.map(attempt, owners) if owner is not None]
        self.assertEqual(len(winners), 1)
        winners[0].__exit__(None, None, None)

    def test_release_refuses_to_delete_a_replacement_claim(self):
        owner = GitPublicationLock(self.repo, self.sha, "fixture-root").__enter__()
        replacement = owner._git(["commit-tree", f"{self.sha}^{{tree}}", "-p", self.sha], text="replacement")
        owner._git(["push", f"--force-with-lease={LOCK_REF}:{owner.claim}",
                    "origin", f"{replacement}:{LOCK_REF}"])
        with self.assertRaises(PublicationLockError):
            owner.__exit__(None, None, None)
        self.assertEqual(owner._remote_claim(), replacement)


if __name__ == "__main__":
    unittest.main()
