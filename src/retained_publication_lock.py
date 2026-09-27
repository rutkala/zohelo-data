"""Non-expiring, Git-server-serialized ownership for retained DBW publication.

Only a dedicated operational ref is created/deleted. Main and feature history
are untouched. Recovery of an abandoned claim requires a stopped-owner check.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from uuid import uuid4

LOCK_REF = "refs/heads/ops-locks/gus-dbw-retained-bronze"


class PublicationLockError(RuntimeError):
    """Exclusive ownership is unavailable or cannot be verified."""


class GitPublicationLock:
    def __init__(
        self,
        repository: Path,
        code_sha: str,
        root_id: str,
        *,
        lock_ref: str = LOCK_REF,
    ) -> None:
        if not re.fullmatch(r"[0-9a-f]{40}", code_sha) or not root_id:
            raise PublicationLockError("exact code identity and Drive root are required")
        self.repository, self.code_sha, self.root_id = repository, code_sha, root_id
        self.lock_ref = lock_ref
        self.claim: str | None = None

    def _git(self, arguments: list[str], *, text: str | None = None) -> str:
        try:
            result = subprocess.run(
                ["git", "-c", "user.name=Zohelo publication", "-c",
                 "user.email=zohelo-publication@users.noreply.github.com", *arguments],
                cwd=self.repository, input=text, text=True, capture_output=True,
                timeout=45, env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise PublicationLockError("Git publication ownership could not be verified") from exc
        if result.returncode:
            # Credential-helper or transport diagnostics must not enter logs.
            raise PublicationLockError(f"Git publication ownership operation failed ({result.returncode})")
        return result.stdout.strip()

    def _remote_claim(self) -> str | None:
        output = self._git(["ls-remote", "origin", self.lock_ref])
        if not output:
            return None
        rows = [line.split() for line in output.splitlines()]
        if len(rows) != 1 or len(rows[0]) != 2 or rows[0][1] != self.lock_ref:
            raise PublicationLockError("Git publication owner is ambiguous")
        if not re.fullmatch(r"[0-9a-f]{40}", rows[0][0]):
            raise PublicationLockError("Git publication owner identity is invalid")
        return rows[0][0]

    def __enter__(self) -> "GitPublicationLock":
        if self._remote_claim() is not None:
            raise PublicationLockError("another publisher holds the operational Git ref")
        source_name = self.lock_ref.split("/")[-1]
        record = json.dumps({"owner": str(uuid4()), "code_sha": self.code_sha,
            "drive_root_sha256": hashlib.sha256(self.root_id.encode()).hexdigest(),
            "pid": os.getpid(), "source": source_name}, sort_keys=True)
        claim = self._git(["commit-tree", f"{self.code_sha}^{{tree}}", "-p", self.code_sha], text=record)
        if not re.fullmatch(r"[0-9a-f]{40}", claim):
            raise PublicationLockError("Git publication claim creation failed")
        try:
            # Explicit empty expected value: the server requires a nonexistent ref.
            self._git(["push", "--no-follow-tags", "--porcelain", f"--force-with-lease={self.lock_ref}:",
                       "origin", f"{claim}:{self.lock_ref}"])
        except PublicationLockError:
            if self._remote_claim() != claim:
                raise
        if self._remote_claim() != claim:
            raise PublicationLockError("Git publication acquisition was not confirmed")
        self.claim = claim
        return self

    def guard(self) -> None:
        if self.claim is None or self._remote_claim() != self.claim:
            raise PublicationLockError("retained publication ownership was lost")

    def __exit__(self, exc_type, exc, traceback) -> bool:
        try:
            self.guard()
            try:
                # Delete only our exact claim; never delete a replacement owner's ref.
                self._git(["push", "--no-follow-tags", "--porcelain", f"--force-with-lease={self.lock_ref}:{self.claim}",
                           "origin", f":{self.lock_ref}"])
            except PublicationLockError:
                if self._remote_claim() is not None:
                    raise
            if self._remote_claim() is not None:
                raise PublicationLockError("Git publication release was not confirmed")
            self.claim = None
        except PublicationLockError:
            if exc_type is None:
                raise
            # Preserve the primary error; an unconfirmed claim stays fail-closed.
        return False
