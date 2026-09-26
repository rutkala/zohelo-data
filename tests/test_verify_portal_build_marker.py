import importlib.util
import json
import unittest
from email.message import Message
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "verify_portal_build_marker.py"
)
SPEC = importlib.util.spec_from_file_location("verify_portal_build_marker", SCRIPT_PATH)
assert SPEC and SPEC.loader
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)


class FakeResponse:
    def __init__(
        self,
        payload,
        *,
        status=200,
        content_type="application/json",
    ):
        self.status = status
        self._body = (
            payload
            if isinstance(payload, bytes)
            else json.dumps(payload).encode("utf-8")
        )
        self.headers = Message()
        self.headers["Content-Type"] = content_type

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self, limit):
        return self._body[:limit]


def marker(commit):
    return {"git_commit": commit, **VERIFY.EXPECTED_CAPABILITIES}


class PortalBuildMarkerTests(unittest.TestCase):
    def test_accepts_exact_marker_and_cache_busts_with_commit(self):
        seen = []
        expected = "a" * 40

        def opener(request, *, timeout):
            seen.append((request.full_url, timeout))
            return FakeResponse(marker(expected))

        result = VERIFY.verify_deployed_marker(
            "https://data.example.test/",
            expected,
            attempts=1,
            timeout_seconds=3,
            opener=opener,
        )

        self.assertEqual(expected, result["git_commit"])
        self.assertEqual(3, seen[0][1])
        self.assertIn(
            f"portal-build.json?verification={expected}",
            seen[0][0],
        )

    def test_rejects_spa_fallback_even_when_status_is_200(self):
        def opener(request, *, timeout):
            return FakeResponse(b"<html>portal</html>", content_type="text/html")

        with self.assertRaisesRegex(
            VERIFY.MarkerVerificationError,
            "unexpected content type",
        ):
            VERIFY.verify_deployed_marker(
                "https://data.example.test",
                "b" * 40,
                attempts=1,
                opener=opener,
            )

    def test_retries_stale_marker_then_accepts_exact_deployment(self):
        expected = "c" * 40
        responses = iter(
            [
                FakeResponse(marker("d" * 40)),
                FakeResponse(marker(expected)),
            ]
        )
        sleeps = []

        result = VERIFY.verify_deployed_marker(
            "https://data.example.test",
            expected,
            attempts=2,
            delay_seconds=0.25,
            opener=lambda request, timeout: next(responses),
            sleeper=sleeps.append,
        )

        self.assertEqual(expected, result["git_commit"])
        self.assertEqual([0.25], sleeps)

    def test_rejects_marker_with_unreviewed_capability_change(self):
        expected = "e" * 40
        changed = marker(expected)
        changed["supported_release_formats"] = [1, 2, 3]

        with self.assertRaisesRegex(
            VERIFY.MarkerVerificationError,
            "exact deployed contract",
        ):
            VERIFY.verify_deployed_marker(
                "https://data.example.test",
                expected,
                attempts=1,
                opener=lambda request, timeout: FakeResponse(changed),
            )

    def test_rejects_oversized_marker(self):
        oversized = b"{" + (b" " * 16_384) + b"}"

        with self.assertRaisesRegex(
            VERIFY.MarkerVerificationError,
            "exceeds 16 KiB",
        ):
            VERIFY.verify_deployed_marker(
                "https://data.example.test",
                "f" * 40,
                attempts=1,
                opener=lambda request, timeout: FakeResponse(oversized),
            )

    def test_requires_absolute_origin_without_query_or_fragment(self):
        for origin in (
            "data.example.test",
            "https://data.example.test/?old=1",
            "https://data.example.test/#fragment",
        ):
            with self.subTest(origin=origin):
                with self.assertRaises(ValueError):
                    VERIFY.build_marker_url(origin, "1" * 40)


if __name__ == "__main__":
    unittest.main()
