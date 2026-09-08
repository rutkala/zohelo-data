import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion import source_credentials  # noqa: E402


ANONYMOUS_SETTINGS = {
    "max_requests": 12,
    "min_request_interval_seconds": 3,
    "quota_windows": [
        {"seconds": 900, "requests": 80},
        {"seconds": 43_200, "requests": 800},
        {"seconds": 604_800, "requests": 8_000},
    ],
}
SECRET = "client-id-ABC_123"


def load_configure_script():
    path = ROOT / "scripts" / "configure-source-secret.py"
    spec = importlib.util.spec_from_file_location("configure_source_secret", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SourceCredentialTests(unittest.TestCase):
    def test_absent_bdl_credential_keeps_anonymous_profile(self):
        status = source_credentials.source_access_status("gus_bdl", environ={})
        effective = source_credentials.effective_source_settings(
            "gus_bdl", ANONYMOUS_SETTINGS, environ={}
        )

        self.assertEqual(status.mode, "anonymous")
        self.assertEqual(status.secret_name, "GUS_BDL_API_KEY")
        self.assertEqual(effective, ANONYMOUS_SETTINGS)
        self.assertIsNot(effective, ANONYMOUS_SETTINGS)
        self.assertIsNot(effective["quota_windows"], ANONYMOUS_SETTINGS["quota_windows"])
        self.assertEqual(
            source_credentials.source_request_headers(
                "gus_bdl", "https://bdl.stat.gov.pl/api/v1/variables", environ={}
            ),
            {},
        )

    def test_empty_github_actions_environment_value_is_anonymous(self):
        environment = {"GUS_BDL_API_KEY": ""}

        self.assertEqual(
            source_credentials.source_access_status("gus_bdl", environ=environment),
            source_credentials.SourceAccessStatus("anonymous", "GUS_BDL_API_KEY"),
        )
        self.assertEqual(
            source_credentials.effective_source_settings(
                "gus_bdl", ANONYMOUS_SETTINGS, environ=environment
            ),
            ANONYMOUS_SETTINGS,
        )

    def test_present_bdl_credential_selects_registered_bounded_profile(self):
        environ = {"GUS_BDL_API_KEY": SECRET}
        status = source_credentials.source_access_status("gus_bdl", environ=environ)
        effective = source_credentials.effective_source_settings(
            "gus_bdl", ANONYMOUS_SETTINGS, environ=environ
        )

        self.assertEqual(status.mode, "registered")
        self.assertEqual(status.secret_name, "GUS_BDL_API_KEY")
        self.assertEqual(effective["max_requests"], 12)
        self.assertEqual(effective["min_request_interval_seconds"], 1)
        self.assertEqual(
            effective["quota_windows"],
            [
                {"seconds": 900, "requests": 400},
                {"seconds": 43_200, "requests": 4_000},
                {"seconds": 604_800, "requests": 40_000},
            ],
        )
        self.assertNotIn(SECRET, repr(effective))
        self.assertNotIn(SECRET, repr(status))

    def test_malformed_credentials_fail_without_echoing_secret(self):
        malformed = (" leading", "trailing ", "line\nbreak", "café", "x" * 513)
        for value in malformed:
            with self.subTest(value_length=len(value)):
                with self.assertRaises(source_credentials.SourceCredentialError) as caught:
                    source_credentials.source_access_status(
                        "gus_bdl", environ={"GUS_BDL_API_KEY": value}
                    )
                self.assertNotIn(value, str(caught.exception))
                self.assertNotIn(value, repr(caught.exception))
                self.assertEqual(str(caught.exception), "Configured source credential is invalid")

    def test_bdl_header_is_limited_to_exact_https_origin_and_api_path(self):
        environ = {"GUS_BDL_API_KEY": SECRET}
        allowed = (
            "https://bdl.stat.gov.pl/api/v1",
            "https://bdl.stat.gov.pl/api/v1/variables",
            "https://bdl.stat.gov.pl/api/v1/data/by-variable/72305?format=json",
        )
        for url in allowed:
            with self.subTest(url=url):
                self.assertEqual(
                    source_credentials.source_request_headers(
                        "gus_bdl", url, environ=environ
                    ),
                    {"X-ClientId": SECRET},
                )

        rejected = (
            "http://bdl.stat.gov.pl/api/v1/variables",
            "https://bdl.stat.gov.pl:443/api/v1/variables",
            "https://BDL.stat.gov.pl/api/v1/variables",
            "https://bdl.stat.gov.pl/api/v10/variables",
            "https://bdl.stat.gov.pl/api/v1evil",
            "https://bdl.stat.gov.pl@evil.test/api/v1/variables",
            "https://evil.test/api/v1/variables",
            "https://bdl.stat.gov.pl/api/v1/variables#fragment",
        )
        for url in rejected:
            with self.subTest(url=url):
                with self.assertRaises(source_credentials.SourceCredentialError) as caught:
                    source_credentials.source_request_headers(
                        "gus_bdl", url, environ=environ
                    )
                self.assertNotIn(SECRET, str(caught.exception))
                self.assertNotIn(SECRET, repr(caught.exception))

    def test_transport_header_is_not_written_into_settings_or_status(self):
        environ = {"GUS_BDL_API_KEY": SECRET}
        settings = source_credentials.effective_source_settings(
            "gus_bdl", ANONYMOUS_SETTINGS, environ=environ
        )
        status = source_credentials.source_access_status("gus_bdl", environ=environ)

        self.assertNotIn("X-ClientId", repr(settings))
        self.assertNotIn("GUS_BDL_API_KEY", repr(settings))
        self.assertNotIn(SECRET, repr(settings))
        self.assertEqual(vars(status), {"mode": "registered", "secret_name": "GUS_BDL_API_KEY"})

    def test_authenticated_fetch_resolves_environment_just_in_time_without_mutating_spec(self):
        environment = {}
        received_headers = []

        def base_fetch(request_spec, allowed_hosts, *, max_bytes, timeout, headers):
            received_headers.append(dict(headers))
            self.assertEqual(request_spec, {"url": "https://bdl.stat.gov.pl/api/v1/years"})
            return 200, b"{}", {"content-type": "application/json"}

        fetcher = source_credentials.make_authenticated_fetch(
            "gus_bdl", base_fetch, environ=environment
        )
        request_spec = {"url": "https://bdl.stat.gov.pl/api/v1/years"}
        fetcher(request_spec, ("bdl.stat.gov.pl",), max_bytes=100, timeout=10)
        environment["GUS_BDL_API_KEY"] = SECRET
        fetcher(request_spec, ("bdl.stat.gov.pl",), max_bytes=100, timeout=10)

        self.assertEqual(received_headers, [{}, {"X-ClientId": SECRET}])
        self.assertEqual(request_spec, {"url": "https://bdl.stat.gov.pl/api/v1/years"})

    def test_access_profile_switch_does_not_touch_quota_attempts(self):
        state = {"quota_attempts": [10.0, 20.0, 30.0]}
        original_attempts = list(state["quota_attempts"])

        anonymous = source_credentials.effective_source_settings(
            "gus_bdl", ANONYMOUS_SETTINGS, environ={}
        )
        registered = source_credentials.effective_source_settings(
            "gus_bdl", anonymous, environ={"GUS_BDL_API_KEY": SECRET}
        )
        returned = source_credentials.effective_source_settings(
            "gus_bdl", ANONYMOUS_SETTINGS, environ={}
        )

        self.assertEqual(state["quota_attempts"], original_attempts)
        self.assertEqual(registered["quota_windows"][0]["requests"], 400)
        self.assertEqual(returned["quota_windows"][0]["requests"], 80)

    def test_other_current_sources_have_public_noncredentialed_status(self):
        for source_id in ("world_bank_wdi", "eurostat"):
            with self.subTest(source_id=source_id):
                self.assertEqual(
                    source_credentials.source_access_status(source_id, environ={}),
                    source_credentials.SourceAccessStatus("public", None),
                )
                self.assertEqual(
                    source_credentials.source_request_headers(
                        source_id, "https://example.test/data", environ={}
                    ),
                    {},
                )


class ConfigureSourceSecretTests(unittest.TestCase):
    def test_helper_sends_approved_secret_over_stdin_and_never_argv(self):
        helper = load_configure_script()
        with (
            patch.object(sys, "argv", ["configure-source-secret.py"]),
            patch.object(helper, "getpass", return_value=SECRET),
            patch.object(helper.subprocess, "run") as run,
            patch("builtins.print"),
        ):
            self.assertEqual(helper.main(), 0)

        positional, keywords = run.call_args
        self.assertEqual(
            positional[0],
            [
                "gh",
                "secret",
                "set",
                "GUS_BDL_API_KEY",
                "--repo",
                "rutkala/zohelo-data",
            ],
        )
        self.assertEqual(keywords["input"], SECRET.encode("ascii"))
        self.assertNotIn(SECRET, repr(positional[0]))
        self.assertTrue(keywords["check"])
        self.assertIs(keywords["stdout"], helper.subprocess.PIPE)
        self.assertIs(keywords["stderr"], helper.subprocess.PIPE)

    def test_helper_fails_closed_when_hidden_input_is_unavailable(self):
        helper = load_configure_script()
        with (
            patch.object(sys, "argv", ["configure-source-secret.py"]),
            patch.object(
                helper,
                "getpass",
                side_effect=helper.GetPassWarning("terminal fallback would echo input"),
            ),
            patch.object(helper.subprocess, "run") as run,
        ):
            with self.assertRaises(RuntimeError) as caught:
                helper.main()

        self.assertEqual(str(caught.exception), "Hidden credential input is unavailable")
        self.assertNotIn("terminal fallback", str(caught.exception))
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
