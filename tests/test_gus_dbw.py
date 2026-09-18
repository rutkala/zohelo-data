from datetime import date
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.sources import gus_dbw
from ingestion import source_credentials


class GusDbwTests(unittest.TestCase):
    def setUp(self):
        self.today = date(2026, 9, 18)

    def test_initial_tasks_includes_all_dictionaries_and_areas(self):
        tasks = gus_dbw.initial_tasks(self.today)
        self.assertTrue(len(tasks) > 10)

        kinds = {t["kind"] for t in tasks}
        self.assertIn("dictionary", kinds)
        self.assertIn("areas", kinds)
        self.assertIn("variable_section_periods", kinds)

        # Check all 6 dictionaries in both languages
        dict_tasks = [t for t in tasks if t["kind"] == "dictionary"]
        resources = {t["cursor"]["resource"] for t in dict_tasks}
        self.assertEqual(resources, set(gus_dbw._DICTIONARIES))
        languages = {t["cursor"]["lang"] for t in dict_tasks}
        self.assertEqual(languages, {"pl", "en"})

    def test_recent_tasks_and_refresh(self):
        recent = gus_dbw.recent_tasks(self.today)
        self.assertTrue(len(recent) >= 7)

        area_task = next(t for t in recent if t["kind"] == "areas")
        refreshed = gus_dbw.refresh_task(area_task, date(2026, 9, 19))
        self.assertIsNotNone(refreshed)
        self.assertEqual(refreshed["kind"], "areas")

        dict_task = next(t for t in recent if t["kind"] == "dictionary")
        refreshed_dict = gus_dbw.refresh_task(dict_task, date(2026, 9, 19))
        self.assertIsNotNone(refreshed_dict)
        self.assertEqual(refreshed_dict["cursor"]["page"], 1)

    def test_request_for_all_kinds(self):
        # 1. Dictionary
        dict_req = gus_dbw.request_for({
            "kind": "dictionary",
            "cursor": {"resource": "flag-dictionary", "page": 2, "page_size": 50, "lang": "pl"},
        })
        self.assertEqual(dict_req["url"], "https://api-dbw.stat.gov.pl/api/dictionaries/flag-dictionary")
        self.assertEqual(dict_req["params"], {"page": "2", "page-size": "50", "lang": "pl"})

        # 2. Areas
        areas_req = gus_dbw.request_for({"kind": "areas", "cursor": {"lang": "en"}})
        self.assertEqual(areas_req["url"], "https://api-dbw.stat.gov.pl/api/area/area-area")
        self.assertEqual(areas_req["params"], {"lang": "en"})

        # 3. Area variables
        var_req = gus_dbw.request_for({
            "kind": "area_variables",
            "cursor": {"area_id": 369, "lang": "pl"},
        })
        self.assertEqual(var_req["url"], "https://api-dbw.stat.gov.pl/api/area/area-variable")
        self.assertEqual(var_req["params"], {"id-obszaru": "369", "lang": "pl"})

        # 4. Variable meta
        meta_req = gus_dbw.request_for({
            "kind": "variable_meta",
            "cursor": {"variable_id": 378, "lang": "pl"},
        })
        self.assertEqual(meta_req["url"], "https://api-dbw.stat.gov.pl/api/variable/variable-meta")
        self.assertEqual(meta_req["params"], {"id-zmiennej": "378", "lang": "pl"})

        # 5. Section periods
        sec_req = gus_dbw.request_for({
            "kind": "variable_section_periods",
            "cursor": {"page": 1, "page_size": 100, "lang": "pl"},
        })
        self.assertEqual(sec_req["url"], "https://api-dbw.stat.gov.pl/api/variable/variable-section-periods")
        self.assertEqual(sec_req["params"], {"numer-strony": "1", "ile-na-stronie": "100", "lang": "pl"})

        # 6. Section position
        pos_req = gus_dbw.request_for({
            "kind": "variable_section_position",
            "cursor": {"section_id": 42, "lang": "pl"},
        })
        self.assertEqual(pos_req["url"], "https://api-dbw.stat.gov.pl/api/variable/variable-section-position")
        self.assertEqual(pos_req["params"], {"id-przekroj": "42", "lang": "pl"})

        # 7. Variable data section
        data_req = gus_dbw.request_for({
            "kind": "variable_data_section",
            "cursor": {
                "variable_id": 378,
                "section_id": 1,
                "year": 2024,
                "period": 242,
                "page": 1,
                "page_size": 100,
                "lang": "pl",
            },
        })
        self.assertEqual(data_req["url"], "https://api-dbw.stat.gov.pl/api/variable/variable-data-section")
        self.assertEqual(data_req["params"]["id-zmienna"], "378")
        self.assertEqual(data_req["params"]["id-przekroj"], "1")

        # 8. Bulk download
        bulk_req = gus_dbw.request_for({
            "kind": "bulk_download",
            "cursor": {"url": "https://dbw.stat.gov.pl/files/sample.zip", "filename": "sample.zip"},
        })
        self.assertEqual(bulk_req["url"], "https://dbw.stat.gov.pl/files/sample.zip")

    def test_interpret_areas_discovers_children_and_emits_variable_tasks(self):
        sample_tree = [
            {
                "id": "369",
                "name": "Gospodarka",
                "children": [
                    {
                        "id": "369-161",
                        "name": "Budownictwo",
                        "children": [
                            {"id": "369-161-162", "name": "Mieszkania"},
                        ],
                    }
                ],
            }
        ]
        task = {"kind": "areas", "cursor": {"lang": "pl"}}
        result = gus_dbw.interpret(task, json.dumps(sample_tree).encode("utf-8"), self.today)

        self.assertEqual(result["status"], "accepted")
        self.assertEqual(result["records"], 1)
        # Should emit area_variables tasks for area IDs discovered
        follow_ups = result["tasks"]
        self.assertTrue(len(follow_ups) >= 1)
        kinds = {t["kind"] for t in follow_ups}
        self.assertEqual(kinds, {"area_variables"})
        self.assertIn(369, [t["cursor"]["area_id"] for t in follow_ups])

    def test_interpret_dictionary_handles_pagination(self):
        sample_dict_page = {
            "page": 1,
            "total": 150,
            "data": [{"id": i, "name": f"item_{i}"} for i in range(100)],
        }
        task = {
            "kind": "dictionary",
            "lane": "discovery",
            "cursor": {"resource": "periods-dictionary", "page": 1, "page_size": 100, "lang": "pl"},
        }
        result = gus_dbw.interpret(task, json.dumps(sample_dict_page).encode("utf-8"), self.today)
        self.assertEqual(result["records"], 100)
        self.assertEqual(len(result["tasks"]), 1)
        next_task = result["tasks"][0]
        self.assertEqual(next_task["cursor"]["page"], 2)

    def test_interpret_rejects_empty_or_malformed_bodies(self):
        task = {"kind": "areas", "cursor": {"lang": "pl"}}
        with self.assertRaises(gus_dbw.GusDbwSourceError):
            gus_dbw.interpret(task, b"", self.today)
        with self.assertRaises(gus_dbw.GusDbwSourceError):
            gus_dbw.interpret(task, b"not json content", self.today)

    def test_source_credentials_anonymous_and_registered(self):
        # Anonymous
        status = source_credentials.source_access_status("gus_dbw", environ={})
        self.assertEqual(status.mode, "anonymous")
        self.assertEqual(status.secret_name, "GUS_DBW_API_KEY")

        headers = source_credentials.source_request_headers(
            "gus_dbw", "https://api-dbw.stat.gov.pl/api/area/area-area", environ={}
        )
        self.assertEqual(headers, {})

        # Registered
        reg_env = {"GUS_DBW_API_KEY": "test-dbw-client-key"}
        status_reg = source_credentials.source_access_status("gus_dbw", environ=reg_env)
        self.assertEqual(status_reg.mode, "registered")

        reg_headers = source_credentials.source_request_headers(
            "gus_dbw", "https://api-dbw.stat.gov.pl/api/area/area-area", environ=reg_env
        )
        self.assertEqual(reg_headers, {"X-ClientId": "test-dbw-client-key"})

        # Registered limits effective settings
        anon_settings = {
            "max_requests": 12,
            "registered_max_requests": 60,
            "quota_windows": [{"seconds": 900, "requests": 80}],
        }
        effective = source_credentials.effective_source_settings("gus_dbw", anon_settings, environ=reg_env)
        self.assertEqual(effective["max_requests"], 60)
        self.assertEqual(len(effective["quota_windows"]), 3)

        # Host protection: reject non-DBW endpoints for DBW key
        with self.assertRaises(source_credentials.SourceCredentialError):
            source_credentials.source_request_headers(
                "gus_dbw", "https://malicious.site/api/area", environ=reg_env
            )


if __name__ == "__main__":
    unittest.main()
