import copy
from datetime import date
import json
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.sources import gus_bdl  # noqa: E402


FIXTURES = ROOT / "tests" / "fixtures" / "sources" / "gus_bdl"
TODAY = date(2026, 9, 10)


def fixture(name):
    return (FIXTURES / name).read_bytes()


def task_of(tasks, kind, **cursor_values):
    return next(
        task
        for task in tasks
        if task["kind"] == kind
        and all(task["cursor"].get(key) == value for key, value in cursor_values.items())
    )


class GusBdlSourceTests(unittest.TestCase):
    def test_bootstrap_is_bilingual_and_starts_independent_recent_and_history_lanes(self):
        tasks = gus_bdl.initial_tasks(TODAY)
        json.dumps(tasks)
        self.assertEqual(gus_bdl.SOURCE_ID, "gus_bdl")
        self.assertEqual(gus_bdl.ALLOWED_HOSTS, ("bdl.stat.gov.pl",))
        for kind in ("subjects", "units", "localities", "variables"):
            self.assertEqual(
                {task["cursor"]["lang"] for task in tasks if task["kind"] == kind},
                {"pl", "en"},
            )

        recent = task_of(
            tasks,
            "data_by_variable",
            variable_id=72305,
            years=[2022, 2023, 2024, 2025, 2026],
        )
        history = task_of(tasks, "data_by_variable", variable_id=72305, years=None)
        self.assertEqual(recent["lane"], "recent")
        self.assertEqual(recent["recurrence_key"], "variable:72305")
        self.assertEqual(recent["cursor"]["asof"], "2026-09-10")
        self.assertEqual(history["lane"], "history")
        self.assertIsNone(history["cursor"]["years"])

    def test_requests_are_one_page_get_descriptions_with_explicit_language_and_years(self):
        tasks = gus_bdl.initial_tasks(TODAY)
        variables = gus_bdl.request_for(task_of(tasks, "variables", lang="en"))
        self.assertEqual(variables["url"], "https://bdl.stat.gov.pl/api/v1/variables")
        self.assertEqual(
            variables["params"],
            {
                "format": "json",
                "lang": "en",
                "page": 0,
                "page-size": 20,
                "sort": "Id",
            },
        )

        recent = task_of(
            tasks, "data_by_variable", years=[2022, 2023, 2024, 2025, 2026]
        )
        request = gus_bdl.request_for(recent)
        self.assertEqual(
            request["url"], "https://bdl.stat.gov.pl/api/v1/data/by-variable/72305"
        )
        self.assertEqual(request["params"]["year"], [2022, 2023, 2024, 2025, 2026])
        self.assertEqual(request["params"]["page"], 0)

        history = task_of(tasks, "data_by_variable", years=None)
        self.assertNotIn("year", gus_bdl.request_for(history)["params"])

    def test_polish_variable_catalogue_paginates_and_admits_every_returned_variable(self):
        root = task_of(gus_bdl.initial_tasks(TODAY), "variables", lang="pl")
        result = gus_bdl.interpret(root, fixture("variables_pl_page_0.json"), TODAY)
        self.assertEqual(result["record_count"], 2)
        self.assertEqual(result["metadata"]["api_total_records"], 21)
        self.assertEqual(result["metadata"]["result_ids"], [72305, 72306])

        for variable_id in (72305, 72306):
            self.assertEqual(
                {
                    task["cursor"]["lang"]
                    for task in result["next_tasks"]
                    if task["kind"] == "variable_detail"
                    and task["cursor"]["entity_id"] == variable_id
                },
                {"en"},
            )
            self.assertEqual(
                {
                    task["lane"]
                    for task in result["next_tasks"]
                    if task["kind"] == "data_by_variable"
                    and task["cursor"]["variable_id"] == variable_id
                },
                {"recent", "history"},
            )
            recent = task_of(
                result["next_tasks"],
                "data_by_variable",
                variable_id=variable_id,
                years=[2022, 2023, 2024, 2025, 2026],
            )
            self.assertEqual(recent["recurrence_key"], f"variable:{variable_id}")
        page_one = task_of(result["next_tasks"], "variables", page=1, lang="pl")
        self.assertIn(":p000001", page_one["id"])

    def test_english_variable_catalogue_only_paginates_to_avoid_duplicate_campaigns(self):
        root = task_of(gus_bdl.initial_tasks(TODAY), "variables", lang="en")
        result = gus_bdl.interpret(root, fixture("variables_en_page_0.json"), TODAY)
        self.assertEqual(result["record_count"], 2)
        self.assertEqual([task["kind"] for task in result["next_tasks"]], ["variables"])

    def test_subject_tree_expansion_keeps_both_source_languages(self):
        root = task_of(gus_bdl.initial_tasks(TODAY), "subjects", lang="pl")
        result = gus_bdl.interpret(root, fixture("subjects_pl_page_0.json"), TODAY)
        child_lists = [
            task
            for task in result["next_tasks"]
            if task["kind"] == "subjects" and task["cursor"].get("parent_id") == "K3"
        ]
        self.assertEqual({task["cursor"]["lang"] for task in child_lists}, {"pl", "en"})
        details = [task for task in result["next_tasks"] if task["kind"] == "subject_detail"]
        self.assertEqual(len(details), 4)

    def test_data_response_counts_observations_and_preserves_source_grain_in_metadata(self):
        root = task_of(gus_bdl.recent_tasks(TODAY), "data_by_variable")
        result = gus_bdl.interpret(root, fixture("data_72305_recent_page_0.json"), TODAY)
        self.assertEqual(result["record_count"], 3)
        self.assertEqual(result["metadata"]["variable_id"], 72305)
        self.assertEqual(result["metadata"]["measure_unit_id"], 26)
        self.assertEqual(result["metadata"]["observation_count"], 3)
        self.assertIn("attr_id", result["metadata"]["record_grain"])
        self.assertEqual(len(result["next_tasks"]), 1)
        page_one = result["next_tasks"][0]
        self.assertEqual(page_one["lane"], "recent")
        self.assertEqual(page_one["cursor"]["page"], 1)
        self.assertFalse(page_one["cursor"]["root"])
        self.assertEqual(page_one["recurrence_key"], "variable:72305")
        self.assertIsNone(gus_bdl.refresh_task(page_one, date(2026, 9, 11)))

    def test_refresh_changes_generation_date_but_keeps_recurrence_key(self):
        root = gus_bdl.recent_tasks(TODAY)[0]
        refreshed = gus_bdl.refresh_task(root, date(2026, 9, 11))
        self.assertNotEqual(refreshed["id"], root["id"])
        self.assertEqual(refreshed["cursor"]["asof"], "2026-09-11")
        self.assertEqual(refreshed["recurrence_key"], root["recurrence_key"])
        self.assertIsNone(
            gus_bdl.refresh_task(
                task_of(gus_bdl.initial_tasks(TODAY), "data_by_variable", years=None),
                date(2026, 9, 11),
            )
        )

    def test_detail_and_compact_live_value_spelling_are_accepted(self):
        detail = task_of(
            gus_bdl.initial_tasks(TODAY), "variable_detail", lang="en", entity_id=72305
        )
        result = gus_bdl.interpret(detail, fixture("variable_72305_en.json"), TODAY)
        self.assertEqual(result["record_count"], 1)
        self.assertEqual(result["metadata"]["language"], "en")

        body = json.loads(fixture("data_72305_recent_page_0.json"))
        body["totalRecords"] = 2
        body["results"][0]["values"][0]["value"] = body["results"][0]["values"][0].pop("val")
        root = gus_bdl.recent_tasks(TODAY)[0]
        self.assertEqual(gus_bdl.interpret(root, json.dumps(body).encode(), TODAY)["record_count"], 3)

    def test_invalid_json_page_mismatch_duplicate_grain_and_tampered_task_fail(self):
        root = gus_bdl.recent_tasks(TODAY)[0]
        for invalid in (b"", b"[]", b"{}"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    gus_bdl.interpret(root, invalid, TODAY)

        wrong_page = json.loads(fixture("data_72305_recent_page_0.json"))
        wrong_page["page"] = 1
        with self.assertRaisesRegex(ValueError, "page does not match"):
            gus_bdl.interpret(root, json.dumps(wrong_page).encode(), TODAY)

        duplicate = json.loads(fixture("data_72305_recent_page_0.json"))
        duplicate["results"][0]["values"].append(
            copy.deepcopy(duplicate["results"][0]["values"][0])
        )
        with self.assertRaisesRegex(ValueError, "duplicate observation grain"):
            gus_bdl.interpret(root, json.dumps(duplicate).encode(), TODAY)

        tampered = copy.deepcopy(root)
        tampered["cursor"]["variable_id"] = "72305/../version"
        with self.assertRaises(ValueError):
            gus_bdl.request_for(tampered)

    def test_persisted_retry_runtime_fields_are_accepted_and_validated(self):
        root = gus_bdl.recent_tasks(TODAY)[0]
        persisted = copy.deepcopy(root)
        persisted.update({"failures": 0, "retry_at": 1789012345.5})
        self.assertEqual(
            gus_bdl.request_for(persisted)["url"],
            "https://bdl.stat.gov.pl/api/v1/data/by-variable/72305",
        )
        for field, invalid in (("failures", -1), ("failures", True), ("retry_at", float("inf"))):
            broken = copy.deepcopy(root)
            broken[field] = invalid
            with self.subTest(field=field, invalid=invalid):
                with self.assertRaises(ValueError):
                    gus_bdl.request_for(broken)


if __name__ == "__main__":
    unittest.main()
