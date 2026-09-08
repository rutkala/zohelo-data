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


def complete_page_fixture(name):
    """Expand compact checked-in examples to their declared page cardinality."""
    payload = json.loads(fixture(name))
    page = payload.get("page", 0)
    page_size = payload.get("pageSize", 100)
    expected = min(page_size, payload["totalRecords"] - page * page_size)
    while len(payload["results"]) < expected:
        item = copy.deepcopy(payload["results"][-1])
        ordinal = len(payload["results"]) + 1
        if name.startswith("variables_"):
            item["id"] = 80000 + ordinal
        else:
            item["id"] = f"9{ordinal:011d}"
            if name.startswith("data_"):
                item["values"] = []
        payload["results"].append(item)
    return json.dumps(payload).encode()


def task_of(tasks, kind, **cursor_values):
    return next(
        task
        for task in tasks
        if task["kind"] == kind
        and all(task["cursor"].get(key) == value for key, value in cursor_values.items())
    )


class GusBdlSourceTests(unittest.TestCase):
    def test_state_migration_retires_only_exact_obsolete_locality_roots(self):
        obsolete_pl = {
            "id": "discovery:localities:pl:root:p000000",
            "lane": "discovery",
            "kind": "localities",
            "cursor": {"lang": "pl", "page": 0, "page_size": 20},
            "failures": 1,
            "retry_at": 1789000000.5,
        }
        obsolete_en = {
            "id": "discovery:localities:en:root:p000000",
            "lane": "discovery",
            "kind": "localities",
            "cursor": {"lang": "en", "page": 0, "page_size": 20},
        }
        healthy = gus_bdl.recent_tasks(TODAY)[0]
        state = {
            "source_id": "gus_bdl",
            "pending": [obsolete_pl, healthy, obsolete_en],
            "completed": {"discovery:years": "2026-09-08T00:00:00+00:00"},
            "receipts": [{"task_id": "discovery:years", "name": "receipt.json"}],
            "rejected_receipts": [{"task_id": obsolete_pl["id"], "name": "rejected.json"}],
            "raw_bytes": 333,
            "quota_attempts": [1788999999.0],
            "provider_retry_at": 1789000100.0,
        }
        original = copy.deepcopy(state)

        migrated = gus_bdl.migrate_state(state, TODAY)
        self.assertEqual(state, original)
        self.assertEqual(migrated["pending"], [healthy])
        for field in (
            "completed",
            "receipts",
            "rejected_receipts",
            "raw_bytes",
            "quota_attempts",
            "provider_retry_at",
        ):
            self.assertEqual(migrated[field], original[field])
        self.assertEqual(
            migrated["plan_dispositions"][obsolete_pl["id"]]["original_task"], obsolete_pl
        )
        self.assertEqual(
            migrated["plan_dispositions"][obsolete_en["id"]]["original_task"], obsolete_en
        )
        for disposition in migrated["plan_dispositions"].values():
            self.assertIn("parent-id", disposition["evidence"])
            self.assertTrue(disposition["reason"])
            self.assertTrue(disposition["contract_revision"])
        self.assertEqual(gus_bdl.migrate_state(migrated, TODAY), migrated)

        malformed = copy.deepcopy(original)
        malformed["pending"][0]["cursor"]["page_size"] = 100
        with self.assertRaisesRegex(ValueError, "unexpected cursor"):
            gus_bdl.migrate_state(malformed, TODAY)

    def test_bootstrap_is_bilingual_and_starts_independent_recent_and_history_lanes(self):
        tasks = gus_bdl.initial_tasks(TODAY)
        json.dumps(tasks)
        self.assertEqual(gus_bdl.SOURCE_ID, "gus_bdl")
        self.assertEqual(gus_bdl.ALLOWED_HOSTS, ("bdl.stat.gov.pl",))
        for kind in ("subjects", "units", "variables"):
            self.assertEqual(
                {task["cursor"]["lang"] for task in tasks if task["kind"] == kind},
                {"pl", "en"},
            )
        self.assertFalse(any(task["kind"] == "localities" for task in tasks))

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
        result = gus_bdl.interpret(root, complete_page_fixture("variables_pl_page_0.json"), TODAY)
        self.assertEqual(result["record_count"], 20)
        self.assertEqual(result["metadata"]["api_total_records"], 21)
        self.assertEqual(result["metadata"]["result_ids"][:2], [72305, 72306])
        self.assertEqual(len(set(result["metadata"]["result_ids"])), 20)

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
        self.assertNotIn("root", page_one["cursor"])
        self.assertEqual(gus_bdl.request_for(page_one)["params"]["page"], 1)

    def test_english_variable_catalogue_only_paginates_to_avoid_duplicate_campaigns(self):
        root = task_of(gus_bdl.initial_tasks(TODAY), "variables", lang="en")
        result = gus_bdl.interpret(root, complete_page_fixture("variables_en_page_0.json"), TODAY)
        self.assertEqual(result["record_count"], 20)
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

    def test_complete_subject_child_list_may_exceed_advertised_page_size(self):
        # Live K9 returns all 21 children on every requested page while echoing
        # pageSize=20 and even advertising a next link. totalRecords and the full
        # unique result set are the only consistent completion evidence.
        task = {
            "id": "discovery:subjects:pl:K9:p000000",
            "lane": "discovery",
            "kind": "subjects",
            "cursor": {
                "lang": "pl",
                "parent_id": "K9",
                "page": 0,
                "page_size": 20,
            },
        }
        results = [
            {
                "id": f"G{number}",
                "parentId": "K9",
                "name": f"Subject {number}",
                "hasVariables": True,
                "children": [],
                "levels": [0, 2, 5, 6],
            }
            for number in range(1, 22)
        ]
        payload = {
            "totalRecords": 21,
            "page": 0,
            "pageSize": 20,
            "results": results,
            "links": {
                "self": "official retained URL",
                "next": "provider advertises a redundant page 1",
            },
        }
        interpreted = gus_bdl.interpret(task, json.dumps(payload).encode(), TODAY)
        self.assertEqual(interpreted["record_count"], 21)
        self.assertEqual(
            interpreted["metadata"]["api_pagination_mode"],
            "complete_child_list",
        )
        self.assertFalse(
            any(
                item["kind"] == "subjects"
                and item["cursor"].get("parent_id") == "K9"
                for item in interpreted["next_tasks"]
            )
        )
        self.assertEqual(
            len(
                [
                    item
                    for item in interpreted["next_tasks"]
                    if item["kind"] == "subject_detail"
                ]
            ),
            42,
        )

        incomplete = copy.deepcopy(payload)
        incomplete["totalRecords"] = 22
        with self.assertRaisesRegex(ValueError, "more results than pageSize"):
            gus_bdl.interpret(task, json.dumps(incomplete).encode(), TODAY)

        duplicate = copy.deepcopy(payload)
        duplicate["results"][-1]["id"] = duplicate["results"][0]["id"]
        with self.assertRaisesRegex(ValueError, "duplicate result identifiers"):
            gus_bdl.interpret(task, json.dumps(duplicate).encode(), TODAY)

        escaped_parent = copy.deepcopy(payload)
        escaped_parent["results"][0]["parentId"] = "K8"
        with self.assertRaisesRegex(ValueError, "escaped the requested parent"):
            gus_bdl.interpret(task, json.dumps(escaped_parent).encode(), TODAY)

    def test_paged_routes_require_exact_interior_last_and_in_range_cardinality(self):
        root = task_of(gus_bdl.initial_tasks(TODAY), "variables", lang="pl")
        with self.assertRaisesRegex(ValueError, "declared page cardinality"):
            gus_bdl.interpret(root, fixture("variables_pl_page_0.json"), TODAY)

        first = gus_bdl.interpret(
            root, complete_page_fixture("variables_pl_page_0.json"), TODAY
        )
        last_task = task_of(first["next_tasks"], "variables", lang="pl", page=1)
        last_payload = json.loads(fixture("variables_pl_page_0.json"))
        last_payload.update({"page": 1, "results": [last_payload["results"][0]]})
        accepted_last = gus_bdl.interpret(
            last_task, json.dumps(last_payload).encode(), TODAY
        )
        self.assertEqual(accepted_last["record_count"], 1)
        self.assertFalse(any(task["kind"] == "variables" for task in accepted_last["next_tasks"]))

        for results in ([], last_payload["results"] * 2):
            short_or_long = copy.deepcopy(last_payload)
            short_or_long["results"] = results
            with self.subTest(result_count=len(results)):
                with self.assertRaisesRegex(ValueError, "declared page cardinality"):
                    gus_bdl.interpret(
                        last_task, json.dumps(short_or_long).encode(), TODAY
                    )

        beyond = copy.deepcopy(last_payload)
        beyond["totalRecords"] = 20
        beyond["results"] = []
        with self.assertRaisesRegex(ValueError, "beyond totalRecords"):
            gus_bdl.interpret(last_task, json.dumps(beyond).encode(), TODAY)

    def test_unpaged_dictionary_and_year_totals_must_match_returned_objects(self):
        tasks = gus_bdl.initial_tasks(TODAY)
        dictionary = task_of(
            tasks, "dictionary", resource="attributes", lang="pl"
        )
        years = task_of(tasks, "years")

        valid_dictionary = {"totalRecords": 1, "results": [{"id": 1}]}
        self.assertEqual(
            gus_bdl.interpret(
                dictionary, json.dumps(valid_dictionary).encode(), TODAY
            )["record_count"],
            1,
        )
        valid_years = {"totalRecords": 2, "results": [{"id": 2025}, {"id": 2026}]}
        self.assertEqual(
            gus_bdl.interpret(years, json.dumps(valid_years).encode(), TODAY)[
                "record_count"
            ],
            2,
        )

        incomplete_dictionary = copy.deepcopy(valid_dictionary)
        incomplete_dictionary["totalRecords"] = 2
        with self.assertRaisesRegex(ValueError, "dictionary result count"):
            gus_bdl.interpret(
                dictionary, json.dumps(incomplete_dictionary).encode(), TODAY
            )
        incomplete_years = copy.deepcopy(valid_years)
        incomplete_years["totalRecords"] = 3
        with self.assertRaisesRegex(ValueError, "years result count"):
            gus_bdl.interpret(years, json.dumps(incomplete_years).encode(), TODAY)

    def test_localities_are_discovered_from_required_municipality_parents(self):
        root = task_of(gus_bdl.initial_tasks(TODAY), "units", lang="pl")
        result = gus_bdl.interpret(root, complete_page_fixture("units_pl_page_0.json"), TODAY)
        locality_tasks = [
            task for task in result["next_tasks"] if task["kind"] == "localities"
        ]
        self.assertEqual(
            {(task["cursor"]["lang"], task["cursor"]["parent_id"]) for task in locality_tasks},
            {("pl", "030210106062"), ("en", "030210106062")},
        )
        for task in locality_tasks:
            request = gus_bdl.request_for(task)
            self.assertEqual(request["params"]["parent-id"], "030210106062")
            self.assertEqual(request["params"]["sort"], "id")
        self.assertEqual(
            gus_bdl.request_for(task_of(result["next_tasks"], "units", page=1))["params"]["page"],
            1,
        )

    def test_level_six_unit_detail_recovers_locality_discovery(self):
        unit_root = task_of(gus_bdl.initial_tasks(TODAY), "units", lang="pl")
        listed = gus_bdl.interpret(unit_root, complete_page_fixture("units_pl_page_0.json"), TODAY)
        detail = task_of(
            listed["next_tasks"],
            "unit_detail",
            lang="pl",
            entity_id="030210106062",
        )
        body = json.dumps(
            {
                "id": "030210106062",
                "name": "Wrocław",
                "level": 6,
                "description": "Source detail",
            }
        ).encode()
        result = gus_bdl.interpret(detail, body, TODAY)
        self.assertEqual(
            {(task["cursor"]["lang"], task["cursor"]["parent_id"]) for task in result["next_tasks"]},
            {("pl", "030210106062"), ("en", "030210106062")},
        )
        for task in result["next_tasks"]:
            self.assertEqual(gus_bdl.request_for(task)["params"]["parent-id"], "030210106062")

    def test_data_response_counts_observations_and_preserves_source_grain_in_metadata(self):
        root = task_of(gus_bdl.recent_tasks(TODAY), "data_by_variable")
        result = gus_bdl.interpret(root, complete_page_fixture("data_72305_recent_page_0.json"), TODAY)
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
        self.assertEqual(gus_bdl.request_for(page_one)["params"]["page"], 1)
        self.assertIsNone(gus_bdl.refresh_task(page_one, date(2026, 9, 11)))

    def test_data_pages_require_complete_unit_membership_and_unique_unit_ids(self):
        root = gus_bdl.recent_tasks(TODAY)[0]
        with self.assertRaisesRegex(ValueError, "declared page cardinality"):
            gus_bdl.interpret(root, fixture("data_72305_recent_page_0.json"), TODAY)

        complete = json.loads(complete_page_fixture("data_72305_recent_page_0.json"))
        # Empty value arrays are legitimate missing observations; page membership
        # is measured from returned units rather than observation values.
        self.assertEqual(
            gus_bdl.interpret(root, json.dumps(complete).encode(), TODAY)["record_count"],
            3,
        )
        repeated_unit = copy.deepcopy(complete)
        repeated_unit["results"][-1]["id"] = repeated_unit["results"][0]["id"]
        with self.assertRaisesRegex(ValueError, "repeated unit identifier"):
            gus_bdl.interpret(root, json.dumps(repeated_unit).encode(), TODAY)

    def test_data_page_markers_may_be_absent_but_explicit_markers_are_strict(self):
        root = gus_bdl.recent_tasks(TODAY)[0]
        body = json.loads(complete_page_fixture("data_72305_recent_page_0.json"))
        self.assertNotIn("page", body)
        self.assertNotIn("pageSize", body)
        self.assertEqual(gus_bdl.interpret(root, json.dumps(body).encode(), TODAY)["record_count"], 3)

        for field, invalid in (
            ("page", -1),
            ("page", "0"),
            ("page", 1),
            ("pageSize", 0),
            ("pageSize", "100"),
            ("pageSize", 20),
        ):
            wrong = copy.deepcopy(body)
            wrong[field] = invalid
            with self.subTest(field=field, invalid=invalid):
                with self.assertRaisesRegex(ValueError, field):
                    gus_bdl.interpret(root, json.dumps(wrong).encode(), TODAY)

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

        wrong_page = json.loads(complete_page_fixture("data_72305_recent_page_0.json"))
        wrong_page["page"] = 1
        with self.assertRaisesRegex(ValueError, "page does not match"):
            gus_bdl.interpret(root, json.dumps(wrong_page).encode(), TODAY)

        duplicate = json.loads(complete_page_fixture("data_72305_recent_page_0.json"))
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
