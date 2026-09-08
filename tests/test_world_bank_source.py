import json
import sys
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.sources import world_bank  # noqa: E402


FIXTURES = ROOT / "tests" / "fixtures" / "sources" / "world_bank"
TODAY = date(2026, 9, 10)


def fixture(name):
    return (FIXTURES / name).read_bytes()


def task_of(tasks, kind, indicator=None):
    return next(
        task
        for task in tasks
        if task["kind"] == kind
        and (indicator is None or task["cursor"].get("indicator") == indicator)
    )


class WorldBankSourceTests(unittest.TestCase):
    def test_bootstrap_starts_discovery_and_both_lanes_for_real_seed_indicators(self):
        tasks = world_bank.initial_tasks(TODAY)
        self.assertEqual(
            [task["kind"] for task in tasks[:2]],
            ["indicator_catalog", "country_catalog"],
        )
        self.assertEqual(
            {
                task["cursor"]["indicator"]
                for task in tasks
                if task["kind"] == "indicator_history"
            },
            {"SP.POP.TOTL", "NY.GDP.MKTP.CD", "SH.XPD.CHEX.GD.ZS"},
        )
        recent = task_of(tasks, "indicator_recent", "SP.POP.TOTL")
        self.assertEqual(recent["recurrence_key"], "indicator:SP.POP.TOTL")
        self.assertEqual(recent["cursor"]["asof"], "2026-09-10")
        self.assertEqual(
            (recent["cursor"]["start_year"], recent["cursor"]["end_year"]),
            (2022, 2026),
        )
        json.dumps(tasks)  # durable queue fields must remain JSON serializable

    def test_requests_are_source_scoped_single_pages_with_footnotes(self):
        tasks = world_bank.initial_tasks(TODAY)
        catalog = world_bank.request_for(task_of(tasks, "indicator_catalog"))
        self.assertEqual(catalog["url"], "https://api.worldbank.org/v2/indicator")
        self.assertEqual(catalog["params"]["source"], "2")
        self.assertEqual(catalog["params"]["page"], "1")
        self.assertEqual(catalog["params"]["per_page"], "25")

        history = world_bank.request_for(
            task_of(tasks, "indicator_history", "SP.POP.TOTL")
        )
        self.assertEqual(
            history["url"],
            "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL",
        )
        self.assertEqual(history["params"]["source"], "2")
        self.assertEqual(history["params"]["date"], "1960:2026")
        self.assertEqual(history["params"]["footnote"], "y")
        self.assertEqual(history["params"]["page"], "1")
        self.assertEqual(world_bank.ALLOWED_HOSTS, ("api.worldbank.org",))

    def test_indicator_discovery_paginates_and_admits_every_returned_indicator(self):
        root = task_of(world_bank.initial_tasks(TODAY), "indicator_catalog")
        result = world_bank.interpret(root, fixture("indicator_page_1.json"), TODAY)
        self.assertEqual(result["record_count"], 2)
        self.assertEqual(result["metadata"]["discovered_indicator_count"], 2)
        self.assertEqual(result["metadata"]["wdi_api_source_id"], "2")
        self.assertEqual(
            {
                (task["kind"], task["cursor"].get("indicator"))
                for task in result["next_tasks"]
            },
            {
                ("indicator_recent", "NY.GDP.MKTP.CD"),
                ("indicator_history", "NY.GDP.MKTP.CD"),
                ("indicator_recent", "SP.POP.TOTL"),
                ("indicator_history", "SP.POP.TOTL"),
                ("indicator_catalog", None),
            },
        )
        page_two = task_of(result["next_tasks"], "indicator_catalog")
        self.assertEqual(page_two["cursor"], {"page": 2})

        final = world_bank.interpret(
            page_two, fixture("indicator_page_2.json"), TODAY
        )
        self.assertFalse(
            any(task["kind"] == "indicator_catalog" for task in final["next_tasks"])
        )
        self.assertEqual(final["metadata"]["api_last_updated"], "2026-07-17")

    def test_country_discovery_preserves_country_and_aggregate_entities(self):
        root = task_of(world_bank.initial_tasks(TODAY), "country_catalog")
        result = world_bank.interpret(root, fixture("country_page_1.json"), TODAY)
        self.assertEqual(result["record_count"], 2)
        self.assertEqual(result["metadata"]["country_entity_count"], 1)
        self.assertEqual(result["metadata"]["aggregate_entity_count"], 1)
        self.assertEqual(result["next_tasks"], [])

    def test_history_pagination_retains_partition_and_finishes_only_at_last_page(self):
        root = task_of(
            world_bank.initial_tasks(TODAY), "indicator_history", "SP.POP.TOTL"
        )
        first = world_bank.interpret(root, fixture("history_page_1.json"), TODAY)
        self.assertEqual(first["record_count"], 2)
        self.assertEqual(first["metadata"]["indicator_id"], "SP.POP.TOTL")
        self.assertEqual(first["metadata"]["period_start_year"], 1960)
        self.assertEqual(len(first["next_tasks"]), 1)
        page_two = first["next_tasks"][0]
        self.assertEqual(page_two["lane"], "history")
        self.assertEqual(page_two["cursor"]["page"], 2)
        self.assertIn(":p000002", page_two["id"])

        final = world_bank.interpret(
            page_two, fixture("history_page_2.json"), TODAY
        )
        self.assertEqual(final["record_count"], 1)
        self.assertEqual(final["next_tasks"], [])

    def test_recent_page_pagination_and_refresh_keep_distinct_daily_ids(self):
        root = task_of(
            world_bank.recent_tasks(TODAY), "indicator_recent", "SP.POP.TOTL"
        )
        first = world_bank.interpret(root, fixture("history_page_1.json"), TODAY)
        page_two = first["next_tasks"][0]
        self.assertEqual(page_two["cursor"]["asof"], "2026-09-10")
        self.assertEqual(page_two["recurrence_key"], "indicator:SP.POP.TOTL")
        self.assertIsNone(world_bank.refresh_task(page_two, date(2026, 9, 11)))

        refreshed = world_bank.refresh_task(root, date(2026, 9, 11))
        self.assertEqual(refreshed["cursor"]["asof"], "2026-09-11")
        self.assertEqual(refreshed["cursor"]["start_year"], 2022)
        self.assertNotEqual(refreshed["id"], root["id"])
        self.assertEqual(refreshed["recurrence_key"], root["recurrence_key"])

    def test_api_errors_false_empty_shapes_and_wrong_pages_do_not_complete(self):
        root = task_of(world_bank.initial_tasks(TODAY), "indicator_catalog")
        invalid_bodies = [
            b"",
            b"{}",
            b"[]",
            b'[{"page":1,"pages":1,"per_page":25,"total":NaN},[]]',
            b'[{"page":1,"pages":0,"per_page":500,"total":0},[]]',
            fixture("api_error.json"),
        ]
        for body in invalid_bodies:
            with self.subTest(body=body[:20]):
                with self.assertRaises(world_bank.WorldBankSourceError):
                    world_bank.interpret(root, body, TODAY)

        wrong_page = json.loads(fixture("indicator_page_1.json"))
        wrong_page[0]["page"] = 2
        with self.assertRaisesRegex(world_bank.WorldBankSourceError, "requested page"):
            world_bank.interpret(root, json.dumps(wrong_page).encode(), TODAY)

        wrong_source = json.loads(fixture("indicator_page_1.json"))
        wrong_source[1][0]["source"]["id"] = "11"
        with self.assertRaisesRegex(world_bank.WorldBankSourceError, "source 2"):
            world_bank.interpret(root, json.dumps(wrong_source).encode(), TODAY)

        history = task_of(
            world_bank.initial_tasks(TODAY), "indicator_history", "SP.POP.TOTL"
        )
        wrong_period = json.loads(fixture("history_page_1.json"))
        wrong_period[1][0]["date"] = "1959"
        with self.assertRaisesRegex(world_bank.WorldBankSourceError, "requested range"):
            world_bank.interpret(history, json.dumps(wrong_period).encode(), TODAY)

    def test_task_tampering_cannot_change_source_path_or_cursor_identity(self):
        root = task_of(
            world_bank.initial_tasks(TODAY), "indicator_history", "SP.POP.TOTL"
        )
        tampered = json.loads(json.dumps(root))
        tampered["cursor"]["indicator"] = "SP.POP.TOTL;OTHER"
        with self.assertRaisesRegex(world_bank.WorldBankSourceError, "indicator"):
            world_bank.request_for(tampered)

        tampered = json.loads(json.dumps(root))
        tampered["cursor"]["page"] = 2
        with self.assertRaisesRegex(world_bank.WorldBankSourceError, "task id"):
            world_bank.request_for(tampered)

        retry = json.loads(json.dumps(root))
        retry.update({"failures": 1, "retry_at": 1_800_000_000.0})
        self.assertEqual(world_bank.request_for(retry), world_bank.request_for(root))


if __name__ == "__main__":
    unittest.main()
