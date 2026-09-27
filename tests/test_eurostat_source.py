import json
from copy import deepcopy
from datetime import date
from pathlib import Path
import sys
import unittest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion.sources.eurostat import (  # noqa: E402
    ALLOWED_GEOS,
    SOURCE_ID,
    STARTER_DATASETS,
    EurostatSourceError,
    decode_jsonstat,
    initial_tasks,
    interpret,
    recent_tasks,
    refresh_task,
    request_for,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "sources" / "eurostat"


def fixture(name):
    return (FIXTURES / name).read_bytes()


def task_for(tasks, dataset, geo="PL"):
    return next(
        task for task in tasks
        if task.get("cursor", {}).get("dataset") == dataset
        and task["cursor"]["geo"] == geo
    )


class EurostatSourceTests(unittest.TestCase):
    today = date(2026, 9, 8)

    def test_initial_tasks_start_all_admitted_history_and_metadata_discovery(self):
        tasks = initial_tasks(self.today)
        self.assertEqual(SOURCE_ID, "eurostat")
        self.assertEqual(len(tasks), 1 + len(STARTER_DATASETS) * len(ALLOWED_GEOS))
        self.assertEqual(len({task["id"] for task in tasks}), len(tasks))
        self.assertEqual(tasks[0]["kind"], "catalogue_toc")
        self.assertEqual(tasks[0]["lane"], "discovery")

        population = task_for(tasks, "demo_pjan")
        self.assertEqual(population["cursor"]["start_period"], "1960")
        self.assertEqual(population["cursor"]["end_period"], "1969")
        self.assertEqual(population["cursor"]["filters"], {"sex": "T", "age": "TOTAL"})
        food = task_for(tasks, "prc_hicp_minr")
        self.assertEqual(food["cursor"]["start_period"], "1996-01")
        self.assertEqual(food["cursor"]["end_period"], "2000-12")
        self.assertNotIn("recurrence_key", food)

    def test_recent_tasks_are_bounded_and_refreshable_by_recurrence_key(self):
        tasks = recent_tasks(self.today)
        self.assertEqual(len(tasks), len(STARTER_DATASETS) * len(ALLOWED_GEOS))
        food = task_for(tasks, "prc_hicp_minr")
        self.assertEqual(food["recurrence_key"], "prc_hicp_minr:PL")
        self.assertEqual(food["cursor"]["start_period"], "2024-01")
        self.assertEqual(food["cursor"]["end_period"], "2026-12")

        refreshed = refresh_task(food, date(2027, 1, 2))
        self.assertEqual(refreshed["recurrence_key"], food["recurrence_key"])
        self.assertEqual(refreshed["cursor"]["start_period"], "2025-01")
        self.assertIn(":2025-01:2027-12", refreshed["id"])
        same_year_next_day = refresh_task(food, date(2026, 9, 9))
        self.assertNotEqual(same_year_next_day["id"], food["id"])
        self.assertTrue(same_year_next_day["id"].endswith(":asof-2026-09-09"))
        self.assertEqual(same_year_next_day["cursor"]["asof"], "2026-09-09")

    def test_requests_use_only_the_statistics_and_catalogue_get_routes(self):
        population = task_for(recent_tasks(self.today), "demo_pjan")
        request = request_for(population)
        self.assertEqual(
            request["url"],
            "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/demo_pjan",
        )
        self.assertEqual(request["params"], {
            "lang": "en",
            "format": "JSON",
            "geo": "PL",
            "sinceTimePeriod": "2024",
            "untilTimePeriod": "2026",
            "sex": "T",
            "age": "TOTAL",
        })
        catalogue = request_for(initial_tasks(self.today)[0])
        self.assertEqual(
            catalogue,
            {
                "url": "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt",
                "params": {"lang": "en"},
            },
        )

    def test_scheduler_retry_fields_do_not_change_request_identity(self):
        task = task_for(recent_tasks(self.today), "demo_pjan")
        expected = request_for(task)
        retry = deepcopy(task)
        retry.update({"failures": 2, "retry_at": 1_789_000_000.5})
        self.assertEqual(request_for(retry), expected)
        with self.assertRaisesRegex(EurostatSourceError, "failures"):
            request_for({**task, "failures": True})
        with self.assertRaisesRegex(EurostatSourceError, "retry_at"):
            request_for({**task, "retry_at": float("inf")})

    def test_admitted_scope_cannot_be_widened_to_other_geos_datasets_or_comext(self):
        task = task_for(recent_tasks(self.today), "nama_10_gdp")
        for dataset, geo in (("nama_10_gdp", "US"), ("other_dataset", "PL"), ("DS-057555", "PL")):
            invalid = deepcopy(task)
            invalid["cursor"]["dataset"] = dataset
            invalid["cursor"]["geo"] = geo
            with self.assertRaises(EurostatSourceError):
                request_for(invalid)

        widened = deepcopy(task)
        widened["cursor"]["filters"] = {"unit": "CP_MEUR"}
        with self.assertRaisesRegex(EurostatSourceError, "cannot be widened"):
            request_for(widened)

        oversized = deepcopy(task)
        oversized["cursor"]["start_period"] = "1975"
        oversized["id"] = "eurostat:recent:nama_10_gdp:pl:1975:2026"
        with self.assertRaisesRegex(EurostatSourceError, "bounded time window"):
            request_for(oversized)

    def test_live_population_fixture_preserves_complete_key_unit_and_status(self):
        fixture_today = date(2025, 9, 8)
        task = task_for(recent_tasks(fixture_today), "demo_pjan")
        result = interpret(task, fixture("demo_pjan_pl_2023_2025.json"), fixture_today)
        self.assertEqual(result["record_count"], 3)
        self.assertEqual(result["metadata"]["dimensions"], [
            "freq", "unit", "age", "sex", "geo", "time",
        ])
        self.assertEqual(result["metadata"]["unit_codes"], ["NR"])
        self.assertEqual(result["metadata"]["status_count"], 1)
        rows = decode_jsonstat(fixture("demo_pjan_pl_2023_2025.json"))
        self.assertEqual(rows[0], {
            "dimensions": {
                "freq": "A", "unit": "NR", "age": "TOTAL", "sex": "T",
                "geo": "PL", "time": "2023",
            },
            "value": 36753736,
            "status": "ep",
            "is_missing": False,
        })

    def test_live_gdp_fixture_validates_requested_native_measure(self):
        fixture_today = date(2025, 9, 8)
        task = task_for(recent_tasks(fixture_today), "nama_10_gdp")
        result = interpret(task, fixture("nama_10_gdp_pl_2023_2025.json"), fixture_today)
        self.assertEqual(result["record_count"], 3)
        self.assertEqual(result["metadata"]["unit_codes"], ["CP_MEUR"])
        rows = decode_jsonstat(fixture("nama_10_gdp_pl_2023_2025.json"))
        self.assertEqual(rows[-1]["dimensions"]["na_item"], "B1GQ")
        self.assertEqual(rows[-1]["dimensions"]["time"], "2025")

    def test_live_food_fixture_preserves_sparse_missing_position(self):
        task = task_for(recent_tasks(self.today), "prc_hicp_minr")
        result = interpret(task, fixture("prc_hicp_minr_pl_2025_2026.json"), self.today)
        self.assertEqual(result["record_count"], 19)
        self.assertEqual(result["metadata"]["cell_count"], 20)
        self.assertEqual(result["metadata"]["missing_count"], 1)
        self.assertEqual(result["metadata"]["unit_codes"], ["I25"])
        rows = decode_jsonstat(fixture("prc_hicp_minr_pl_2025_2026.json"))
        self.assertEqual(rows[0]["dimensions"]["coicop18"], "CP01")
        self.assertEqual(rows[-1]["dimensions"]["time"], "2026-08")
        self.assertTrue(rows[-1]["is_missing"])
        self.assertIsNone(rows[-1]["value"])

    def test_successful_history_response_schedules_only_the_next_window(self):
        task = task_for(initial_tasks(self.today), "demo_pjan")
        body = json.loads(fixture("demo_pjan_pl_2023_2025.json"))
        time = body["dimension"]["time"]["category"]
        time["index"] = {"1960": 0, "1961": 1, "1962": 2}
        time["label"] = {"1960": "1960", "1961": "1961", "1962": "1962"}
        result = interpret(task, json.dumps(body).encode(), self.today)
        self.assertEqual(len(result["next_tasks"]), 1)
        following = result["next_tasks"][0]
        self.assertEqual(following["cursor"]["start_period"], "1970")
        self.assertEqual(following["cursor"]["end_period"], "1979")

    def test_valid_sparse_empty_history_window_advances_without_becoming_an_error(self):
        task = task_for(initial_tasks(self.today), "nama_10_gdp", geo="HR")
        body = json.loads(fixture("nama_10_gdp_pl_2023_2025.json"))
        body["value"] = {}
        body["size"][-1] = 10
        time = body["dimension"]["time"]["category"]
        years = [str(year) for year in range(1975, 1985)]
        time["index"] = {year: position for position, year in enumerate(years)}
        time["label"] = {year: year for year in years}
        geo = body["dimension"]["geo"]["category"]
        geo["index"] = {"HR": 0}
        geo["label"] = {"HR": "Croatia"}
        result = interpret(task, json.dumps(body).encode(), self.today)
        self.assertEqual(result["record_count"], 0)
        self.assertEqual(result["metadata"]["cell_count"], 10)
        self.assertEqual(result["metadata"]["missing_count"], 10)
        self.assertEqual(result["next_tasks"][0]["cursor"]["start_period"], "1985")

    def test_jsonstat_rejects_bad_shapes_and_response_slice_escape(self):
        task = task_for(recent_tasks(self.today), "demo_pjan")
        body = json.loads(fixture("demo_pjan_pl_2023_2025.json"))

        escaped = deepcopy(body)
        escaped["dimension"]["geo"]["category"]["index"] = {"US": 0}
        escaped["dimension"]["geo"]["category"]["label"] = {"US": "United States"}
        with self.assertRaisesRegex(EurostatSourceError, "escaped requested geo"):
            interpret(task, json.dumps(escaped).encode(), self.today)

        bad_position = deepcopy(body)
        bad_position["value"]["3"] = 1
        with self.assertRaisesRegex(EurostatSourceError, "outside the cube"):
            decode_jsonstat(json.dumps(bad_position).encode())

        bad_index = deepcopy(body)
        bad_index["dimension"]["time"]["category"]["index"] = {
            "2023": 0, "2024": 2, "2025": 3,
        }
        with self.assertRaisesRegex(EurostatSourceError, "category positions"):
            decode_jsonstat(json.dumps(bad_index).encode())

        with self.assertRaisesRegex(EurostatSourceError, "error or asynchronous warning"):
            interpret(task, b'{"warning":{"status":413}}', self.today)

    def test_catalogue_discovery_is_metadata_only(self):
        task = initial_tasks(self.today)[0]
        body = b"title\tcode\ttype\nPopulation\tdemo_pjan\tdataset\nGDP\tnama_10_gdp\tdataset\n"
        result = interpret(task, body, self.today)
        self.assertEqual(result["record_count"], 2)
        self.assertEqual(result["next_tasks"], [])
        self.assertEqual(result["metadata"]["activation"], "metadata-only")


if __name__ == "__main__":
    unittest.main()
