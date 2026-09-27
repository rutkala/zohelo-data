"""Tests for resumable, complete Eurostat bulk recovery planning."""
from __future__ import annotations

from copy import deepcopy
from itertools import product
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.eurostat_bulk_recovery import (  # noqa: E402
    EurostatBulkRecoveryError,
    accept_partition,
    advance_async_recovery,
    apply_dataflow,
    apply_datastructure,
    build_partitions,
    pending_partition_requests,
    split_partition_on_413,
    start_413_recovery,
    start_async_recovery,
)


INITIAL = {
    "dataset_id": "demo_cube",
    "url": (
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/"
        "data/demo_cube"
    ),
    "params": {"format": "TSV", "compressed": "true"},
    "version": "2026-09-08T11:00:00+0200",
    "kind": "eurostat_tsv_gzip",
}

DATAFLOW = """\
<mes:Structure xmlns:mes="urn:sdmx:org.sdmx.infomodel.message:2.1"
 xmlns:str="urn:sdmx:org.sdmx.infomodel.structure:2.1"
 xmlns:com="urn:sdmx:org.sdmx.infomodel.common:2.1">
 <mes:Structures><str:Dataflows>
  <str:Dataflow agencyID="ESTAT" id="demo_cube" version="1.0">
   <str:Structure><com:Ref agencyID="ESTAT" id="DSD_DEMO" version="7.0"
     class="DataStructure"/></str:Structure>
  </str:Dataflow>
 </str:Dataflows></mes:Structures>
</mes:Structure>
"""

DSD = """\
<mes:Structure xmlns:mes="urn:sdmx:org.sdmx.infomodel.message:2.1"
 xmlns:str="urn:sdmx:org.sdmx.infomodel.structure:2.1"
 xmlns:com="urn:sdmx:org.sdmx.infomodel.common:2.1">
 <mes:Structures><str:DataStructures>
  <str:DataStructure agencyID="ESTAT" id="DSD_DEMO" version="7.0">
   <str:DataStructureComponents><str:DimensionList>
    <str:Dimension id="freq" position="1"><str:LocalRepresentation>
     <str:Enumeration><com:Ref agencyID="ESTAT" id="CL_FREQ" version="1.0"
       class="Codelist"/></str:Enumeration>
    </str:LocalRepresentation></str:Dimension>
    <str:Dimension id="geo" position="2"><str:LocalRepresentation>
     <str:Enumeration><com:Ref agencyID="ESTAT" id="CL_GEO" version="9.0"
       class="Codelist"/></str:Enumeration>
    </str:LocalRepresentation></str:Dimension>
    <str:TimeDimension id="TIME_PERIOD" position="3"/>
   </str:DimensionList></str:DataStructureComponents>
  </str:DataStructure>
 </str:DataStructures></mes:Structures>
</mes:Structure>
"""

CONSTRAINT_ALL_KEYS = """\
<mes:Structure xmlns:mes="urn:sdmx:org.sdmx.infomodel.message:2.1"
 xmlns:str="urn:sdmx:org.sdmx.infomodel.structure:2.1"
 xmlns:com="urn:sdmx:org.sdmx.infomodel.common:2.1">
 <mes:Structures><str:Constraints>
  <str:ContentConstraint agencyID="ESTAT" id="demo_cube" version="1.0">
   <str:ConstraintAttachment><str:Dataflow><com:Ref agencyID="ESTAT"
    id="demo_cube" version="1.0" class="Dataflow"/></str:Dataflow>
   </str:ConstraintAttachment>
   <str:CubeRegion include="true">
    <com:KeyValue id="freq"><com:Value>A</com:Value></com:KeyValue>
    <com:KeyValue id="geo"><com:Value>PL</com:Value><com:Value>DE</com:Value>
     <com:Value>CZ</com:Value></com:KeyValue>
    <com:TimeKeyValue id="TIME_PERIOD"><com:TimeRange>
     <com:StartPeriod>2020</com:StartPeriod><com:EndPeriod>2022</com:EndPeriod>
    </com:TimeRange></com:TimeKeyValue>
   </str:CubeRegion>
  </str:ContentConstraint>
 </str:Constraints></mes:Structures>
</mes:Structure>
"""

CODELISTS = """\
<mes:Structure xmlns:mes="urn:sdmx:org.sdmx.infomodel.message:2.1"
 xmlns:str="urn:sdmx:org.sdmx.infomodel.structure:2.1">
 <mes:Structures><str:Codelists>
  <str:Codelist agencyID="ESTAT" id="CL_FREQ" version="1.0">
   <str:Code id="A"/>
  </str:Codelist>
  <str:Codelist agencyID="ESTAT" id="CL_GEO" version="9.0">
   <str:Code id="PL"/><str:Code id="DE"/><str:Code id="CZ"/>
  </str:Codelist>
 </str:Codelists></mes:Structures>
</mes:Structure>
"""


def raw(name):
    return {"id": name, "sha256": name.ljust(64, "a")[:64], "size_bytes": 100}


def envelope(request_id, status):
    return (
        '<env:Envelope xmlns:env="urn:envelope"><env:Body><queued>'
        f"<id>{request_id}</id><status>{status}</status>"
        "<url>https://attacker.example/steal</url>"
        "</queued></env:Body></env:Envelope>"
    )


def fault(message):
    return (
        '<S:Fault xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">'
        f"<faultcode>100</faultcode><faultstring>{message}</faultstring></S:Fault>"
    )


def structured_plan(constraint=CONSTRAINT_ALL_KEYS, codelists=None):
    plan = start_413_recovery(INITIAL, raw("failure413"))
    plan = apply_dataflow(plan, DATAFLOW, raw("dataflow"))
    plan = apply_datastructure(plan, DSD, raw("datastructure"))
    return build_partitions(
        plan,
        constraint,
        {} if codelists is None else {"all": codelists},
        raw("constraint"),
    )


class EurostatAsyncRecoveryTests(unittest.TestCase):
    def test_poll_available_and_expired_resubmit_preserve_identity_and_evidence(self):
        plan = start_async_recovery(
            INITIAL, envelope("job-1", "PROCESSING"), raw("submitted")
        )
        self.assertEqual(plan["phase"], "poll")
        self.assertEqual(
            plan["next_request"]["url"],
            "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/status/job-1",
        )
        plan = advance_async_recovery(
            plan, envelope("job-1", "EXPIRED"), raw("expired")
        )
        self.assertEqual(plan["phase"], "resubmit")
        self.assertEqual(plan["next_request"], {
            "url": INITIAL["url"], "params": INITIAL["params"]
        })
        plan = advance_async_recovery(
            plan, envelope("job-2", "SUBMITTED"), raw("resubmitted")
        )
        plan = advance_async_recovery(
            plan, envelope("job-2", "AVAILABLE"), raw("available")
        )
        self.assertEqual(plan["phase"], "download")
        self.assertEqual(plan["initial_distribution"]["dataset_id"], "demo_cube")
        self.assertEqual(plan["initial_distribution"]["version"], INITIAL["version"])
        self.assertEqual(plan["submission_count"], 2)
        self.assertEqual(
            [item["id"] for item in plan["raw_envelopes"]],
            ["submitted", "expired", "resubmitted", "available"],
        )
        self.assertNotIn("attacker", str(plan))

    def test_malicious_stored_or_provider_urls_are_never_followed(self):
        plan = start_async_recovery(
            INITIAL, envelope("job-safe", "PROCESSING"), raw("initial")
        )
        changed = deepcopy(plan)
        changed["next_request"]["url"] = "https://attacker.example/poll"
        with self.assertRaisesRegex(EurostatBulkRecoveryError, "authoritative"):
            advance_async_recovery(
                changed, envelope("job-safe", "AVAILABLE"), raw("next")
            )
        unsafe_initial = deepcopy(INITIAL)
        unsafe_initial["url"] = "https://attacker.example/data/demo_cube"
        with self.assertRaisesRegex(EurostatBulkRecoveryError, "authoritative"):
            start_async_recovery(
                unsafe_initial, envelope("job-safe", "SUBMITTED"), raw("unsafe")
            )

    def test_async_recovery_accepts_a_canonical_partition_key_url(self):
        partition = deepcopy(INITIAL)
        partition["url"] += "/A.PL+DE"
        partition_id = "partition:" + "a" * 64
        partition.update({
            "dataset_id": "demo_cube::partition::" + "a" * 64,
            "original_dataset_id": "demo_cube",
            "partition_id": partition_id,
        })
        plan = start_async_recovery(
            partition, envelope("job-partition", "SUBMITTED"), raw("partition")
        )
        self.assertEqual(plan["initial_distribution"]["url"], partition["url"])

        malformed = deepcopy(partition)
        malformed["url"] = INITIAL["url"] + "/A.%2e%2e"
        with self.assertRaisesRegex(EurostatBulkRecoveryError, "canonical"):
            start_async_recovery(
                malformed, envelope("job-partition", "SUBMITTED"), raw("bad-key")
            )

    def test_documented_faults_retry_or_finish_only_an_empty_partition(self):
        plan = start_async_recovery(
            INITIAL, envelope("job-fault", "AVAILABLE"), raw("available")
        )
        plan = advance_async_recovery(
            plan,
            fault("DATA_NOT_YET_AVAILABLE: Requested data is not yet available."),
            raw("not-yet"),
        )
        self.assertEqual(plan["phase"], "poll")
        plan = advance_async_recovery(
            plan, fault("UNKNOWN_REQUEST: Unknown request."), raw("unknown")
        )
        self.assertEqual(plan["phase"], "resubmit")
        self.assertEqual(plan["next_request"]["url"], INITIAL["url"])

        with self.assertRaisesRegex(EurostatBulkRecoveryError, "unpartitioned"):
            advance_async_recovery(
                start_async_recovery(
                    INITIAL, envelope("job-empty", "AVAILABLE"), raw("empty-ready")
                ),
                fault("NO_RESULTS: The query did not return any results."),
                raw("empty-parent"),
            )

        partition = deepcopy(INITIAL)
        partition.update({
            "dataset_id": "demo_cube::partition::" + "b" * 64,
            "original_dataset_id": "demo_cube",
            "partition_id": "partition:" + "b" * 64,
            "url": INITIAL["url"] + "/A.PL",
        })
        partition_plan = start_async_recovery(
            partition, envelope("job-empty-leaf", "AVAILABLE"), raw("leaf-ready")
        )
        partition_plan = advance_async_recovery(
            partition_plan,
            fault("NO_RESULTS: The query did not return any results."),
            raw("empty-leaf"),
        )
        self.assertEqual(partition_plan["phase"], "empty_partition")
        self.assertEqual(partition_plan["next_request"], {})


class EurostatPartitionRecoveryTests(unittest.TestCase):
    def test_complete_constraint_partitions_have_no_holes_or_overlap(self):
        plan = structured_plan()
        requests = pending_partition_requests(plan)
        self.assertEqual(len(requests), 2)
        self.assertTrue(all("startPeriod" not in item["request"]["params"] for item in requests))
        target = next(
            item["partition_id"]
            for item in requests if "+" in item["request"]["url"]
        )
        plan = split_partition_on_413(plan, target, raw("repeat413"))

        coverage = []
        leaf_ids = []
        for partition_id, partition in plan["partitions"].items():
            if partition["status"] == "split":
                continue
            leaf_ids.append(partition_id)
            selected = partition["selections"]
            geos = selected.get("geo", plan["domains"]["geo"])
            coverage.extend(product(geos, plan["time_periods"]))
        expected = set(product(["PL", "DE", "CZ"], ["2020", "2021", "2022"]))
        self.assertEqual(set(coverage), expected)
        self.assertEqual(len(coverage), len(expected))

        for partition_id in leaf_ids[:-1]:
            plan = accept_partition(plan, partition_id, raw("accepted" + partition_id[-4:]))
        self.assertEqual(plan["phase"], "partitions_pending")
        plan = accept_partition(plan, leaf_ids[-1], raw("accepted-final"))
        self.assertEqual(plan["phase"], "complete")
        self.assertEqual(pending_partition_requests(plan), [])

    def test_codelists_supply_domains_missing_from_constraint(self):
        time_only = CONSTRAINT_ALL_KEYS.replace(
            '<com:KeyValue id="freq"><com:Value>A</com:Value></com:KeyValue>', ""
        ).replace(
            '<com:KeyValue id="geo"><com:Value>PL</com:Value><com:Value>DE</com:Value>\n'
            '     <com:Value>CZ</com:Value></com:KeyValue>',
            "",
        )
        plan = structured_plan(time_only, CODELISTS)
        self.assertEqual(plan["domains"]["freq"], ["A"])
        self.assertEqual(plan["domains"]["geo"], ["PL", "DE", "CZ"])

    def test_mixed_frequencies_keep_all_time_and_append_keys_before_query(self):
        initial = deepcopy(INITIAL)
        initial["url"] += "?format=TSV&compressed=true"
        initial["params"] = {}
        mixed = CONSTRAINT_ALL_KEYS.replace(
            "<com:Value>A</com:Value></com:KeyValue>",
            "<com:Value>A</com:Value><com:Value>M</com:Value></com:KeyValue>",
        ).replace(
            "<com:TimeRange>\n"
            "     <com:StartPeriod>2020</com:StartPeriod><com:EndPeriod>2022</com:EndPeriod>\n"
            "    </com:TimeRange>",
            "<com:Value>2019</com:Value><com:Value>2019-01</com:Value>",
        )
        plan = start_413_recovery(initial, raw("mixed413"))
        plan = apply_dataflow(plan, DATAFLOW, raw("dataflow"))
        plan = apply_datastructure(plan, DSD, raw("datastructure"))
        plan = build_partitions(plan, mixed, {}, raw("constraint"))

        requests = [item["request"] for item in pending_partition_requests(plan)]
        self.assertEqual(plan["time_periods"], ["2019", "2019-01"])
        self.assertEqual(len(requests), 2)
        self.assertTrue(all("startPeriod" not in item["params"] for item in requests))
        self.assertTrue(all("/data/demo_cube/" in item["url"] for item in requests))
        self.assertTrue(
            all(
                item["url"].endswith("?format=TSV&compressed=true")
                for item in requests
            )
        )

    def test_unknown_or_incomplete_constraints_fail_closed(self):
        unknown = CONSTRAINT_ALL_KEYS.replace(
            '<com:KeyValue id="freq">',
            '<com:KeyValue id="invented">',
        )
        with self.assertRaisesRegex(EurostatBulkRecoveryError, "unknown dimensions"):
            structured_plan(unknown)

        plan = start_413_recovery(INITIAL, raw("failure413"))
        plan = apply_dataflow(plan, DATAFLOW, raw("dataflow"))
        unordered = DSD.replace('position="2"', 'position="4"')
        with self.assertRaisesRegex(EurostatBulkRecoveryError, "fully ordered"):
            apply_datastructure(plan, unordered, raw("bad-dsd"))

        singleton = CONSTRAINT_ALL_KEYS.replace(
            "<com:Value>PL</com:Value><com:Value>DE</com:Value>\n"
            "     <com:Value>CZ</com:Value>",
            "<com:Value>PL</com:Value>",
        )
        with self.assertRaisesRegex(EurostatBulkRecoveryError, "cannot be split further"):
            structured_plan(singleton)

    def test_ds_trade_routes_use_separate_comext_base(self):
        distribution = {
            **INITIAL,
            "dataset_id": "DS-057555",
            "url": (
                "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/"
                "data/DS-057555"
            ),
        }
        plan = start_413_recovery(distribution, raw("comext413"))
        self.assertIn("/api/comext/dissemination/", str(plan["structure_requests"]))

    def test_mutated_partition_url_is_rejected_before_use(self):
        plan = structured_plan()
        partition_id = next(iter(plan["partitions"]))
        plan["partitions"][partition_id]["request"]["url"] = (
            "https://attacker.example/partition"
        )
        with self.assertRaisesRegex(EurostatBulkRecoveryError, "authoritative"):
            pending_partition_requests(plan)


if __name__ == "__main__":
    unittest.main()
