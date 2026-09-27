import gzip
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ingestion.full_source_adapters import (  # noqa: E402
    EUROSTAT_INVENTORY_URL,
    FullSourceAdapterError,
    WDI_ZIP_URL,
    distributions_from_inventory,
    initial_distributions,
    inspect_distribution,
    parse_catalogue,
    parse_eurostat_async,
)


INVENTORY = """Code\tTitle\tData download url (tsv)\tData download url (csv)\tLast data change\tLast structural change\tData structure download url
demo_pjan\tPopulation\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo_pjan/?format=TSV&lang=en\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo_pjan?format=SDMX-CSV\t2026-09-10T11:00:00+0200\t2026-09-01T11:00:00+0200\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/demo_pjan/?references=descendants&format=sdmx_2.1_generic
nama_10_gdp\tGDP\t\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10_gdp?format=SDMX-CSV\t2026-09-09T23:00:00+0200\t2026-09-01T11:00:00+0200\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/nama_10_gdp/?references=descendants&format=sdmx_2.1_generic
prc_hicp_minr\tHICP\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/prc_hicp_minr?format=TSV\t\t2026-09-08T11:00:00+0200\t2026-09-01T11:00:00+0200\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/prc_hicp_minr/?references=descendants&format=sdmx_2.1_generic
DS-057555\tTrade\thttps://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/DS-057555?format=TSV\t\t2026-09-07T11:00:00+0200\t2026-09-01T11:00:00+0200\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/DS-057555/?references=descendants&format=sdmx_2.1_generic
"""

CODELIST_INVENTORY = """Code\tSource\tVersion\tLabel\tSpecific tsv download url\tSpecific sdmx download url
GEO\tESTAT\t13.0\tGeopolitical entity\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/codelist/ESTAT/GEO/13.0?format=TSV&formatVersion=2.0\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/codelist/ESTAT/GEO/13.0
UNIT\tESTAT\t5.7\tUnit of measure\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/structure/codelist/ESTAT/UNIT/5.7?format=TSV&formatVersion=2.0\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/codelist/ESTAT/UNIT/5.7
"""

METADATA_INVENTORY = """Code\tEsms package download url\tHtml metadata download url\tLast metadata change
aact_esmsea_is\thttps://ec.europa.eu/eurostat/api/dissemination/files?file=metadata/aact_esmsea_is.sdmx.zip\thttps://ec.europa.eu/eurostat/cache/metadata/en/aact_esmsea_is.htm\t2026-02-05T16:13:25.837+01:00
"""


class FullSourcePlanningTests(unittest.TestCase):
    def test_wdi_is_one_complete_official_zip_with_no_artificial_scope(self):
        self.assertEqual(
            initial_distributions("world_bank_wdi"),
            [
                {
                    "dataset_id": "WDI",
                    "url": WDI_ZIP_URL,
                    "params": {},
                    "version": None,
                    "kind": "wdi_zip",
                }
            ],
        )
        self.assertEqual(initial_distributions("wdi"), initial_distributions("world_bank_wdi"))
        with self.assertRaisesRegex(FullSourceAdapterError, "does not use"):
            initial_distributions("world_bank_wdi", b"invented catalogue")

    def test_eurostat_starts_with_the_complete_official_inventory(self):
        inventories = initial_distributions("eurostat")
        self.assertEqual(len(inventories), 3)
        self.assertEqual(
            [item["params"]["type"] for item in inventories],
            ["data", "codelist", "metadata"],
        )
        self.assertTrue(all(item["url"] == EUROSTAT_INVENTORY_URL for item in inventories))
        self.assertEqual(
            [item["kind"] for item in inventories],
            [
                "eurostat_inventory",
                "eurostat_inventory_codelist",
                "eurostat_inventory_metadata",
            ],
        )

    def test_eurostat_inventory_plans_every_row_and_preserves_metadata(self):
        rows = parse_catalogue("eurostat", INVENTORY.encode("utf-8"))
        self.assertEqual(
            [row["dataset_id"] for row in rows],
            ["demo_pjan", "nama_10_gdp", "prc_hicp_minr", "DS-057555"],
        )
        self.assertEqual(rows[0]["route_source"], "inventory")
        self.assertEqual(rows[0]["inventory_fields"]["Title"], "Population")
        self.assertEqual(rows[1]["route_source"], "official_template")
        self.assertEqual(
            rows[1]["tsv_url"],
            "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10_gdp",
        )
        self.assertIn("/api/comext/dissemination/", rows[-1]["tsv_url"])

        distributions = initial_distributions("eurostat", INVENTORY)
        self.assertEqual(len(distributions), 8)
        data = [item for item in distributions if item["kind"] == "eurostat_tsv_gzip"]
        structures = [
            item for item in distributions if item["kind"] == "eurostat_structure_sdmx"
        ]
        self.assertEqual(len(data), 4)
        self.assertEqual(len(structures), 4)
        self.assertEqual(data[0]["params"], {"compressed": "true"})
        self.assertEqual(
            data[1]["params"], {"format": "TSV", "compressed": "true"}
        )
        self.assertEqual(structures[0]["version"], "2026-09-01T11:00:00+0200")
        self.assertEqual(distributions[0]["version"], "2026-09-10T11:00:00+0200")
        self.assertEqual(distributions[0]["catalogue_metadata"], rows[0])

    def test_codelist_and_metadata_inventories_plan_all_exact_official_files(self):
        codelists = distributions_from_inventory("codelist", CODELIST_INVENTORY)
        self.assertEqual([item["dataset_id"] for item in codelists], ["GEO", "UNIT"])
        self.assertEqual(codelists[0]["version"], "13.0")
        self.assertEqual(codelists[0]["params"], {})
        self.assertIn("/GEO/13.0?format=TSV", codelists[0]["url"])

        metadata = distributions_from_inventory(
            "eurostat_inventory_metadata", METADATA_INVENTORY
        )
        self.assertEqual(len(metadata), 1)
        self.assertEqual(metadata[0]["kind"], "eurostat_metadata_zip")
        self.assertEqual(metadata[0]["version"], "2026-02-05T16:13:25.837+01:00")
        self.assertEqual(
            metadata[0]["url"],
            "https://ec.europa.eu/eurostat/api/dissemination/files?file="
            "metadata/aact_esmsea_is.sdmx.zip",
        )

    def test_inventory_rejects_malformed_duplicate_and_unsafe_routes(self):
        with self.assertRaisesRegex(FullSourceAdapterError, "no Code"):
            parse_catalogue("eurostat", "Title\nPopulation\n")
        with self.assertRaisesRegex(FullSourceAdapterError, "repeats dataset"):
            parse_catalogue(
                "eurostat",
                "Code\tData download url (tsv)\tData structure download url\n"
                "DEMO_PJAN\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/DEMO_PJAN?format=TSV\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/DEMO_PJAN?references=descendants&format=sdmx_2.1_generic\n"
                "demo_pjan\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo_pjan?format=TSV\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/demo_pjan?references=descendants&format=sdmx_2.1_generic\n",
            )
        unsafe = (
            "Code\tData download url (tsv)\tData structure download url\n"
            "demo_pjan\thttps://attacker.example/data/demo_pjan?format=TSV\t"
            "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/demo_pjan?references=descendants&format=sdmx_2.1_generic\n"
        )
        with self.assertRaisesRegex(FullSourceAdapterError, "unsafe TSV URL"):
            parse_catalogue("eurostat", unsafe)
        wrong_dataset = (
            "Code\tData download url (tsv)\tData structure download url\n"
            "demo_pjan\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/other?format=TSV\t"
            "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/demo_pjan?references=descendants&format=sdmx_2.1_generic\n"
        )
        with self.assertRaisesRegex(FullSourceAdapterError, "does not match"):
            parse_catalogue("eurostat", wrong_dataset)
        injected_query = (
            "Code\tData download url (tsv)\tData structure download url\n"
            "demo_pjan\thttps://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo_pjan?format=TSV&url=https://attacker.example\t"
            "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/demo_pjan?references=descendants&format=sdmx_2.1_generic\n"
        )
        with self.assertRaisesRegex(FullSourceAdapterError, "unsafe TSV URL parameter"):
            parse_catalogue("eurostat", injected_query)

        unsafe_metadata = METADATA_INVENTORY.replace(
            "https://ec.europa.eu/eurostat/api/dissemination/files",
            "https://attacker.example/files",
        )
        with self.assertRaisesRegex(FullSourceAdapterError, "unsafe metadata URL"):
            parse_catalogue("eurostat", unsafe_metadata, "metadata")


class FullSourceInspectionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.folder = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_wdi_zip_streams_every_member_and_reports_csv_shape_and_years(self):
        archive = self.folder / "WDI_CSV.zip"
        data = (
            '"Country Name","Country Code","Indicator Name","Indicator Code","1960","2025"\r\n'
            '"Poland","POL","Population","SP.POP.TOTL","29637450","36620000"\r\n'
        )
        series = '"Series Code","Indicator Name"\r\n"SP.POP.TOTL","Population"\r\n'
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as target:
            target.writestr("WDI_CSV/WDICSV.csv", data)
            target.writestr(
                "WDI_CSV/WDICountry.csv",
                '"Country Code","Long Name"\r\n"POL","Poland"\r\n',
            )
            target.writestr("WDI_CSV/WDISeries.csv", series)
            target.writestr("WDI_CSV/README.txt", "exact provider notes")

        result = inspect_distribution(archive, "wdi_zip")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["member_count"], 4)
        self.assertEqual(result["year_columns"], ["1960", "2025"])
        self.assertEqual(result["year_range"], ["1960", "2025"])
        self.assertEqual(result["sha256"], hashlib.sha256(archive.read_bytes()).hexdigest())
        by_name = {member["name"]: member for member in result["members"]}
        self.assertEqual(by_name["WDI_CSV/WDICSV.csv"]["row_count"], 1)
        self.assertEqual(
            by_name["WDI_CSV/WDICountry.csv"]["columns"],
            ["Country Code", "Long Name"],
        )
        self.assertRegex(by_name["WDI_CSV/README.txt"]["crc32"], r"^[0-9a-f]{8}$")

    def test_wdi_zip_rejects_unsafe_members_and_crc_corruption(self):
        unsafe = self.folder / "unsafe.zip"
        with zipfile.ZipFile(unsafe, "w") as target:
            target.writestr("../WDICSV.csv", "Code,1960\nPOL,1\n")
        with self.assertRaisesRegex(FullSourceAdapterError, "unsafe member path"):
            inspect_distribution(unsafe, "wdi_zip")

        corrupt = self.folder / "corrupt.zip"
        payload = b"Code,1960\nPOL,123456789\n"
        with zipfile.ZipFile(corrupt, "w", compression=zipfile.ZIP_STORED) as target:
            target.writestr("WDICSV.csv", payload)
        raw = bytearray(corrupt.read_bytes())
        offset = raw.find(payload)
        self.assertGreaterEqual(offset, 0)
        raw[offset + len(payload) - 3] ^= 1
        corrupt.write_bytes(raw)
        with self.assertRaisesRegex(FullSourceAdapterError, "invalid WDI ZIP member"):
            inspect_distribution(corrupt, "wdi_zip")

    def test_eurostat_gzip_preserves_file_and_reports_null_empty_and_flags(self):
        target = self.folder / "demo.tsv.gz"
        tsv = (
            "freq,unit,geo\\TIME_PERIOD\t2020 \t2021-01 \t2022-Q1 \n"
            "A,NR,PL\t123 p\t:\t\n"
            "A,NR,DE\t456 \t: z\t789 e\n"
        ).encode("utf-8")
        # Deterministic fixture bytes keep the assertion focused on inspection
        # leaving the exact download untouched.
        target.write_bytes(gzip.compress(tsv, mtime=0))
        exact = target.read_bytes()

        result = inspect_distribution(target, "eurostat_tsv_gzip")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["series_dimensions"], ["freq", "unit", "geo"])
        self.assertEqual(result["periods"], ["2020", "2021-01", "2022-Q1"])
        self.assertEqual(result["year_range"], ["2020", "2022"])
        self.assertEqual(result["empty_cell_count"], 1)
        self.assertEqual(result["null_cell_count"], 2)
        self.assertEqual(result["flagged_cell_count"], 3)
        self.assertEqual(target.read_bytes(), exact)

    def test_header_only_tsv_is_a_valid_empty_dataset(self):
        target = self.folder / "empty.tsv.gz"
        target.write_bytes(gzip.compress(b"freq,geo\\TIME_PERIOD\t2025 \n", mtime=0))
        result = inspect_distribution(target, "eurostat_tsv_gzip")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["row_count"], 0)
        self.assertEqual(result["periods"], ["2025"])

    def test_codelist_tsv_and_streamed_sdmx_structure_are_inspected(self):
        codelist = self.folder / "geo.tsv"
        codelist.write_text(
            "CODE\tLABEL\nPL\tPoland\nDE\tGermany\n", encoding="utf-8"
        )
        codelist_result = inspect_distribution(codelist, "eurostat_codelist_tsv")
        self.assertEqual(codelist_result["status"], "complete")
        self.assertEqual(codelist_result["row_count"], 2)
        self.assertEqual(codelist_result["columns"], ["CODE", "LABEL"])

        structure = self.folder / "structure.xml"
        structure.write_bytes(
            b'<mes:Structure xmlns:mes="urn:sdmx:message" '
            b'xmlns:str="urn:sdmx:structure"><str:Dataflow id="demo">'
            b"<str:Name>Population</str:Name></str:Dataflow></mes:Structure>"
        )
        structure_result = inspect_distribution(
            structure, "eurostat_structure_sdmx"
        )
        self.assertEqual(structure_result["status"], "complete")
        self.assertEqual(structure_result["xml_root"], "Structure")
        self.assertEqual(structure_result["xml_element_count"], 3)
        self.assertEqual(
            structure_result["xml_element_names"],
            ["Dataflow", "Name", "Structure"],
        )

    def test_metadata_zip_streams_all_members_and_rejects_xml_entities(self):
        package = self.folder / "metadata.sdmx.zip"
        with zipfile.ZipFile(package, "w", compression=zipfile.ZIP_DEFLATED) as target:
            target.writestr(
                "report.xml",
                '<Report xmlns="urn:sdmx"><Name>Quality report</Name></Report>',
            )
            target.writestr("notes.txt", "provider metadata notes")
        result = inspect_distribution(package, "eurostat_metadata_zip")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["member_count"], 2)
        xml_member = next(item for item in result["members"] if item["name"] == "report.xml")
        self.assertEqual(xml_member["xml_root"], "Report")
        self.assertEqual(xml_member["xml_element_count"], 2)

        unsafe = self.folder / "unsafe-metadata.sdmx.zip"
        with zipfile.ZipFile(unsafe, "w") as target:
            target.writestr(
                "report.xml",
                '<!DOCTYPE x [<!ENTITY leaked "bad">]><Report>&leaked;</Report>',
            )
        with self.assertRaisesRegex(FullSourceAdapterError, "DTD/entity"):
            inspect_distribution(unsafe, "eurostat_metadata_zip")

    def test_async_payload_and_unknown_kind_have_explicit_statuses(self):
        async_file = self.folder / "async.gz"
        async_file.write_bytes(
            b'<env:Envelope xmlns:env="urn:envelope"><env:Body><queued>'
            b"<id>98de05ea-540a-43d3-903b-7c9e14faf808</id>"
            b"<status>SUBMITTED</status></queued></env:Body></env:Envelope>"
        )
        result = inspect_distribution(async_file, "eurostat_tsv_gzip")
        self.assertEqual(result["status"], "async")
        self.assertEqual(result["byte_size"], async_file.stat().st_size)
        self.assertEqual(result["request_id"], "98de05ea-540a-43d3-903b-7c9e14faf808")
        self.assertEqual(
            result["poll"]["url"],
            "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/status/"
            "98de05ea-540a-43d3-903b-7c9e14faf808",
        )

        unsupported = inspect_distribution(async_file, "future_provider_format")
        self.assertEqual(unsupported["status"], "unsupported")

    def test_async_parser_preserves_initial_request_and_never_follows_response_urls(self):
        body = (
            '<env:Envelope xmlns:env="urn:envelope"><env:Body><queued>'
            "<id>job-123</id><status>PROCESSING</status>"
            "<url>https://attacker.example/result</url>"
            "</queued></env:Body></env:Envelope>"
        )
        initial = {
            "url": "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/demo",
            "params": {"format": "TSV", "compressed": "true"},
        }
        result = parse_eurostat_async(body, initial)
        self.assertEqual(result["initial_request"], initial)
        self.assertEqual(
            result["ready"]["url"],
            "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/data/job-123",
        )
        self.assertNotIn("attacker", str(result))

        unsafe = body.replace("job-123", "../../secret")
        with self.assertRaisesRegex(FullSourceAdapterError, "safe request id"):
            parse_eurostat_async(unsafe)

    def test_official_async_poll_key_shape_and_terminal_statuses(self):
        template = (
            '<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">'
            '<env:Body><ns0:asyncResponse xmlns:ns0="urn:async" xmlns:ns1="urn:status">'
            "<ns1:status><ns1:key>98de05ea-540a-43d3-903b-7c9e14faf808</ns1:key>"
            "<ns1:status>{status}</ns1:status></ns1:status>"
            "</ns0:asyncResponse></env:Body></env:Envelope>"
        )
        expectations = {
            "PROCESSING": "async",
            "AVAILABLE": "available",
            "EXPIRED": "expired",
            "UNKNOWN_REQUEST": "async_error",
            "ERROR": "async_error",
        }
        for provider_status, status in expectations.items():
            with self.subTest(provider_status=provider_status):
                result = parse_eurostat_async(
                    template.format(status=provider_status)
                )
                self.assertEqual(result["status"], status)
                self.assertEqual(result["provider_status"], provider_status)
                self.assertEqual(
                    result["request_id"],
                    "98de05ea-540a-43d3-903b-7c9e14faf808",
                )

        conflicting = template.format(status="PROCESSING").replace(
            "<ns1:status>PROCESSING</ns1:status>",
            "<ns1:id>different-id</ns1:id><ns1:status>PROCESSING</ns1:status>",
        )
        with self.assertRaisesRegex(FullSourceAdapterError, "single safe request id"):
            parse_eurostat_async(conflicting)

    def test_official_soap_faults_have_explicit_recovery_status(self):
        fault_template = (
            '<S:Fault xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">'
            "<faultcode>{code}</faultcode><faultstring>{message}</faultstring></S:Fault>"
        )
        cases = [
            (
                "413",
                "EXTRACTION_TOO_BIG: The requested extraction is too big",
                "unsupported",
                "EXTRACTION_TOO_BIG",
            ),
            (
                "100",
                "NO_RESULTS: The query did not return any results.",
                "no_results",
                "NO_RESULTS",
            ),
            (
                "100",
                "DATA_NOT_YET_AVAILABLE: Requested data is not yet available.",
                "async",
                "DATA_NOT_YET_AVAILABLE",
            ),
            (
                "100",
                "UNKNOWN_REQUEST: Unknown request.",
                "async_error",
                "UNKNOWN_REQUEST",
            ),
        ]
        for code, message, status, fault_type in cases:
            with self.subTest(fault_type=fault_type):
                result = parse_eurostat_async(
                    fault_template.format(code=code, message=message)
                )
                self.assertEqual(result["status"], status)
                self.assertEqual(result["fault_code"], code)
                self.assertEqual(result["fault_type"], fault_type)
                if code == "413":
                    self.assertEqual(result["http_status"], 413)

        target = self.folder / "no-results.xml"
        target.write_text(
            fault_template.format(
                code="100",
                message="NO_RESULTS: The query did not return any results.",
            ),
            encoding="utf-8",
        )
        inspected = inspect_distribution(target, "eurostat_tsv_gzip")
        self.assertEqual(inspected["status"], "no_results")
        self.assertEqual(inspected["fault_type"], "NO_RESULTS")

    def test_provider_413_is_an_explicit_future_partition_blocker(self):
        target = self.folder / "too-large.gz"
        target.write_bytes(b'{"error":"too many cells","status":413}')
        result = inspect_distribution(target, "eurostat_tsv_gzip")
        self.assertEqual(result["status"], "unsupported")
        self.assertEqual(result["http_status"], 413)

    def test_known_malformed_payloads_fail_instead_of_looking_complete(self):
        not_gzip = self.folder / "not-gzip.tsv.gz"
        not_gzip.write_text("freq,geo\\TIME_PERIOD\t2025\n", encoding="utf-8")
        with self.assertRaisesRegex(FullSourceAdapterError, "not a GZIP"):
            inspect_distribution(not_gzip, "eurostat_tsv_gzip")

        ragged = self.folder / "ragged.tsv.gz"
        ragged.write_bytes(
            gzip.compress(b"freq,geo\\TIME_PERIOD\t2024\t2025\nA,PL\t1\n", mtime=0)
        )
        with self.assertRaisesRegex(FullSourceAdapterError, "expected 3"):
            inspect_distribution(ragged, "eurostat_tsv_gzip")

    def test_inventory_inspection_reports_all_dataset_versions(self):
        target = self.folder / "inventory.txt"
        target.write_text(INVENTORY, encoding="utf-8")
        result = inspect_distribution(target, "eurostat_inventory")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["dataset_count"], 4)
        self.assertEqual(result["dataset_ids"][-1], "DS-057555")
        self.assertEqual(result["versions"]["demo_pjan"], "2026-09-10T11:00:00+0200")


if __name__ == "__main__":
    unittest.main()
