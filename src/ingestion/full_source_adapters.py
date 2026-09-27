"""Pure planning and local inspection helpers for complete source distributions.

Network transport, retries, exact-byte persistence, and durable queue state belong to
the caller.  This module only turns trusted source catalogues into allowlisted request
descriptors and inspects completed local downloads without loading them into memory.
"""

from __future__ import annotations

import csv
from copy import deepcopy
import gzip
import hashlib
import io
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any, BinaryIO, Mapping
from urllib.parse import parse_qsl, urlsplit
import zipfile
import xml.etree.ElementTree as ET


WDI_SOURCE_ID = "world_bank_wdi"
EUROSTAT_SOURCE_ID = "eurostat"

WDI_ZIP_URL = "https://databank.worldbank.org/data/download/WDI_CSV.zip"
WDI_REDIRECT_URL = (
    "https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip"
)
EUROSTAT_INVENTORY_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/files/inventory"
)
EUROSTAT_DATA_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{dataset}"
)
EUROSTAT_COMEXT_DATA_URL = (
    "https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/{dataset}"
)
EUROSTAT_ASYNC_STATUS_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/status/{request_id}"
)
EUROSTAT_ASYNC_DATA_URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/1.0/async/data/{request_id}"
)

_DATASET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.$-]{0,127}$")
_YEAR_RE = re.compile(r"^(?:18|19|20|21)\d{2}(?:$|[-_])")
_SOURCE_ALIASES = {
    "wdi": WDI_SOURCE_ID,
    "world_bank": WDI_SOURCE_ID,
    WDI_SOURCE_ID: WDI_SOURCE_ID,
    EUROSTAT_SOURCE_ID: EUROSTAT_SOURCE_ID,
}
_EUROSTAT_PATH_RE = re.compile(
    r"^/eurostat/api/(?:comext/)?dissemination/sdmx/2\.1/data/([^/]+)/?$",
    re.IGNORECASE,
)
_EUROSTAT_ALLOWED_QUERY_KEYS = {"format", "lang", "compressed"}
_ASYNC_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,127}$")
_KNOWN_KINDS = {
    "wdi_zip",
    "eurostat_inventory",
    "eurostat_inventory_codelist",
    "eurostat_inventory_metadata",
    "eurostat_tsv_gzip",
    "eurostat_structure_sdmx",
    "eurostat_codelist_tsv",
    "eurostat_metadata_zip",
}
_INVENTORY_KINDS = {
    "data": "eurostat_inventory",
    "codelist": "eurostat_inventory_codelist",
    "metadata": "eurostat_inventory_metadata",
}


class FullSourceAdapterError(ValueError):
    """A catalogue, distribution descriptor, or local payload is invalid."""


def initial_distributions(
    source_id: str, inventory_body: bytes | str | None = None
) -> list[dict[str, Any]]:
    """Return complete-distribution request descriptors for a supported source.

    Eurostat is a two-stage plan.  The first call returns its full inventory request;
    after that exact response is retained, pass its bytes back as ``inventory_body``
    to plan every listed dataset.  No geography, topic, or dataset limit is applied.
    """
    source = _source(source_id)
    if source == WDI_SOURCE_ID:
        if inventory_body is not None:
            raise FullSourceAdapterError("WDI does not use an external inventory body")
        return [
            {
                "dataset_id": "WDI",
                "url": WDI_ZIP_URL,
                "params": {},
                "version": None,
                "kind": "wdi_zip",
            }
        ]

    if inventory_body is None:
        return [
            {
                "dataset_id": f"__inventory_{inventory_type}__",
                "url": EUROSTAT_INVENTORY_URL,
                "params": {"type": inventory_type, "lang": "en"},
                "version": None,
                "kind": kind,
            }
            for inventory_type, kind in _INVENTORY_KINDS.items()
        ]

    return distributions_from_inventory("data", inventory_body)


def distributions_from_inventory(
    inventory_kind: str, body: bytes | str
) -> list[dict[str, Any]]:
    """Plan every exact distribution listed by one official Eurostat inventory."""
    inventory_type = _inventory_type(inventory_kind)
    rows = parse_catalogue(EUROSTAT_SOURCE_ID, body, inventory_type)
    planned: list[dict[str, Any]] = []
    for row in rows:
        if inventory_type == "data":
            planned.append(
                {
                    "dataset_id": row["dataset_id"],
                    "url": row["tsv_url"],
                    "params": row["request_params"],
                    "version": row["last_data_change"],
                    "kind": "eurostat_tsv_gzip",
                    "catalogue_metadata": row,
                }
            )
            if row["structure_url"]:
                planned.append(
                    {
                        "dataset_id": f"{row['dataset_id']}::__structure__",
                        "url": row["structure_url"],
                        "params": {},
                        "version": row["last_structural_change"],
                        "kind": "eurostat_structure_sdmx",
                        "catalogue_metadata": row,
                    }
                )
        elif inventory_type == "codelist":
            planned.append(
                {
                    "dataset_id": row["dataset_id"],
                    "url": row["tsv_url"],
                    "params": {},
                    "version": row["version"],
                    "kind": "eurostat_codelist_tsv",
                    "catalogue_metadata": row,
                }
            )
        else:
            planned.append(
                {
                    "dataset_id": row["dataset_id"],
                    "url": row["package_url"],
                    "params": {},
                    "version": row["last_metadata_change"],
                    "kind": "eurostat_metadata_zip",
                    "catalogue_metadata": row,
                }
            )
    return planned


def parse_catalogue(
    source_id: str, body: bytes | str, inventory_kind: str = "data"
) -> list[dict[str, Any]]:
    """Parse a complete source catalogue into JSON-serializable metadata rows."""
    source = _source(source_id)
    if source != EUROSTAT_SOURCE_ID:
        raise FullSourceAdapterError(f"{source} has no separately parsed catalogue")

    inventory_type = _inventory_type(inventory_kind)
    text = _decode_text(body, f"Eurostat {inventory_type} inventory")
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter="\t")
    if reader.fieldnames is None:
        raise FullSourceAdapterError("Eurostat inventory has no header")
    headings = {_heading(name): name for name in reader.fieldnames if name is not None}
    code_heading = headings.get("code")
    if code_heading is None:
        raise FullSourceAdapterError("Eurostat inventory has no Code column")
    title_heading = headings.get("title") or headings.get("label")
    if inventory_type == "data":
        version_heading = headings.get("last data change")
        structural_heading = headings.get("last structural change")
        tsv_heading = headings.get("data download url (tsv)")
        structure_heading = headings.get("data structure download url")
        if tsv_heading is None:
            raise FullSourceAdapterError(
                "Eurostat data inventory has no Data download url (tsv) column"
            )
        if structure_heading is None:
            raise FullSourceAdapterError(
                "Eurostat data inventory has no Data structure download url column"
            )
    elif inventory_type == "codelist":
        version_heading = headings.get("version")
        tsv_heading = headings.get("specific tsv download url")
        if tsv_heading is None:
            raise FullSourceAdapterError(
                "Eurostat codelist inventory has no Specific tsv download url column"
            )
    else:
        version_heading = headings.get("last metadata change")
        package_heading = headings.get("esms package download url")
        if package_heading is None:
            raise FullSourceAdapterError(
                "Eurostat metadata inventory has no Esms package download url column"
            )

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line_number, raw in enumerate(reader, start=2):
        if None in raw:
            raise FullSourceAdapterError(
                f"Eurostat inventory row {line_number} has more fields than its header"
            )
        if not any((value or "").strip() for value in raw.values()):
            continue
        dataset_id = (raw.get(code_heading) or "").strip()
        _validate_dataset_id(dataset_id, line_number)
        identity = dataset_id.upper()
        if identity in seen:
            raise FullSourceAdapterError(
                f"Eurostat inventory repeats dataset code {dataset_id!r}"
            )
        seen.add(identity)

        inventory_fields = {
            str(key): value
            for key, value in raw.items()
            if key is not None and value not in (None, "")
        }
        common = {
            "dataset_id": dataset_id,
            "title": (raw.get(title_heading) or "").strip()
            if title_heading
            else None,
            "inventory_fields": inventory_fields,
        }
        if inventory_type == "data":
            direct_url = (raw.get(tsv_heading) or "").strip()
            if direct_url:
                tsv_url, query_keys = _validated_eurostat_data_url(
                    direct_url, dataset_id
                )
                route_source = "inventory"
                request_params = {}
                if "format" not in query_keys:
                    request_params["format"] = "TSV"
                if "compressed" not in query_keys:
                    request_params["compressed"] = "true"
            else:
                template = (
                    EUROSTAT_COMEXT_DATA_URL
                    if dataset_id.upper().startswith("DS-")
                    else EUROSTAT_DATA_URL
                )
                tsv_url = template.format(dataset=dataset_id)
                route_source = "official_template"
                request_params = {"format": "TSV", "compressed": "true"}
            structure_url = (raw.get(structure_heading) or "").strip()
            if not structure_url:
                raise FullSourceAdapterError(
                    f"Eurostat dataset {dataset_id!r} has no structure URL"
                )
            structure_url = _validated_eurostat_structure_url(
                structure_url, dataset_id
            )
            common.update(
                {
                    "last_data_change": (raw.get(version_heading) or "").strip()
                    if version_heading
                    else None,
                    "last_structural_change": (
                        raw.get(structural_heading) or ""
                    ).strip()
                    if structural_heading
                    else None,
                    "tsv_url": tsv_url,
                    "request_params": request_params,
                    "structure_url": structure_url or None,
                    "route_source": route_source,
                }
            )
        elif inventory_type == "codelist":
            direct_url = (raw.get(tsv_heading) or "").strip()
            if not direct_url:
                raise FullSourceAdapterError(
                    f"Eurostat codelist {dataset_id!r} has no versioned TSV URL"
                )
            common.update(
                {
                    "version": (raw.get(version_heading) or "").strip()
                    if version_heading
                    else None,
                    "tsv_url": _validated_eurostat_codelist_url(
                        direct_url, dataset_id
                    ),
                }
            )
        else:
            package_url = (raw.get(package_heading) or "").strip()
            if not package_url:
                raise FullSourceAdapterError(
                    f"Eurostat metadata item {dataset_id!r} has no package URL"
                )
            common.update(
                {
                    "last_metadata_change": (raw.get(version_heading) or "").strip()
                    if version_heading
                    else None,
                    "package_url": _validated_eurostat_metadata_url(
                        package_url, dataset_id
                    ),
                }
            )
        rows.append(common)

    if not rows:
        raise FullSourceAdapterError("Eurostat inventory contains no datasets")
    return rows


def inspect_distribution(path: str | os.PathLike[str], kind: str) -> dict[str, Any]:
    """Inspect and validate one exact local distribution using bounded memory."""
    file_path = Path(path)
    if not isinstance(kind, str) or not kind:
        raise FullSourceAdapterError("distribution kind must be a non-empty string")
    if kind not in _KNOWN_KINDS:
        return {
            "status": "unsupported",
            "kind": kind,
            "reason": "no local inspector is registered for this distribution kind",
        }
    if not file_path.is_file():
        raise FullSourceAdapterError(f"distribution path is not a regular file: {file_path}")

    common = {
        "kind": kind,
        "byte_size": file_path.stat().st_size,
        "sha256": _file_sha256(file_path),
    }
    if kind == "wdi_zip":
        return {**common, **_inspect_wdi_zip(file_path)}
    if kind.startswith("eurostat_inventory"):
        inventory_type = _inventory_type(kind)
        body = file_path.read_bytes()
        rows = parse_catalogue(EUROSTAT_SOURCE_ID, body, inventory_type)
        return {
            **common,
            "status": "complete",
            "dataset_count": len(rows),
            "columns": list(rows[0]["inventory_fields"]),
            "dataset_ids": [row["dataset_id"] for row in rows],
            "versions": {
                row["dataset_id"]: _row_version(row, inventory_type) for row in rows
            },
        }
    if kind == "eurostat_tsv_gzip":
        return {**common, **_inspect_eurostat_tsv_gzip(file_path)}
    if kind == "eurostat_codelist_tsv":
        return {**common, **_inspect_plain_tsv(file_path)}
    if kind == "eurostat_structure_sdmx":
        return {**common, **_inspect_xml_stream(file_path)}
    return {**common, **_inspect_metadata_zip(file_path)}


def parse_eurostat_async(
    body: bytes | str, initial_request: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Parse an official asynchronous envelope into safe follow-up descriptors.

    The original request descriptor is copied into the result so durable runners can
    retain the exact request that led to asynchronous processing.  Follow-up URLs are
    constructed locally from a strictly validated provider request identifier; response
    URLs are never followed.
    """
    text = _decode_text(body, "Eurostat asynchronous envelope")
    lowered = text.lower()
    if "<!doctype" in lowered or "<!entity" in lowered:
        raise FullSourceAdapterError(
            "Eurostat asynchronous XML cannot contain DTD/entity declarations"
        )
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise FullSourceAdapterError("invalid Eurostat asynchronous XML envelope") from exc
    root_name = _local_name(root.tag).lower()
    fault = root if root_name == "fault" else next(
        (
            element
            for element in root.iter()
            if _local_name(element.tag).lower() == "fault"
        ),
        None,
    )
    if fault is not None:
        result = _parse_eurostat_fault(fault)
        if initial_request is not None:
            if not isinstance(initial_request, Mapping):
                raise FullSourceAdapterError("initial_request must be a mapping")
            result["initial_request"] = deepcopy(dict(initial_request))
        return result
    if root_name != "envelope":
        raise FullSourceAdapterError(
            "Eurostat asynchronous response is not an Envelope or Fault"
        )

    request_ids = [
        (element.text or "").strip()
        for element in root.iter()
        if _local_name(element.tag).lower() in {"id", "key"}
        and len(element) == 0
        and (element.text or "").strip()
    ]
    statuses = [
        (element.text or "").strip().upper()
        for element in root.iter()
        if _local_name(element.tag).lower() == "status"
        and len(element) == 0
        and (element.text or "").strip()
    ]
    if len(request_ids) != 1 or not _ASYNC_ID_RE.fullmatch(request_ids[0]):
        raise FullSourceAdapterError(
            "Eurostat asynchronous envelope has no single safe request id"
        )
    allowed_statuses = {
        "SUBMITTED",
        "PROCESSING",
        "AVAILABLE",
        "EXPIRED",
        "UNKNOWN_REQUEST",
        "ERROR",
    }
    if len(statuses) != 1 or statuses[0] not in allowed_statuses:
        raise FullSourceAdapterError(
            "Eurostat asynchronous envelope has no single valid status"
        )
    request_id = request_ids[0]
    state = {
        "SUBMITTED": "async",
        "PROCESSING": "async",
        "AVAILABLE": "available",
        "EXPIRED": "expired",
        "UNKNOWN_REQUEST": "async_error",
        "ERROR": "async_error",
    }[statuses[0]]
    result: dict[str, Any] = {
        "status": state,
        "provider_status": statuses[0],
        "request_id": request_id,
        "poll": {
            "url": EUROSTAT_ASYNC_STATUS_URL.format(request_id=request_id),
            "params": {},
        },
        "ready": {
            "url": EUROSTAT_ASYNC_DATA_URL.format(request_id=request_id),
            "params": {},
        },
    }
    if initial_request is not None:
        if not isinstance(initial_request, Mapping):
            raise FullSourceAdapterError("initial_request must be a mapping")
        result["initial_request"] = deepcopy(dict(initial_request))
    return result


def _parse_eurostat_fault(fault: ET.Element) -> dict[str, Any]:
    codes = [
        (element.text or "").strip()
        for element in fault.iter()
        if _local_name(element.tag).lower() == "faultcode"
        and len(element) == 0
        and (element.text or "").strip()
    ]
    messages = [
        (element.text or "").strip()
        for element in fault.iter()
        if _local_name(element.tag).lower() == "faultstring"
        and len(element) == 0
        and (element.text or "").strip()
    ]
    if len(codes) != 1 or len(messages) != 1 or not codes[0].isdigit():
        raise FullSourceAdapterError(
            "Eurostat asynchronous fault has no single code and message"
        )
    message = messages[0]
    fault_type = message.split(":", 1)[0].strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", fault_type):
        raise FullSourceAdapterError("Eurostat asynchronous fault has an invalid type")
    common = {
        "fault_code": codes[0],
        "fault_type": fault_type,
        "fault_message": message,
    }
    if codes[0] == "413" and fault_type == "EXTRACTION_TOO_BIG":
        return {
            "status": "unsupported",
            "http_status": 413,
            "reason": "full dataset requires an adaptive partition plan",
            **common,
        }
    if codes[0] == "100" and fault_type == "NO_RESULTS":
        return {"status": "no_results", **common}
    if codes[0] == "100" and fault_type == "DATA_NOT_YET_AVAILABLE":
        return {
            "status": "async",
            "provider_status": "DATA_NOT_YET_AVAILABLE",
            **common,
        }
    if codes[0] == "100" and fault_type == "UNKNOWN_REQUEST":
        return {
            "status": "async_error",
            "provider_status": "UNKNOWN_REQUEST",
            **common,
        }
    return {"status": "unsupported", **common}


def _inspect_wdi_zip(path: Path) -> dict[str, Any]:
    try:
        archive = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as exc:
        raise FullSourceAdapterError(f"invalid WDI ZIP: {exc}") from exc

    members: list[dict[str, Any]] = []
    years: set[str] = set()
    try:
        infos = archive.infolist()
        file_infos = [info for info in infos if not info.is_dir()]
        if not file_infos:
            raise FullSourceAdapterError("WDI ZIP contains no files")
        for info in infos:
            _validate_zip_member(info)
            if info.is_dir():
                continue
            member: dict[str, Any] = {
                "name": info.filename,
                "byte_size": info.file_size,
                "compressed_size": info.compress_size,
                "crc32": f"{info.CRC:08x}",
            }
            try:
                if info.filename.lower().endswith(".csv"):
                    csv_metadata = _inspect_zip_csv(archive, info)
                    member.update(csv_metadata)
                    years.update(csv_metadata["year_columns"])
                else:
                    _consume_binary(archive.open(info, "r"))
            except (OSError, EOFError, RuntimeError, UnicodeError, zipfile.BadZipFile) as exc:
                raise FullSourceAdapterError(
                    f"invalid WDI ZIP member {info.filename!r}: {exc}"
                ) from exc
            members.append(member)
    finally:
        archive.close()

    data_members = [
        member
        for member in members
        if {"Country Name", "Country Code", "Indicator Name", "Indicator Code"}
        <= set(member.get("columns", []))
        and member.get("year_columns")
        and member.get("row_count", 0) > 0
    ]
    country_metadata = [
        member
        for member in members
        if "Country Code" in member.get("columns", [])
        and "Country Name" not in member.get("columns", [])
    ]
    series_metadata = [
        member for member in members if "Series Code" in member.get("columns", [])
    ]
    if not data_members or not country_metadata or not series_metadata:
        raise FullSourceAdapterError(
            "ZIP is not a complete WDI bundle with data, country, and series metadata"
        )

    ordered_years = sorted(years)
    return {
        "status": "complete",
        "archive_format": "zip",
        "member_count": len(members),
        "members": members,
        "year_columns": ordered_years,
        "year_range": [ordered_years[0], ordered_years[-1]] if ordered_years else None,
    }


def _inspect_metadata_zip(path: Path) -> dict[str, Any]:
    try:
        archive = zipfile.ZipFile(path)
    except (OSError, zipfile.BadZipFile) as exc:
        raise FullSourceAdapterError(f"invalid Eurostat metadata ZIP: {exc}") from exc
    members: list[dict[str, Any]] = []
    try:
        infos = archive.infolist()
        if not any(not info.is_dir() for info in infos):
            raise FullSourceAdapterError("Eurostat metadata ZIP contains no files")
        for info in infos:
            _validate_zip_member(info)
            if info.is_dir():
                continue
            member: dict[str, Any] = {
                "name": info.filename,
                "byte_size": info.file_size,
                "compressed_size": info.compress_size,
                "crc32": f"{info.CRC:08x}",
            }
            try:
                if info.filename.lower().endswith(".xml"):
                    with archive.open(info, "r") as stream:
                        member.update(_parse_xml_stream(stream))
                else:
                    _consume_binary(archive.open(info, "r"))
            except (OSError, EOFError, RuntimeError, UnicodeError, zipfile.BadZipFile, ET.ParseError) as exc:
                raise FullSourceAdapterError(
                    f"invalid Eurostat metadata ZIP member {info.filename!r}: {exc}"
                ) from exc
            members.append(member)
    finally:
        archive.close()
    return {
        "status": "complete",
        "archive_format": "zip",
        "member_count": len(members),
        "members": members,
    }


def _inspect_plain_tsv(path: Path) -> dict[str, Any]:
    try:
        with path.open("rt", encoding="utf-8-sig", errors="strict", newline="") as stream:
            reader = csv.reader(stream, delimiter="\t")
            try:
                columns = next(reader)
            except StopIteration:
                raise FullSourceAdapterError("Eurostat codelist TSV is empty") from None
            if len(columns) < 2:
                raise FullSourceAdapterError("Eurostat codelist TSV has no label columns")
            row_count = 0
            for line_number, row in enumerate(reader, start=2):
                if len(row) != len(columns):
                    raise FullSourceAdapterError(
                        f"Eurostat codelist TSV row {line_number} has {len(row)} fields; "
                        f"expected {len(columns)}"
                    )
                row_count += 1
    except FullSourceAdapterError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise FullSourceAdapterError(f"invalid Eurostat codelist TSV: {exc}") from exc
    return {
        "status": "complete",
        "archive_format": None,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
    }


def _inspect_xml_stream(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as stream:
            metadata = _parse_xml_stream(stream)
    except (OSError, ET.ParseError) as exc:
        raise FullSourceAdapterError(f"invalid Eurostat SDMX XML: {exc}") from exc
    return {"status": "complete", "archive_format": None, **metadata}


def _parse_xml_stream(stream: BinaryIO) -> dict[str, Any]:
    element_count = 0
    root_name: str | None = None
    names: set[str] = set()
    safe_stream = _DtdRejectingReader(stream)
    for event, element in ET.iterparse(safe_stream, events=("start", "end")):
        name = _local_name(element.tag)
        if event == "start":
            if root_name is None:
                root_name = name
            names.add(name)
        else:
            element_count += 1
            element.clear()
    if root_name is None:
        raise ET.ParseError("XML document is empty")
    return {
        "xml_root": root_name,
        "xml_element_count": element_count,
        "xml_element_names": sorted(names),
    }


class _DtdRejectingReader:
    """File-like streaming guard against entity declarations in provider XML."""

    def __init__(self, stream: BinaryIO):
        self._stream = stream
        self._tail = b""

    def read(self, size: int = -1) -> bytes:
        chunk = self._stream.read(size)
        scanned = (self._tail + chunk).lower()
        if b"<!doctype" in scanned or b"<!entity" in scanned:
            raise FullSourceAdapterError("XML DTD/entity declarations are not allowed")
        self._tail = scanned[-16:]
        return chunk


def _inspect_zip_csv(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> dict[str, Any]:
    with archive.open(info, "r") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="strict", newline="")
        reader = csv.reader(text)
        try:
            columns = next(reader)
        except StopIteration:
            columns = []
            row_count = 0
        else:
            row_count = sum(1 for _ in reader)
        # TextIOWrapper may buffer through EOF; an explicit read guarantees that the
        # ZipExtFile CRC check is reached even for unusual CSV parser behavior.
        text.read()
    year_columns = [column.strip() for column in columns if _is_year(column)]
    return {
        "row_count": row_count,
        "columns": columns,
        "year_columns": year_columns,
    }


def _inspect_eurostat_tsv_gzip(path: Path) -> dict[str, Any]:
    with path.open("rb") as source:
        magic = source.read(2)
    if magic != b"\x1f\x8b":
        body = _read_prefix(path, 64 * 1024 + 1)
        if len(body) > 64 * 1024 and _is_async_payload(body):
            raise FullSourceAdapterError(
                "Eurostat asynchronous envelope exceeds the 64 KiB control limit"
            )
        if _is_async_payload(body):
            return {**parse_eurostat_async(body), "archive_format": None}
        blocker = _provider_blocker(body)
        if blocker is not None:
            return {**blocker, "archive_format": None}
        raise FullSourceAdapterError("Eurostat compressed TSV is not a GZIP stream")

    try:
        with gzip.open(path, "rb") as binary_stream:
            prefix = binary_stream.read(4096)
        if _is_async_payload(prefix):
            with gzip.open(path, "rb") as binary_stream:
                return {
                    **parse_eurostat_async(binary_stream.read()),
                    "archive_format": "gzip",
                }
        blocker = _provider_blocker(prefix)
        if blocker is not None:
            # Consume the complete stream to verify its GZIP checksum before reporting
            # the provider's explicit full-dataset blocker.
            with gzip.open(path, "rb") as binary_stream:
                body = binary_stream.read()
            return {**(_provider_blocker(body) or blocker), "archive_format": "gzip"}
        with gzip.open(path, "rt", encoding="utf-8-sig", errors="strict", newline="") as stream:
            reader = csv.reader(stream, delimiter="\t")
            try:
                columns = next(reader)
            except StopIteration:
                raise FullSourceAdapterError("Eurostat TSV is empty") from None
            if not columns or "\\TIME_PERIOD" not in columns[0].upper():
                raise FullSourceAdapterError(
                    "Eurostat TSV header has no series key/TIME_PERIOD column"
                )

            row_count = 0
            empty_count = 0
            null_count = 0
            flagged_count = 0
            for line_number, row in enumerate(reader, start=2):
                if len(row) != len(columns):
                    raise FullSourceAdapterError(
                        f"Eurostat TSV row {line_number} has {len(row)} fields; "
                        f"expected {len(columns)}"
                    )
                row_count += 1
                for cell in row[1:]:
                    value = cell.strip()
                    if not value:
                        empty_count += 1
                        continue
                    value_part, separator, flag_part = value.partition(" ")
                    if value_part == ":":
                        null_count += 1
                    if separator and flag_part.strip():
                        flagged_count += 1
    except FullSourceAdapterError:
        raise
    except (OSError, EOFError, UnicodeError) as exc:
        raise FullSourceAdapterError(f"invalid Eurostat GZIP TSV: {exc}") from exc

    periods = [column.strip() for column in columns[1:]]
    years = sorted({period[:4] for period in periods if _is_year(period)})
    return {
        "status": "complete",
        "archive_format": "gzip",
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "series_dimensions": columns[0].split("\\", 1)[0].split(","),
        "periods": periods,
        "year_columns": years,
        "year_range": [years[0], years[-1]] if years else None,
        "empty_cell_count": empty_count,
        "null_cell_count": null_count,
        "flagged_cell_count": flagged_count,
    }


def _validated_eurostat_data_url(url: str, dataset_id: str) -> tuple[str, set[str]]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "ec.europa.eu"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe TSV URL for {dataset_id!r}"
        )
    match = _EUROSTAT_PATH_RE.fullmatch(parsed.path)
    if match is None or match.group(1).upper() != dataset_id.upper():
        raise FullSourceAdapterError(
            f"Eurostat inventory TSV URL does not match dataset {dataset_id!r}"
        )
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if len({key.lower() for key, _ in query}) != len(query):
        raise FullSourceAdapterError(
            f"Eurostat inventory has duplicate TSV URL parameters for {dataset_id!r}"
        )
    query_keys: set[str] = set()
    for key, value in query:
        normalized = key.lower()
        if normalized not in _EUROSTAT_ALLOWED_QUERY_KEYS:
            raise FullSourceAdapterError(
                f"Eurostat inventory has an unsafe TSV URL parameter {key!r}"
            )
        if normalized == "format" and value.upper() != "TSV":
            raise FullSourceAdapterError(
                f"Eurostat inventory TSV URL has non-TSV format for {dataset_id!r}"
            )
        query_keys.add(normalized)
        if normalized == "compressed" and value.lower() != "true":
            raise FullSourceAdapterError(
                f"Eurostat inventory TSV URL disables compression for {dataset_id!r}"
            )
    return url, query_keys


def _validated_eurostat_structure_url(url: str, dataset_id: str) -> str:
    parsed = _official_eurostat_url(url, f"structure URL for {dataset_id!r}")
    pattern = re.compile(
        r"^/eurostat/api/dissemination/sdmx/2\.1/dataflow/ESTAT/([^/]+)/?$",
        re.IGNORECASE,
    )
    match = pattern.fullmatch(parsed.path)
    if match is None or match.group(1).upper() != dataset_id.upper():
        raise FullSourceAdapterError(
            f"Eurostat inventory structure URL does not match dataset {dataset_id!r}"
        )
    query = parse_qsl(parsed.query, keep_blank_values=True)
    allowed = {"references", "format"}
    if any(key.lower() not in allowed for key, _ in query):
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe structure URL for {dataset_id!r}"
        )
    return url


def _validated_eurostat_codelist_url(url: str, dataset_id: str) -> str:
    parsed = _official_eurostat_url(url, f"codelist URL for {dataset_id!r}")
    pattern = re.compile(
        r"^/eurostat/api/dissemination/sdmx/3\.0/structure/codelist/ESTAT/"
        r"([^/]+)/([^/]+)$",
        re.IGNORECASE,
    )
    match = pattern.fullmatch(parsed.path)
    if match is None or match.group(1).upper() != dataset_id.upper():
        raise FullSourceAdapterError(
            f"Eurostat inventory codelist URL does not match code {dataset_id!r}"
        )
    query = parse_qsl(parsed.query, keep_blank_values=True)
    allowed = {"format", "formatversion"}
    if any(key.lower() not in allowed for key, _ in query):
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe codelist URL for {dataset_id!r}"
        )
    formats = [value.upper() for key, value in query if key.lower() == "format"]
    if formats != ["TSV"]:
        raise FullSourceAdapterError(
            f"Eurostat inventory codelist URL is not TSV for {dataset_id!r}"
        )
    return url


def _validated_eurostat_metadata_url(url: str, dataset_id: str) -> str:
    parsed = _official_eurostat_url(url, f"metadata URL for {dataset_id!r}")
    if parsed.path != "/eurostat/api/dissemination/files":
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe metadata URL for {dataset_id!r}"
        )
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if len(query) != 1 or query[0][0].lower() != "file":
        raise FullSourceAdapterError(
            f"Eurostat inventory has an unsafe metadata URL for {dataset_id!r}"
        )
    expected = f"metadata/{dataset_id}.sdmx.zip".lower()
    if query[0][1].lower() != expected:
        raise FullSourceAdapterError(
            f"Eurostat inventory metadata URL does not match code {dataset_id!r}"
        )
    return url


def _official_eurostat_url(url: str, label: str):
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "ec.europa.eu"
        or parsed.port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise FullSourceAdapterError(f"Eurostat inventory has an unsafe {label}")
    return parsed


def _validate_zip_member(info: zipfile.ZipInfo) -> None:
    name = info.filename
    posix = PurePosixPath(name.replace("\\", "/"))
    if (
        not name
        or name.startswith(("/", "\\"))
        or re.match(r"^[A-Za-z]:", name)
        or ".." in posix.parts
    ):
        raise FullSourceAdapterError(f"WDI ZIP has unsafe member path {name!r}")
    if info.flag_bits & 0x1:
        raise FullSourceAdapterError(f"WDI ZIP has encrypted member {name!r}")


def _consume_binary(stream: BinaryIO) -> None:
    with stream:
        while stream.read(1024 * 1024):
            pass


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_prefix(path: Path, byte_count: int) -> bytes:
    with path.open("rb") as stream:
        return stream.read(byte_count)


def _inventory_type(inventory_kind: str) -> str:
    if not isinstance(inventory_kind, str):
        raise FullSourceAdapterError("inventory kind must be a string")
    normalized = inventory_kind.strip().lower()
    aliases = {
        "data": "data",
        "eurostat_inventory": "data",
        "codelist": "codelist",
        "eurostat_inventory_codelist": "codelist",
        "metadata": "metadata",
        "eurostat_inventory_metadata": "metadata",
    }
    try:
        return aliases[normalized]
    except KeyError:
        raise FullSourceAdapterError(
            f"unsupported Eurostat inventory kind {inventory_kind!r}"
        ) from None


def _row_version(row: Mapping[str, Any], inventory_type: str) -> Any:
    if inventory_type == "data":
        return row.get("last_data_change")
    if inventory_type == "codelist":
        return row.get("version")
    return row.get("last_metadata_change")


def _source(source_id: str) -> str:
    if not isinstance(source_id, str):
        raise FullSourceAdapterError("source_id must be a string")
    source = _SOURCE_ALIASES.get(source_id.strip().lower())
    if source is None:
        raise FullSourceAdapterError(f"unsupported full-distribution source {source_id!r}")
    return source


def _decode_text(body: bytes | str, label: str) -> str:
    if isinstance(body, str):
        return body.lstrip("\ufeff")
    if not isinstance(body, bytes):
        raise FullSourceAdapterError(f"{label} must be bytes or text")
    try:
        return body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise FullSourceAdapterError(f"{label} is not valid UTF-8") from exc


def _heading(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _validate_dataset_id(dataset_id: str, line_number: int) -> None:
    if not _DATASET_RE.fullmatch(dataset_id):
        raise FullSourceAdapterError(
            f"Eurostat inventory row {line_number} has an invalid dataset code"
        )


def _is_year(value: str) -> bool:
    return _YEAR_RE.match(value.strip()) is not None


def _is_async_payload(body: bytes) -> bool:
    prefix = body.lstrip().lower()
    if prefix.startswith(b"<?xml"):
        declaration_end = prefix.find(b"?>")
        prefix = prefix[declaration_end + 2 :].lstrip() if declaration_end >= 0 else prefix
    return re.match(
        rb"<(?:[a-z_][a-z0-9_.-]*:)?(?:envelope|fault)(?:\s|>)", prefix
    ) is not None


def _provider_blocker(body: bytes) -> dict[str, Any] | None:
    compact = re.sub(rb"\s+", b"", body.lower())
    if b'"status":413' in compact:
        return {
            "status": "unsupported",
            "http_status": 413,
            "reason": "full dataset requires a future adaptive partition plan",
        }
    return None


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].rsplit(":", 1)[-1]


__all__ = [
    "EUROSTAT_DATA_URL",
    "EUROSTAT_INVENTORY_URL",
    "EUROSTAT_SOURCE_ID",
    "FullSourceAdapterError",
    "WDI_REDIRECT_URL",
    "WDI_SOURCE_ID",
    "WDI_ZIP_URL",
    "initial_distributions",
    "distributions_from_inventory",
    "inspect_distribution",
    "parse_eurostat_async",
    "parse_catalogue",
]
