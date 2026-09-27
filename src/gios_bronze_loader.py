"""Landing-to-Bronze Parquet transformation loader for GIOŚ PJP air quality data (PL-ENV-010).

Vectorized DuckDB architecture per ADR 0007 / ADR 0009:
- Reads native Excel metadata and zip measurement archives from Landing (01_landing/gios_pjp/native/bulk/)
- Normalizes station registry into br_gios_stations.parquet
- Normalizes sensor registry into br_gios_sensors.parquet
- Normalizes hourly/daily pollutant measurements into br_gios_measurements.parquet
- Writes typed ZSTD Parquet files
- Uploads to Google Drive 02_bronze/gios_pjp/ idempotently using safe_drive_upload
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import re
import sys
import tempfile
import threading
from typing import Any
import xml.etree.ElementTree as ET
import zipfile

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from drive_safe_upload import safe_drive_upload
from storage_manager import StorageManager

logger = logging.getLogger("gios_bronze_loader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

DRIVE_LOCK = threading.Lock()


def _escape_query(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _resolve_or_create_folder(storage: StorageManager, folder_name: str, parent_id: str) -> str:
    query = (
        f"name='{_escape_query(folder_name)}' and '{_escape_query(parent_id)}' in parents "
        "and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    with DRIVE_LOCK:
        existing = storage.drive_service.files().list(q=query, spaces="drive", fields="files(id,name)").execute().get("files", [])
    if existing:
        return existing[0]["id"]
    with DRIVE_LOCK:
        created = storage.drive_service.files().create(
            body={"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]},
            fields="id,name",
        ).execute(num_retries=4)
    return created["id"]


def col_letter_to_index(col_str: str) -> int:
    """Convert spreadsheet column letter (A, B, ..., Z, AA, etc.) to 0-based index."""
    idx = 0
    for char in col_str:
        if "A" <= char <= "Z":
            idx = idx * 26 + (ord(char) - ord("A") + 1)
    return idx - 1


def read_xlsx_sheet(xlsx_path: Path, sheet_name: str | None = None) -> list[list[str | None]]:
    """Parse a specific sheet from an XLSX file without external dependencies."""
    with zipfile.ZipFile(xlsx_path, "r") as z:
        # 1. Read shared strings if present
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in z.namelist():
            tree = ET.fromstring(z.read("xl/sharedStrings.xml"))
            ns = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
            for si in tree.findall(".//ns:si", ns):
                # An si can have a direct t, or multiple r/t elements
                t_elem = si.find("ns:t", ns)
                if t_elem is not None and t_elem.text:
                    shared_strings.append(t_elem.text)
                else:
                    parts = [r.find("ns:t", ns).text for r in si.findall(".//ns:r", ns) if r.find("ns:t", ns) is not None and r.find("ns:t", ns).text]
                    shared_strings.append("".join(parts))

        # 2. Locate workbook sheets
        wb_tree = ET.fromstring(z.read("xl/workbook.xml"))
        wb_ns = {
            "ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
            "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
        }
        rels_tree = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        rels_ns = {"ns": "http://schemas.openxmlformats.org/package/2006/relationships"}
        rel_map = {r.attrib["Id"]: r.attrib["Target"] for r in rels_tree.findall("ns:Relationship", rels_ns)}

        sheet_target = None
        for sheet in wb_tree.findall(".//ns:sheet", wb_ns):
            s_name = sheet.attrib.get("name")
            r_id = sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
            if sheet_name is None or (s_name and s_name.strip().lower() == sheet_name.strip().lower()):
                sheet_target = rel_map.get(r_id)
                break

        if not sheet_target:
            return []

        if not sheet_target.startswith("xl/"):
            sheet_target = "xl/" + sheet_target

        sheet_tree = ET.fromstring(z.read(sheet_target))
        sheet_ns = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

        rows: list[list[str | None]] = []
        for row_elem in sheet_tree.findall(".//ns:row", sheet_ns):
            row_cells: list[str | None] = []
            for c in row_elem.findall("ns:c", sheet_ns):
                cell_ref = c.attrib.get("r", "")
                m = re.match(r"([A-Z]+)(\d+)", cell_ref)
                if m:
                    target_idx = col_letter_to_index(m.group(1))
                else:
                    target_idx = len(row_cells)

                while len(row_cells) < target_idx:
                    row_cells.append(None)

                t_attr = c.attrib.get("t")
                v = c.find("ns:v", sheet_ns)
                val = v.text if v is not None else None
                if t_attr == "s" and val is not None:
                    idx = int(val)
                    val = shared_strings[idx] if idx < len(shared_strings) else val
                elif t_attr == "inlineStr":
                    is_t = c.find(".//ns:t", sheet_ns)
                    val = is_t.text if is_t is not None else ""

                row_cells.append(val)
            rows.append(row_cells)

        return rows


def transform_gios_metadata(
    excel_path: Path,
    out_stations: Path,
    out_sensors: Path,
) -> tuple[int, int]:
    """Transform GIOŚ metadata Excel sheets into stations and sensors Parquet tables."""
    now_utc = datetime.now(timezone.utc).isoformat()
    con = duckdb.connect(":memory:")

    # 1. Transform Stations (sheet 'Stacje')
    stacje_rows = read_xlsx_sheet(excel_path, "Stacje")
    stations_count = 0
    if stacje_rows:
        headers = [str(h).strip() if h is not None else "" for h in stacje_rows[0]]
        header_map = {h.lower(): i for i, h in enumerate(headers)}

        def get_col(row: list[str | None], *names: str) -> str | None:
            for name in names:
                idx = header_map.get(name.lower())
                if idx is not None and idx < len(row):
                    val = row[idx]
                    if val is not None and str(val).strip():
                        return str(val).strip()
            return None

        station_records = []
        for r in stacje_rows[1:]:
            st_code = get_col(r, "kod stacji", "kod_stacji", "station_code")
            if not st_code:
                continue
            old_code = get_col(r, "stary kod stacji", "stary_kod_stacji", "old_station_code")
            name = get_col(r, "nazwa stacji", "nazwa_stacji", "station_name") or st_code
            st_type = get_col(r, "typ stacji", "typ_stacji", "station_type")
            area_type = get_col(r, "typ obszaru", "typ_obszaru", "area_type")
            lat_str = get_col(r, "wgs84 φ n", "wgs84_phi_n", "szerokosc", "latitude", "lat")
            lon_str = get_col(r, "wgs84 λ e", "wgs84_lambda_e", "dlugosc", "longitude", "lon")
            voiv = get_col(r, "województwo", "wojewodztwo", "voivodeship")
            city = get_col(r, "miejscowość", "miejscowosc", "city")
            addr = get_col(r, "adres", "address")

            lat = float(lat_str.replace(",", ".")) if lat_str else None
            lon = float(lon_str.replace(",", ".")) if lon_str else None

            station_records.append((
                st_code, old_code, name, st_type, area_type,
                lat, lon, voiv, city, addr, now_utc
            ))

        con.execute("""
            CREATE TABLE stations_tbl (
                station_code VARCHAR,
                old_station_code VARCHAR,
                station_name VARCHAR,
                station_type VARCHAR,
                area_type VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                voivodeship VARCHAR,
                city VARCHAR,
                address VARCHAR,
                processed_at_utc TIMESTAMPTZ
            );
        """)
        con.executemany("INSERT INTO stations_tbl VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", station_records)
        con.execute(f"COPY stations_tbl TO '{out_stations}' (FORMAT PARQUET, COMPRESSION ZSTD);")
        stations_count = len(station_records)
    else:
        # Empty schema table
        con.execute(f"""
            COPY (
                SELECT
                    CAST(NULL AS VARCHAR) AS station_code,
                    CAST(NULL AS VARCHAR) AS old_station_code,
                    CAST(NULL AS VARCHAR) AS station_name,
                    CAST(NULL AS VARCHAR) AS station_type,
                    CAST(NULL AS VARCHAR) AS area_type,
                    CAST(NULL AS DOUBLE) AS latitude,
                    CAST(NULL AS DOUBLE) AS longitude,
                    CAST(NULL AS VARCHAR) AS voivodeship,
                    CAST(NULL AS VARCHAR) AS city,
                    CAST(NULL AS VARCHAR) AS address,
                    TIMESTAMPTZ '{now_utc}' AS processed_at_utc
                WHERE 1 = 0
            ) TO '{out_stations}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

    # 2. Transform Sensors (sheet 'Stanowiska')
    stanowiska_rows = read_xlsx_sheet(excel_path, "Stanowiska")
    sensors_count = 0
    if stanowiska_rows:
        headers = [str(h).strip() if h is not None else "" for h in stanowiska_rows[0]]
        header_map = {h.lower(): i for i, h in enumerate(headers)}

        def get_sensor_col(row: list[str | None], *names: str) -> str | None:
            for name in names:
                idx = header_map.get(name.lower())
                if idx is not None and idx < len(row):
                    val = row[idx]
                    if val is not None and str(val).strip():
                        return str(val).strip()
            return None

        sensor_records = []
        for r in stanowiska_rows[1:]:
            sensor_code = get_sensor_col(r, "kod stanowiska", "kod_stanowiska", "sensor_code")
            st_code = get_sensor_col(r, "kod stacji", "kod_stacji", "station_code")
            if not sensor_code or not st_code:
                continue
            pollutant = get_sensor_col(r, "wskaźnik", "wskaznik", "pollutant", "parametr")
            interval = get_sensor_col(r, "czas uśredniania", "czas_usredniania", "averaging_interval")
            meas_type = get_sensor_col(r, "typ pomiaru", "typ_pomiaru", "measurement_type")

            sensor_records.append((
                sensor_code, st_code, pollutant, interval, meas_type, now_utc
            ))

        con.execute("""
            CREATE TABLE sensors_tbl (
                sensor_code VARCHAR,
                station_code VARCHAR,
                pollutant VARCHAR,
                averaging_interval VARCHAR,
                measurement_type VARCHAR,
                processed_at_utc TIMESTAMPTZ
            );
        """)
        con.executemany("INSERT INTO sensors_tbl VALUES (?, ?, ?, ?, ?, ?)", sensor_records)
        con.execute(f"COPY sensors_tbl TO '{out_sensors}' (FORMAT PARQUET, COMPRESSION ZSTD);")
        sensors_count = len(sensor_records)
    else:
        con.execute(f"""
            COPY (
                SELECT
                    CAST(NULL AS VARCHAR) AS sensor_code,
                    CAST(NULL AS VARCHAR) AS station_code,
                    CAST(NULL AS VARCHAR) AS pollutant,
                    CAST(NULL AS VARCHAR) AS averaging_interval,
                    CAST(NULL AS VARCHAR) AS measurement_type,
                    TIMESTAMPTZ '{now_utc}' AS processed_at_utc
                WHERE 1 = 0
            ) TO '{out_sensors}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

    con.close()
    return stations_count, sensors_count


def _parse_measurement_date(date_str: str) -> tuple[str, str] | None:
    """Extract (timestamp_iso, date_iso) from various date/time formats in GIOŚ files."""
    cleaned = date_str.strip()
    # Try YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S"), dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def transform_gios_measurements(
    zip_paths: list[Path],
    out_measurements: Path,
    workspace: Path,
) -> int:
    """Extract and transform GIOŚ measurement archives into typed Parquet table."""
    now_utc = datetime.now(timezone.utc).isoformat()
    con = duckdb.connect(":memory:")

    con.execute("""
        CREATE TABLE meas_tbl (
            station_code VARCHAR,
            sensor_code VARCHAR,
            pollutant VARCHAR,
            averaging_interval VARCHAR,
            measurement_timestamp TIMESTAMP,
            observation_date DATE,
            value DOUBLE,
            unit VARCHAR,
            processed_at_utc TIMESTAMPTZ
        );
    """)

    total_inserted = 0

    with tempfile.TemporaryDirectory(dir=str(workspace)) as extract_dir:
        extract_path = Path(extract_dir)

        for zpath in zip_paths:
            logger.info("Processing GIOŚ measurement archive: %s", zpath.name)
            try:
                with zipfile.ZipFile(zpath, "r") as z:
                    for name in z.namelist():
                        if name.endswith("/") or name.startswith("__MACOSX"):
                            continue
                        extracted_file = z.extract(name, extract_path)
                        p_file = Path(extracted_file)

                        # Parse either CSV or XLSX
                        if p_file.suffix.lower() == ".csv":
                            _load_csv_measurement(con, p_file, now_utc)
                        elif p_file.suffix.lower() == ".xlsx":
                            _load_xlsx_measurement(con, p_file, now_utc)
            except Exception as exc:
                logger.warning("Error reading archive %s: %s", zpath.name, exc)

    total_inserted = con.execute("SELECT COUNT(*) FROM meas_tbl").fetchone()[0]
    con.execute(f"COPY meas_tbl TO '{out_measurements}' (FORMAT PARQUET, COMPRESSION ZSTD);")
    con.close()
    return total_inserted


def _load_csv_measurement(con: duckdb.DuckDBPyConnection, csv_path: Path, now_utc: str) -> None:
    """Parse GIOŚ measurement CSV (tabular or matrix) into temp table."""
    try:
        # Detect delimiter and encoding
        sample_bytes = csv_path.read_bytes()[:4096]
        enc = "utf-8"
        try:
            sample_bytes.decode("utf-8")
        except UnicodeDecodeError:
            enc = "latin2"

        with open(csv_path, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f, delimiter=";" if ";" in f.readline() else ",")
            f.seek(0)
            rows = list(reader)

        if not rows:
            return

        header = [c.strip() for c in rows[0]]
        # Case A: Tabular structure
        # (has station_code/kod_stacji and date/data and value/wartosc)
        header_lower = [h.lower() for h in header]
        if any("kod" in h for h in header_lower) and any("wart" in h or "value" in h for h in header_lower):
            # Direct insert via column mapping
            st_idx = next(i for i, h in enumerate(header_lower) if "stac" in h or "station" in h)
            val_idx = next(i for i, h in enumerate(header_lower) if "wart" in h or "val" in h)
            date_idx = next(i for i, h in enumerate(header_lower) if "data" in h or "date" in h or "czas" in h)
            sens_idx = next((i for i, h in enumerate(header_lower) if "stanowisk" in h or "sensor" in h), None)
            poll_idx = next((i for i, h in enumerate(header_lower) if "wskaznik" in h or "wskaźnik" in h or "pollutant" in h), None)

            records = []
            for r in rows[1:]:
                if len(r) <= max(st_idx, val_idx, date_idx):
                    continue
                parsed_dt = _parse_measurement_date(r[date_idx])
                if not parsed_dt:
                    continue
                try:
                    v_str = r[val_idx].replace(",", ".").strip()
                    val = float(v_str) if v_str else None
                except ValueError:
                    val = None
                if val is None:
                    continue

                st_code = r[st_idx].strip()
                sens_code = r[sens_idx].strip() if sens_idx is not None and sens_idx < len(r) else st_code
                poll = r[poll_idx].strip() if poll_idx is not None and poll_idx < len(r) else "PM10"
                records.append((
                    st_code, sens_code, poll, "1g", parsed_dt[0], parsed_dt[1], val, "ug/m3", now_utc
                ))

            if records:
                con.executemany("INSERT INTO meas_tbl VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", records)

        else:
            # Case B: Matrix structure (Column 0 = date/time, Column 1..N = sensor/station codes)
            # Infer pollutant from filename if possible (e.g. 2023_PM10_1g.csv)
            pollutant = "PM10"
            m = re.search(r"(PM10|PM2\.5|PM25|NO2|SO2|CO|O3|C6H6)", csv_path.stem, re.IGNORECASE)
            if m:
                pollutant = m.group(1).upper().replace("PM25", "PM2.5")

            interval = "24g" if "24g" in csv_path.stem.lower() else "1g"
            columns = header[1:]
            records = []
            for r in rows[1:]:
                if not r or len(r) < 2:
                    continue
                parsed_dt = _parse_measurement_date(r[0])
                if not parsed_dt:
                    continue
                for col_idx, col_name in enumerate(columns, start=1):
                    if col_idx >= len(r):
                        break
                    v_str = r[col_idx].replace(",", ".").strip()
                    if not v_str:
                        continue
                    try:
                        val = float(v_str)
                    except ValueError:
                        continue
                    code = col_name.strip()
                    records.append((
                        code, code, pollutant, interval, parsed_dt[0], parsed_dt[1], val, "ug/m3", now_utc
                    ))

            if records:
                con.executemany("INSERT INTO meas_tbl VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", records)

    except Exception as exc:
        logger.warning("Could not parse CSV %s: %s", csv_path.name, exc)


def _load_xlsx_measurement(con: duckdb.DuckDBPyConnection, xlsx_path: Path, now_utc: str) -> None:
    """Parse GIOŚ measurement XLSX into temp table."""
    try:
        rows = read_xlsx_sheet(xlsx_path)
        if not rows or len(rows) < 2:
            return

        header = [str(c).strip() if c is not None else "" for c in rows[0]]
        pollutant = "PM10"
        m = re.search(r"(PM10|PM2\.5|PM25|NO2|SO2|CO|O3|C6H6)", xlsx_path.stem, re.IGNORECASE)
        if m:
            pollutant = m.group(1).upper().replace("PM25", "PM2.5")

        interval = "24g" if "24g" in xlsx_path.stem.lower() else "1g"
        columns = header[1:]
        records = []
        for r in rows[1:]:
            if not r or len(r) < 2 or not r[0]:
                continue
            parsed_dt = _parse_measurement_date(str(r[0]))
            if not parsed_dt:
                continue
            for col_idx, col_name in enumerate(columns, start=1):
                if col_idx >= len(r) or r[col_idx] is None:
                    continue
                v_str = str(r[col_idx]).replace(",", ".").strip()
                if not v_str:
                    continue
                try:
                    val = float(v_str)
                except ValueError:
                    continue
                code = col_name.strip()
                records.append((
                    code, code, pollutant, interval, parsed_dt[0], parsed_dt[1], val, "ug/m3", now_utc
                ))

        if records:
            con.executemany("INSERT INTO meas_tbl VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", records)

    except Exception as exc:
        logger.warning("Could not parse XLSX measurement %s: %s", xlsx_path.name, exc)


def transform_gios_bronze(
    landing_workspace: Path,
    output_workspace: Path,
    storage: StorageManager | None = None,
    allow_codespace: bool = False,
    skip_upload: bool = False,
) -> dict[str, Any]:
    """Execute complete Landing-to-Bronze transformation for GIOŚ data."""
    output_workspace.mkdir(parents=True, exist_ok=True)

    # 1. Stations & Sensors metadata
    metadata_xlsx = landing_workspace / "Metadane oraz kody stacji i stanowisk pomiarowych.xlsx"
    if not metadata_xlsx.exists():
        candidates = list(landing_workspace.rglob("*.xlsx"))
        if candidates:
            metadata_xlsx = candidates[0]

    stations_parquet = output_workspace / "br_gios_stations.parquet"
    sensors_parquet = output_workspace / "br_gios_sensors.parquet"

    if metadata_xlsx.exists():
        logger.info("Transforming GIOŚ metadata from %s...", metadata_xlsx.name)
        stations_count, sensors_count = transform_gios_metadata(
            metadata_xlsx, stations_parquet, sensors_parquet
        )
    else:
        logger.warning("Metadata Excel file not found in %s; emitting empty schemas.", landing_workspace)
        con = duckdb.connect(":memory:")
        now_utc = datetime.now(timezone.utc).isoformat()
        con.execute(f"""
            COPY (SELECT CAST(NULL AS VARCHAR) AS station_code, CAST(NULL AS VARCHAR) AS old_station_code,
                         CAST(NULL AS VARCHAR) AS station_name, CAST(NULL AS VARCHAR) AS station_type,
                         CAST(NULL AS VARCHAR) AS area_type, CAST(NULL AS DOUBLE) AS latitude,
                         CAST(NULL AS DOUBLE) AS longitude, CAST(NULL AS VARCHAR) AS voivodeship,
                         CAST(NULL AS VARCHAR) AS city, CAST(NULL AS VARCHAR) AS address,
                         TIMESTAMPTZ '{now_utc}' AS processed_at_utc WHERE 1 = 0)
            TO '{stations_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        con.execute(f"""
            COPY (SELECT CAST(NULL AS VARCHAR) AS sensor_code, CAST(NULL AS VARCHAR) AS station_code,
                         CAST(NULL AS VARCHAR) AS pollutant, CAST(NULL AS VARCHAR) AS averaging_interval,
                         CAST(NULL AS VARCHAR) AS measurement_type,
                         TIMESTAMPTZ '{now_utc}' AS processed_at_utc WHERE 1 = 0)
            TO '{sensors_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        con.close()
        stations_count = 0
        sensors_count = 0

    # 2. Measurement archives
    zip_archives = list(landing_workspace.rglob("*.zip"))
    # Exclude non-measurement zips if any
    meas_zips = [z for z in zip_archives if "statystyki" not in z.name.lower()]
    measurements_parquet = output_workspace / "br_gios_measurements.parquet"

    logger.info("Transforming %d measurement archives...", len(meas_zips))
    meas_count = transform_gios_measurements(meas_zips, measurements_parquet, output_workspace)

    summary_data = {
        "source_id": "gios_pjp",
        "stations_count": stations_count,
        "sensors_count": sensors_count,
        "measurements_count": meas_count,
        "status": "completed",
        "transformed_at_utc": datetime.now(timezone.utc).isoformat(),
        "tables": {
            "br_gios_stations.parquet": stations_count,
            "br_gios_sensors.parquet": sensors_count,
            "br_gios_measurements.parquet": meas_count,
        },
    }

    if skip_upload:
        summary_path = output_workspace / "summary.json"
        summary_path.write_text(json.dumps(summary_data, indent=2), encoding="utf-8")
        return {**summary_data, "status": "transformed_locally"}

    if storage is None:
        storage = StorageManager()

    in_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    if not in_actions and not allow_codespace:
        raise PermissionError("Production Drive upload requires --allow-codespace or GITHUB_ACTIONS=true.")

    bronze_root = storage.resolve_zone("bronze", create=True)
    gios_bronze = _resolve_or_create_folder(storage, "gios_pjp", bronze_root)

    uploaded_files = []
    for pq_path in (stations_parquet, sensors_parquet, measurements_parquet):
        res = safe_drive_upload(
            storage,
            pq_path,
            pq_path.name,
            gios_bronze,
            mime_type="application/octet-stream",
            drive_lock=DRIVE_LOCK,
        )
        uploaded_files.append({"file_name": pq_path.name, "drive_id": res["id"], "size": pq_path.stat().st_size})

    control_root = storage.resolve_zone("control", create=True)
    campaign_control = _resolve_or_create_folder(storage, "source_campaigns", control_root)
    gios_control = _resolve_or_create_folder(storage, "gios_pjp", campaign_control)

    checkpoint_data = {
        **summary_data,
        "status": "published_to_drive",
        "drive_files": uploaded_files,
    }
    cp_path = output_workspace / "checkpoint.json"
    cp_path.write_text(json.dumps(checkpoint_data, indent=2), encoding="utf-8")
    safe_drive_upload(
        storage,
        cp_path,
        "checkpoint.json",
        gios_control,
        mime_type="application/json",
        drive_lock=DRIVE_LOCK,
    )

    logger.info("GIOŚ Bronze transformation complete and checkpoint recorded on Drive.")
    return checkpoint_data


def main():
    parser = argparse.ArgumentParser(description="GIOŚ PJP Landing-to-Bronze Transformer")
    parser.add_argument("--landing-workspace", type=str, default="portal/test-results/gios-bulk")
    parser.add_argument("--output-workspace", type=str, default="portal/test-results/gios-bronze")
    parser.add_argument("--allow-codespace", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    args = parser.parse_args()

    res = transform_gios_bronze(
        landing_workspace=Path(args.landing_workspace).resolve(),
        output_workspace=Path(args.output_workspace).resolve(),
        allow_codespace=args.allow_codespace,
        skip_upload=args.skip_upload,
    )
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
