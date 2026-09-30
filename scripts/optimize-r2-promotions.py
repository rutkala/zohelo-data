#!/usr/bin/env python3
"""Convert v1 physical pass-through promotions to zero-copy Iceberg references.

Only tables created by promote-current-r2.py are touched. Existing curated
BDL/Eurostat/NBP/WDI Silver/Gold tables are not part of this mapping.

For each promoted table:
- verify source/target file counts, row counts, and file sizes match;
- atomically replace target copied data-file references with source file refs;
- verify R2 SQL reads still work;
- delete only the now-unreferenced copied target objects.

Future real transformations may replace these zero-copy tables with independently
materialized files when row-level cleaning/conformance changes the data.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import time
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.typedef import Record
import requests

SILVER = {
 "biala_lista_header":"biala_lista_header",
 "biala_lista_masks":"biala_lista_masks",
 "biala_lista_taxpayers":"biala_lista_taxpayers",
 "dbw_dictionaries":"dbw_dictionaries",
 "dbw_indicators":"dbw_indicators",
 "dbw_metadata":"dbw_metadata",
 "dbw_observations":"dbw_observations",
 "gleif_lei2":"gleif_lei2",
 "gleif_relationship_records":"gleif_relationship_records",
 "gleif_reporting_exceptions":"gleif_reporting_exceptions",
 "opendata_org_locations":"opendata_org_locations",
 "opendata_org_organizations":"opendata_org_organizations",
 "opendata_org_people":"opendata_org_people",
 "prg_counties":"prg_counties",
 "prg_country":"prg_country",
 "prg_municipalities":"prg_municipalities",
 "prg_voivodeships":"prg_voivodeships",
 "teryt_simc":"teryt_simc",
 "teryt_terc":"teryt_terc",
 "teryt_ulic":"teryt_ulic",
}
GOLD = {
 "biala_lista_header":"dim_vat_header",
 "biala_lista_masks":"dim_vat_masks",
 "biala_lista_taxpayers":"dim_vat_taxpayer_account",
 "dbw_dictionaries":"dim_dbw_dictionary",
 "dbw_indicators":"dim_dbw_indicator",
 "dbw_metadata":"dim_dbw_metadata",
 "dbw_observations":"fact_dbw_observations",
 "gleif_lei2":"dim_corporate_entity",
 "gleif_relationship_records":"fact_corporate_relationships",
 "gleif_reporting_exceptions":"fact_corporate_reporting_exceptions",
 "opendata_org_locations":"dim_opendata_location",
 "opendata_org_organizations":"dim_opendata_organization",
 "opendata_org_people":"dim_opendata_person",
 "prg_counties":"dim_prg_county",
 "prg_country":"dim_prg_country",
 "prg_municipalities":"dim_prg_municipality",
 "prg_voivodeships":"dim_prg_voivodeship",
 "teryt_simc":"dim_locality",
 "teryt_terc":"dim_territory",
 "teryt_ulic":"dim_street",
}


def req(name: str) -> str:
    value=os.environ.get(name,"").strip()
    if not value: raise RuntimeError(f"missing {name}")
    return value


def catalog():
    return RestCatalog(
        name="zohelo", warehouse=req("R2_WAREHOUSE"), uri=req("R2_CATALOG_URI"),
        token=req("R2_DATA_CATALOG_TOKEN"),
        **{
          "s3.access-key-id":req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
          "s3.secret-access-key":req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
          "s3.endpoint":req("R2_S3_ENDPOINT"), "s3.region":"auto",
        })


def s3():
    return boto3.client(
        "s3", endpoint_url=req("R2_S3_ENDPOINT"), region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4", retries={"max_attempts":10,"mode":"standard"}),
    )


def parse(uri: str):
    p=urlsplit(uri)
    if p.scheme!="s3": raise RuntimeError(f"unexpected uri {uri}")
    return p.netloc,p.path.lstrip("/")


def clone_ref(table, source_file):
    return DataFile.from_args(
      _table_format_version=int(table.metadata.format_version),
      content=DataFileContent.DATA,
      file_path=str(source_file.file_path),
      file_format=FileFormat.PARQUET,
      partition=Record(),
      record_count=int(source_file.record_count),
      file_size_in_bytes=int(source_file.file_size_in_bytes),
      spec_id=int(table.metadata.default_spec_id))


def sql(query: str):
    url=(f"https://api.sql.cloudflarestorage.com/api/v1/accounts/"
         f"{req('CLOUDFLARE_ACCOUNT_ID')}/r2-sql/query/{req('R2_LAKEHOUSE_BUCKET')}")
    last=None
    for attempt in range(1,6):
        try:
            r=requests.post(url,headers={
                "Authorization":f"Bearer {req('R2_DATA_CATALOG_TOKEN')}",
                "Content-Type":"application/json"},json={"query":query},timeout=120)
            if r.status_code!=200: raise RuntimeError(f"R2 SQL {r.status_code}: {r.text[:300]}")
            payload=r.json()
            if not payload.get("success",False): raise RuntimeError(f"R2 SQL failed: {payload}")
            return
        except requests.RequestException as exc:
            last=exc
            if attempt==5: break
            delay=min(2**(attempt-1),16)
            print(json.dumps({"result":"retry","operation":"r2_sql","attempt":attempt,
                              "delay_seconds":delay,"error":str(exc)[:240]}),flush=True)
            time.sleep(delay)
    raise RuntimeError(f"R2 SQL connection failed after retries: {last}")


def optimize(cat, client, source_id, target_id):
    source=cat.load_table(source_id)
    target=cat.load_table(target_id)
    source_files=list(source.scan().plan_files())
    target_files=list(target.scan().plan_files())
    if len(source_files)!=len(target_files):
        raise RuntimeError(f"file count mismatch {target_id}")
    source_rows=sum(int(x.file.record_count) for x in source_files)
    target_rows=sum(int(x.file.record_count) for x in target_files)
    source_bytes=sum(int(x.file.file_size_in_bytes) for x in source_files)
    target_bytes=sum(int(x.file.file_size_in_bytes) for x in target_files)
    if (source_rows,source_bytes)!=(target_rows,target_bytes):
        raise RuntimeError(f"row/byte mismatch {target_id}")

    source_paths={str(x.file.file_path) for x in source_files}
    target_paths={str(x.file.file_path) for x in target_files}
    if target_paths==source_paths:
        return {"target":".".join(target_id),"files":len(source_files),"rows":source_rows,
                "bytes_reclaimed":0,"already_zero_copy":True}

    old_files=[x.file for x in target_files]
    with target.transaction() as tx:
        with tx.update_snapshot(snapshot_properties={
            "zohelo.transform.mode":"zero-copy-promotion-v1",
            "zohelo.source.table":".".join(source_id),
        }).overwrite() as overwrite:
            for f in old_files:
                overwrite.delete_data_file(f)
            for task in source_files:
                overwrite.append_data_file(clone_ref(target, task.file))

    refreshed=cat.load_table(target_id)
    final={str(x.file.file_path) for x in refreshed.scan().plan_files()}
    if final!=source_paths:
        raise RuntimeError(f"zero-copy membership mismatch {target_id}")

    reclaimed=0
    bucket=req("R2_LAKEHOUSE_BUCKET")
    for f in old_files:
        b,key=parse(str(f.file_path))
        if b!=bucket: raise RuntimeError(f"unexpected target bucket {b}")
        # Delete only old promotion objects after the catalog no longer references them.
        if "/data/" not in key or not (key.startswith("03_silver/") or key.startswith("04_gold/")):
            raise RuntimeError(f"refusing to delete unexpected promotion path {key}")
        client.delete_object(Bucket=bucket,Key=key)
        reclaimed+=int(f.file_size_in_bytes)

    print(json.dumps({"result":"optimized","source":".".join(source_id),
      "target":".".join(target_id),"files":len(source_files),"rows":source_rows,
      "bytes_reclaimed":reclaimed}),flush=True)
    return {"target":".".join(target_id),"files":len(source_files),"rows":source_rows,
            "bytes_reclaimed":reclaimed,"already_zero_copy":False}


def optimize_with_retry(client, source_id, target_id):
    last=None
    for attempt in range(1,6):
        try:
            # Use a fresh REST catalog/session on each attempt so a stale HTTP
            # keep-alive connection cannot poison the remainder of the run.
            return optimize(catalog(),client,source_id,target_id)
        except requests.RequestException as exc:
            last=exc
        except Exception as exc:
            # PyIceberg may surface requests/urllib3 connection failures through
            # a wrapper exception. Retry only recognizable transport failures;
            # deterministic membership/row/schema errors still fail immediately.
            text=str(exc)
            if not any(token in text for token in (
                "Connection aborted","RemoteDisconnected","Connection reset",
                "Read timed out","ConnectTimeout","Max retries exceeded",
            )):
                raise
            last=exc
        if attempt==5:
            break
        delay=min(2**(attempt-1),16)
        print(json.dumps({"result":"retry","operation":"optimize",
                          "source":".".join(source_id),"target":".".join(target_id),
                          "attempt":attempt,"delay_seconds":delay,
                          "error":str(last)[:240]}),flush=True)
        time.sleep(delay)
    raise RuntimeError(f"catalog connection failed after retries for {target_id}: {last}")


def main():
    client=s3(); results=[]
    for bronze,silver in SILVER.items():
        results.append(optimize_with_retry(client,("bronze",bronze),("silver",silver)))
    for bronze,gold in GOLD.items():
        silver=SILVER[bronze]
        results.append(optimize_with_retry(client,("silver",silver),("gold",gold)))

    for q in [
      "SELECT * FROM silver.dbw_observations LIMIT 1;",
      "SELECT * FROM gold.fact_dbw_observations LIMIT 1;",
      "SELECT * FROM gold.dim_opendata_organization LIMIT 1;",
      "SELECT * FROM gold.dim_corporate_entity LIMIT 1;",
    ]:
        sql(q)
    total=sum(int(x["bytes_reclaimed"]) for x in results)
    print(json.dumps({"result":"pass","operation":"optimize-r2-promotions",
      "tables":len(results),"bytes_reclaimed":total,
      "completed_at_utc":datetime.now(timezone.utc).isoformat(),
      "google_drive_accessed":False}),flush=True)


if __name__=="__main__":
    main()
