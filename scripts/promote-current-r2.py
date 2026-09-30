#!/usr/bin/env python3
"""Promote the currently available R2 Iceberg Bronze data through Silver and Gold.

This v1 promotion is intentionally conservative:
- existing production BDL/Eurostat/NBP/WDI Silver/Gold tables are untouched;
- Bronze tables that already contain typed, parsed Parquet are physically cloned
  into Silver so each Iceberg table owns its own R2 objects;
- Gold receives source-oriented semantic tables for the same data;
- no Google Drive access, no schedules, no source ingestion, no release/current logic.
"""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib, json, os
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError, TableAlreadyExistsError
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import TableProperties
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
PREFIX={"silver":"03_silver","gold":"04_gold"}

def req(n):
    v=os.environ.get(n,"").strip()
    if not v: raise RuntimeError(f"missing {n}")
    return v

def s3():
    return boto3.client("s3",endpoint_url=req("R2_S3_ENDPOINT"),region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4",retries={"max_attempts":10,"mode":"standard"}))

def catalog():
    return RestCatalog(name="zohelo",warehouse=req("R2_WAREHOUSE"),uri=req("R2_CATALOG_URI"),
        token=req("R2_DATA_CATALOG_TOKEN"),
        **{"s3.access-key-id":req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
           "s3.secret-access-key":req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
           "s3.endpoint":req("R2_S3_ENDPOINT"),"s3.region":"auto"})

def parse(uri):
    p=urlsplit(uri)
    if p.scheme!="s3": raise RuntimeError(f"unexpected uri {uri}")
    return p.netloc,p.path.lstrip("/")

def ensure_ns(cat,ns):
    try: cat.create_namespace(ns)
    except NamespaceAlreadyExistsError: pass

def target_key(layer,table,src):
    h=hashlib.sha256(src.encode()).hexdigest()[:16]
    return f"{PREFIX[layer]}/{table}/data/{h}-{PurePosixPath(src).name}"

def minimal(table,source_file,new_uri):
    return DataFile.from_args(
      _table_format_version=int(table.metadata.format_version),
      content=DataFileContent.DATA,file_path=new_uri,file_format=FileFormat.PARQUET,
      partition=Record(),record_count=int(source_file.record_count),
      file_size_in_bytes=int(source_file.file_size_in_bytes),
      spec_id=int(table.metadata.default_spec_id))

def copy_one(client,bucket,src,dst,size):
    try:
        h=client.head_object(Bucket=bucket,Key=dst)
        if int(h["ContentLength"])==size: return
    except Exception: pass
    client.copy_object(Bucket=bucket,Key=dst,CopySource={"Bucket":bucket,"Key":src})
    h=client.head_object(Bucket=bucket,Key=dst)
    if int(h["ContentLength"])!=size: raise RuntimeError(f"copy size mismatch {dst}")

def promote(cat,client,bucket,src_id,dst_id,layer):
    source=cat.load_table(src_id)
    ensure_ns(cat,layer)
    try: target=cat.load_table(dst_id)
    except NoSuchTableError:
        try:
            target=cat.create_table(dst_id,schema=source.schema(),properties={
              "zohelo.transform.mode":"physical-promotion-v1",
              "zohelo.source.table":".".join(src_id),
              "zohelo.ready":"partial-current-data"})
        except TableAlreadyExistsError: target=cat.load_table(dst_id)
    source_tasks=list(source.scan().plan_files())
    expected={}
    jobs=[]
    for task in source_tasks:
        b,k=parse(str(task.file.file_path))
        if b!=bucket: raise RuntimeError(f"unexpected source bucket {b}")
        dst=target_key(layer,dst_id[1],k)
        uri=f"s3://{bucket}/{dst}"
        expected[uri]=task.file
        jobs.append((client,bucket,k,dst,int(task.file.file_size_in_bytes)))
    with ThreadPoolExecutor(max_workers=24) as pool:
        list(pool.map(lambda x: copy_one(*x),jobs))
    existing={str(t.file.file_path):t.file for t in target.scan().plan_files()}
    unexpected=set(existing)-set(expected)
    if unexpected: raise RuntimeError(f"unexpected files in {'.'.join(dst_id)}: {len(unexpected)}")
    missing=[u for u in expected if u not in existing]
    if missing:
        with target.transaction() as tx:
            if tx.table_metadata.name_mapping() is None:
                tx.set_properties(**{TableProperties.DEFAULT_NAME_MAPPING:
                    tx.table_metadata.schema().name_mapping.model_dump_json()})
            with tx.update_snapshot(snapshot_properties={
                "zohelo.operation":"physical-promotion-v1",
                "zohelo.source.table":".".join(src_id)}).fast_append() as app:
                for uri in missing: app.append_data_file(minimal(target,expected[uri],uri))
        target=cat.load_table(dst_id)
    final=list(target.scan().plan_files())
    if len(final)!=len(expected): raise RuntimeError(f"membership mismatch {'.'.join(dst_id)}")
    rows=sum(int(t.file.record_count) for t in final)
    print(json.dumps({"result":"promoted","source":".".join(src_id),"target":".".join(dst_id),
                      "files":len(final),"rows":rows,"copied_or_reused":len(expected)}),flush=True)
    return {"source":".".join(src_id),"target":".".join(dst_id),"files":len(final),"rows":rows}

def sql(query):
    url=f"https://api.sql.cloudflarestorage.com/api/v1/accounts/{req('CLOUDFLARE_ACCOUNT_ID')}/r2-sql/query/{req('R2_LAKEHOUSE_BUCKET')}"
    r=requests.post(url,headers={"Authorization":f"Bearer {req('R2_DATA_CATALOG_TOKEN')}","Content-Type":"application/json"},
                    json={"query":query},timeout=120)
    if r.status_code!=200: raise RuntimeError(f"R2 SQL {r.status_code}: {r.text[:300]}")
    p=r.json()
    if not p.get("success",False): raise RuntimeError(f"R2 SQL failed: {p}")
    return p

def main():
    cat=catalog(); client=s3(); bucket=req("R2_LAKEHOUSE_BUCKET")
    results=[]
    for src,dst in SILVER.items():
        results.append(promote(cat,client,bucket,("bronze",src),("silver",dst),"silver"))
    for silver_name,gold_name in GOLD.items():
        results.append(promote(cat,client,bucket,("silver",SILVER[silver_name]),("gold",gold_name),"gold"))
    for q in [
      "SHOW TABLES IN silver;","SHOW TABLES IN gold;",
      "SELECT * FROM silver.dbw_observations LIMIT 1;",
      "SELECT * FROM gold.fact_dbw_observations LIMIT 1;",
      "SELECT * FROM gold.dim_corporate_entity LIMIT 1;",
      "SELECT * FROM gold.dim_opendata_organization LIMIT 1;",
      "SELECT * FROM gold.dim_territory LIMIT 1;"]:
        sql(q); print(json.dumps({"result":"sql_pass","query":q}),flush=True)
    print(json.dumps({"result":"pass","operation":"promote-current-r2",
      "completed_at_utc":datetime.now(timezone.utc).isoformat(),"promotions":len(results),
      "google_drive_accessed":False}),flush=True)

if __name__=="__main__": main()
