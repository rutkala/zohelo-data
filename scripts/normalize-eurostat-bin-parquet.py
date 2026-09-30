#!/usr/bin/env python3
"""Normalize legacy Eurostat Bronze *.bin objects that contain Parquet bytes.

The historical Eurostat bulk Bronze output used a generic object store whose
content-addressed names always ended in .bin, even when the payload was Parquet.
This one-time job:

1. inventories every 02_bronze/eurostat/**/*.bin object;
2. proves every object is readable Parquet with one consistent schema;
3. copies it to a canonical .parquet key;
4. creates/updates Bronze, Silver and Gold Iceberg tables that reference those
   canonical Parquet objects (zero-copy across layers);
5. validates representative R2 SQL reads;
6. deletes the misleading old .bin objects only after all checks pass.

No Google Drive access is used.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
import pyarrow.fs as pafs
from pyarrow.fs import AwsStandardS3RetryStrategy
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError, TableAlreadyExistsError
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.table import TableProperties
from pyiceberg.typedef import Record
import requests

SOURCE_PREFIX="02_bronze/eurostat/"
BRONZE_ID=("bronze","eurostat_full_observations")
SILVER_ID=("silver","eurostat_full_observations")
GOLD_ID=("gold","fact_eurostat_full_observations")


def req(name: str) -> str:
    value=os.environ.get(name,"").strip()
    if not value: raise RuntimeError(f"missing {name}")
    return value


def s3():
    return boto3.client(
        "s3",endpoint_url=req("R2_S3_ENDPOINT"),region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4",retries={"max_attempts":10,"mode":"standard"}))


def fs():
    endpoint=urlsplit(req("R2_S3_ENDPOINT"))
    if not endpoint.hostname: raise RuntimeError("invalid R2 endpoint")
    return pafs.S3FileSystem(
        access_key=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        secret_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        region="auto",endpoint_override=endpoint.netloc,scheme="https",
        retry_strategy=AwsStandardS3RetryStrategy(max_attempts=8))


def catalog():
    return RestCatalog(
        name="zohelo",warehouse=req("R2_WAREHOUSE"),uri=req("R2_CATALOG_URI"),
        token=req("R2_DATA_CATALOG_TOKEN"),
        **{"s3.access-key-id":req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
           "s3.secret-access-key":req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
           "s3.endpoint":req("R2_S3_ENDPOINT"),"s3.region":"auto"})


def list_bins(client,bucket: str):
    result=[]
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket,Prefix=SOURCE_PREFIX):
        for item in page.get("Contents",[]):
            key=str(item["Key"])
            if key.lower().endswith(".bin") and int(item.get("Size",0))>0:
                result.append((key,int(item["Size"])))
    return sorted(result)


def canonical_key(source_key: str) -> str:
    digest=hashlib.sha256(source_key.encode()).hexdigest()[:20]
    return f"02_bronze/eurostat_full_observations/data/{digest}.parquet"


def ensure_namespace(cat,ns: str):
    try: cat.create_namespace(ns)
    except NamespaceAlreadyExistsError: pass


def copy_one(client,bucket: str,source_key: str,target_key: str,size: int):
    try:
        head=client.head_object(Bucket=bucket,Key=target_key)
        if int(head.get("ContentLength",-1))==size: return
    except Exception:
        pass
    client.copy_object(
        Bucket=bucket,Key=target_key,
        CopySource={"Bucket":bucket,"Key":source_key},
        ContentType="application/vnd.apache.parquet",
        MetadataDirective="REPLACE",
        Metadata={"zohelo-source-key-sha256":hashlib.sha256(source_key.encode()).hexdigest()},
    )
    head=client.head_object(Bucket=bucket,Key=target_key)
    if int(head.get("ContentLength",-1))!=size:
        raise RuntimeError(f"copy size mismatch {target_key}")


def minimal(table,uri: str,rows: int,size: int):
    return DataFile.from_args(
        _table_format_version=int(table.metadata.format_version),
        content=DataFileContent.DATA,file_path=uri,file_format=FileFormat.PARQUET,
        partition=Record(),record_count=rows,file_size_in_bytes=size,
        spec_id=int(table.metadata.default_spec_id))


def ensure_table(cat,identifier,schema,files):
    ensure_namespace(cat,identifier[0])
    try: table=cat.load_table(identifier)
    except NoSuchTableError:
        try:
            table=cat.create_table(identifier,schema=schema,properties={
                "zohelo.source":"legacy-eurostat-bulk-bronze-normalization",
                "zohelo.ready":"partial-current-data"})
        except TableAlreadyExistsError:
            table=cat.load_table(identifier)
    existing={str(task.file.file_path):task.file for task in table.scan().plan_files()}
    expected={uri:(rows,size) for uri,rows,size in files}
    unexpected=set(existing)-set(expected)
    if unexpected:
        raise RuntimeError(f"unexpected data files in {'.'.join(identifier)}: {len(unexpected)}")
    missing=[uri for uri in expected if uri not in existing]
    if missing:
        with table.transaction() as tx:
            if tx.table_metadata.name_mapping() is None:
                tx.set_properties(**{TableProperties.DEFAULT_NAME_MAPPING:
                    tx.table_metadata.schema().name_mapping.model_dump_json()})
            with tx.update_snapshot(snapshot_properties={
                "zohelo.operation":"normalize-eurostat-bin-parquet",
            }).fast_append() as append:
                for uri in missing:
                    rows,size=expected[uri]
                    append.append_data_file(minimal(table,uri,rows,size))
        table=cat.load_table(identifier)
    final=list(table.scan().plan_files())
    final_map={str(task.file.file_path):(int(task.file.record_count),int(task.file.file_size_in_bytes))
               for task in final}
    if final_map!=expected:
        raise RuntimeError(f"membership mismatch {'.'.join(identifier)}")
    return {"table":".".join(identifier),"files":len(final),
            "rows":sum(row for row,_ in final_map.values()),
            "bytes":sum(size for _,size in final_map.values())}


def sql(query: str):
    url=(f"https://api.sql.cloudflarestorage.com/api/v1/accounts/"
         f"{req('CLOUDFLARE_ACCOUNT_ID')}/r2-sql/query/{req('R2_LAKEHOUSE_BUCKET')}")
    r=requests.post(url,headers={"Authorization":f"Bearer {req('R2_DATA_CATALOG_TOKEN')}",
                    "Content-Type":"application/json"},json={"query":query},timeout=120)
    if r.status_code!=200: raise RuntimeError(f"R2 SQL {r.status_code}: {r.text[:300]}")
    payload=r.json()
    if not payload.get("success",False): raise RuntimeError(f"R2 SQL failed: {payload}")


def main():
    client=s3(); arrow=fs(); cat=catalog(); bucket=req("R2_LAKEHOUSE_BUCKET")
    sources=list_bins(client,bucket)
    if not sources:
        # Idempotent success after a previous completed normalization.
        for q in [
            "SELECT * FROM bronze.eurostat_full_observations LIMIT 1;",
            "SELECT * FROM silver.eurostat_full_observations LIMIT 1;",
            "SELECT * FROM gold.fact_eurostat_full_observations LIMIT 1;",
        ]: sql(q)
        print(json.dumps({"result":"pass","operation":"normalize-eurostat-bin-parquet",
                          "already_normalized":True,"google_drive_accessed":False}))
        return

    reference_schema=None
    files=[]
    source_bytes=0
    source_rows=0
    for index,(key,size) in enumerate(sources,1):
        metadata=pq.read_metadata(f"{bucket}/{key}",filesystem=arrow)
        schema=metadata.schema.to_arrow_schema()
        if reference_schema is None:
            reference_schema=schema
        elif not schema.equals(reference_schema,check_metadata=False):
            raise RuntimeError(f"Eurostat legacy Parquet schema mismatch: {key}")
        rows=int(metadata.num_rows)
        if rows<=0:
            raise RuntimeError(f"Eurostat legacy Parquet object is empty: {key}")
        target=canonical_key(key)
        copy_one(client,bucket,key,target,size)
        files.append((f"s3://{bucket}/{target}",rows,size))
        source_bytes+=size; source_rows+=rows
        if index%20==0 or index==len(sources):
            print(json.dumps({"operation":"verified_and_copied","objects":index,
                              "total":len(sources)}),flush=True)

    assert reference_schema is not None
    results=[
        ensure_table(cat,BRONZE_ID,reference_schema,files),
        ensure_table(cat,SILVER_ID,reference_schema,files),
        ensure_table(cat,GOLD_ID,reference_schema,files),
    ]
    for result in results:
        if result["rows"]!=source_rows or result["bytes"]!=source_bytes:
            raise RuntimeError(f"row/byte reconciliation failed {result['table']}")

    for q in [
        "SELECT * FROM bronze.eurostat_full_observations LIMIT 1;",
        "SELECT * FROM silver.eurostat_full_observations LIMIT 1;",
        "SELECT * FROM gold.fact_eurostat_full_observations LIMIT 1;",
    ]:
        sql(q)

    # No production table may point at the legacy .bin keys before deletion.
    source_uris={f"s3://{bucket}/{key}" for key,_ in sources}
    for ns in ("bronze","silver","gold"):
        for identifier in cat.list_tables(ns):
            refs={str(task.file.file_path) for task in cat.load_table(identifier).scan().plan_files()}
            overlap=refs & source_uris
            if overlap:
                raise RuntimeError(f"legacy .bin object still referenced by {identifier}: {len(overlap)}")

    for key,_ in sources:
        client.delete_object(Bucket=bucket,Key=key)
    remaining=list_bins(client,bucket)
    if remaining:
        raise RuntimeError(f"legacy Eurostat .bin cleanup incomplete: {len(remaining)}")

    # Final reads after destructive cleanup.
    for q in [
        "SELECT * FROM bronze.eurostat_full_observations LIMIT 1;",
        "SELECT * FROM silver.eurostat_full_observations LIMIT 1;",
        "SELECT * FROM gold.fact_eurostat_full_observations LIMIT 1;",
    ]:
        sql(q)

    print(json.dumps({
        "result":"pass","operation":"normalize-eurostat-bin-parquet",
        "legacy_objects":len(sources),"bytes":source_bytes,"rows":source_rows,
        "tables":results,"legacy_bin_deleted":True,
        "completed_at_utc":datetime.now(timezone.utc).isoformat(),
        "google_drive_accessed":False,
    },sort_keys=True),flush=True)


if __name__=="__main__":
    main()
