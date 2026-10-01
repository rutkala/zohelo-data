#!/usr/bin/env python3
"""Archive only R2 Landing objects already proven by current Iceberg provenance.

Supported bounded scopes:
- Eurostat: bulk raw_sha256 in bronze.eurostat_full_observations and response_sha256
  in bronze.eurostat_observations.
- World Bank WDI: the one bulk ZIP whose SHA-256 equals silver.wdi_observations.archive_sha256.

Every archive copy is server-side copied and then fully streamed back through SHA-256
before the immutable receipt is written. Landing is never deleted here because the
migrated source-campaign pointers still require separate R2-native reconciliation.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import PurePosixPath
import re
from typing import Any

import boto3
from botocore.config import Config
import requests

SOURCES=("eurostat","world_bank_wdi")
NAME_RE=re.compile(r"^(raw|response)-([0-9a-f]{64})\.bin$")


def req(name:str)->str:
    value=os.environ.get(name,"").strip()
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def s3():
    return boto3.client(
        "s3",endpoint_url=req("R2_S3_ENDPOINT").rstrip("/"),region_name="auto",
        aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4",retries={"max_attempts":8,"mode":"standard"}))


def list_objects(client,bucket:str,prefix:str)->list[dict[str,Any]]:
    rows=[]
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket,Prefix=prefix):
        for item in page.get("Contents",[]):
            key=str(item["Key"]); size=int(item.get("Size",0))
            if key.endswith("/") and size==0:
                continue
            rows.append({"key":key,"size":size})
    rows.sort(key=lambda x:x["key"])
    return rows


def landing(client,bucket:str,source:str)->dict[str,dict[str,Any]]:
    result={}
    prefix=f"01_landing/{source}/"
    for item in list_objects(client,bucket,prefix):
        if item["size"]<=0:
            continue
        name=PurePosixPath(item["key"]).name
        match=NAME_RE.fullmatch(name)
        if not match:
            raise RuntimeError(f"unexpected Landing object: {item['key']}")
        kind,digest=match.groups()
        if digest in result:
            raise RuntimeError(f"duplicate Landing digest: {digest}")
        result[digest]={**item,"kind":kind,"sha256":digest}
    return result


def sql(query:str)->list[dict[str,Any]]:
    url=(f"https://api.sql.cloudflarestorage.com/api/v1/accounts/"
         f"{req('CLOUDFLARE_ACCOUNT_ID')}/r2-sql/query/{req('R2_LAKEHOUSE_BUCKET')}")
    response=requests.post(
        url,
        headers={"Authorization":f"Bearer {req('R2_DATA_CATALOG_TOKEN')}",
                 "Content-Type":"application/json"},
        json={"query":query},timeout=180,
    )
    if response.status_code!=200:
        raise RuntimeError(f"R2 SQL {response.status_code}: {response.text[:500]}")
    payload=response.json()
    if payload.get("success") is False:
        raise RuntimeError(f"R2 SQL failed: {json.dumps(payload)[:800]}")
    candidates=[]
    if isinstance(payload.get("result"),list): candidates.extend(payload["result"])
    elif isinstance(payload.get("result"),dict): candidates.append(payload["result"])
    candidates.append(payload)
    for candidate in candidates:
        if not isinstance(candidate,dict): continue
        value=candidate.get("results")
        if isinstance(value,list) and (not value or isinstance(value[0],dict)):
            return value
        if isinstance(value,dict):
            columns=value.get("columns"); rows=value.get("rows")
            if isinstance(columns,list) and isinstance(rows,list):
                return [dict(zip(columns,row)) for row in rows]
        rows=candidate.get("rows")
        if isinstance(rows,list) and (not rows or isinstance(rows[0],dict)):
            return rows
    raise RuntimeError(f"unsupported R2 SQL response: {json.dumps(payload)[:1000]}")


def one_column(query:str,column:str)->set[str]:
    result=set()
    for row in sql(query):
        value=row.get(column)
        if value is None: continue
        if not isinstance(value,str) or not re.fullmatch(r"[0-9a-f]{64}",value):
            raise RuntimeError(f"invalid {column}: {value!r}")
        result.add(value)
    return result


def proven_for(source:str,meta:dict[str,dict[str,Any]])->tuple[set[str],dict[str,Any]]:
    if source=="eurostat":
        bulk=one_column(
            "SELECT DISTINCT raw_sha256 FROM bronze.eurostat_full_observations "
            "WHERE raw_sha256 IS NOT NULL;","raw_sha256")
        responses=one_column(
            "SELECT DISTINCT response_sha256 FROM bronze.eurostat_observations "
            "WHERE response_sha256 IS NOT NULL;","response_sha256")
        proven=bulk|responses
        return proven,{
            "proof":"r2_sql_distinct_provenance_hash_membership",
            "bronze_tables":[
                {"table":"bronze.eurostat_full_observations","column":"raw_sha256","distinct_sha256":len(bulk)},
                {"table":"bronze.eurostat_observations","column":"response_sha256","distinct_sha256":len(responses)},
            ],
        }
    bulk={digest for digest,item in meta.items() if "/bulk/" in item["key"]}
    modeled=one_column(
        "SELECT DISTINCT archive_sha256 FROM silver.wdi_observations "
        "WHERE archive_sha256 IS NOT NULL;","archive_sha256")
    if len(bulk)!=1 or modeled!=bulk:
        raise RuntimeError(
            f"WDI bulk/model lineage mismatch: landing={sorted(bulk)} modeled={sorted(modeled)}")
    return bulk,{
        "proof":"silver_archive_sha256_equals_bulk_landing_sha256",
        "bronze_tables":[
            {"tables":["bronze.wdi_country","bronze.wdi_country_series","bronze.wdi_data",
                       "bronze.wdi_footnote","bronze.wdi_series","bronze.wdi_series_time"],
             "modeled_lineage_table":"silver.wdi_observations",
             "column":"archive_sha256","distinct_sha256":1},
        ],
    }


def hash_object(client,bucket:str,key:str)->tuple[str,int]:
    response=client.get_object(Bucket=bucket,Key=key)
    body=response["Body"]; digest=sha256(); size=0
    try:
        while True:
            chunk=body.read(1024*1024)
            if not chunk: break
            digest.update(chunk); size+=len(chunk)
    finally:
        body.close()
    return digest.hexdigest(),size


def archive_key(source:str,item:dict[str,Any])->str:
    return f"05_archive/{source}/{item['kind']}/{item['sha256']}.bin"


def ensure_archive(client,bucket:str,source:str,item:dict[str,Any])->dict[str,Any]:
    target=archive_key(source,item)
    copied=False
    try:
        head=client.head_object(Bucket=bucket,Key=target)
        present=int(head.get("ContentLength",-1))==int(item["size"])
    except Exception:
        present=False
    if not present:
        client.copy_object(
            Bucket=bucket,Key=target,
            CopySource={"Bucket":bucket,"Key":item["key"]},
            MetadataDirective="REPLACE",
            Metadata={"source-sha256":item["sha256"]},
            ContentType="application/octet-stream",
        )
        copied=True
    observed,size=hash_object(client,bucket,target)
    if observed!=item["sha256"] or size!=item["size"]:
        raise RuntimeError(f"Archive readback mismatch: {item['key']}")
    return {
        "source_key":item["key"],"archive_key":target,"size":size,"sha256":observed,
        "kind":item["kind"],"copied_this_run":copied,"compression":"none_exact_native_bytes",
    }


def blockers(client,lakehouse_bucket:str,source:str)->list[str]:
    prefixes=[f"06_control/source_campaigns/{source}/",f"06_control/source_campaigns/{source}_bulk/"]
    if source=="eurostat":
        prefixes.append("06_control/source_campaigns/eurostat_bulk_bronze/")
    names={"current-ingestion-state.json","current-landing.json","publication-owner.json"}
    found=[]
    for prefix in prefixes:
        for item in list_objects(client,lakehouse_bucket,prefix):
            if PurePosixPath(item["key"]).name in names and item["size"]>0:
                found.append(item["key"])
    return sorted(found)


def immutable_json(client,bucket:str,key:str,value:dict[str,Any])->None:
    raw=(json.dumps(value,indent=2,sort_keys=True)+"\n").encode()
    try:
        existing=client.get_object(Bucket=bucket,Key=key)["Body"].read()
    except Exception:
        existing=None
    if existing is not None:
        if existing!=raw:
            raise RuntimeError(f"immutable receipt conflict: {key}")
        return
    client.put_object(
        Bucket=bucket,Key=key,Body=raw,ContentType="application/json",
        Metadata={"sha256":sha256(raw).hexdigest()})


def main()->int:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source",choices=SOURCES,required=True)
    args=parser.parse_args()
    client=s3(); landing_bucket=req("R2_LANDING_BUCKET"); lakehouse_bucket=req("R2_LAKEHOUSE_BUCKET")
    meta=landing(client,landing_bucket,args.source)
    proven,proof=proven_for(args.source,meta)
    if not proven<=set(meta):
        raise RuntimeError(f"Iceberg provenance lacks Landing bytes: {len(proven-set(meta))}")
    selected=[meta[digest] for digest in sorted(proven)]

    with ThreadPoolExecutor(max_workers=16) as pool:
        archived=list(pool.map(lambda item: ensure_archive(client,landing_bucket,args.source,item),selected))

    active=blockers(client,lakehouse_bucket,args.source)
    digest=sha256(json.dumps(
        [{"key":x["source_key"],"size":x["size"],"sha256":x["sha256"]} for x in archived],
        sort_keys=True,separators=(",",":")).encode()).hexdigest()
    receipt={
        "format_version":1,
        "kind":"partial_landing_bronze_lineage_receipt",
        "source_id":args.source,
        "input_set_sha256":digest,
        "coverage_scope":"only_landing_hashes_proven_by_current_iceberg_provenance",
        "proof":proof,
        "landing_inputs":selected,
        "archive":archived,
        "archive_readback_verified":True,
        "landing_deleted":False,
        "landing_delete_eligible":False,
        "blocking_control_references":active,
        "unproven_landing_objects":len(meta)-len(selected),
        "unproven_landing_bytes":sum(int(item["size"]) for d,item in meta.items() if d not in proven),
        "observed_at_utc":datetime.now(timezone.utc).isoformat(),
    }
    receipt_key=f"06_control/lineage/{args.source}/partial-{digest}.json"
    immutable_json(client,lakehouse_bucket,receipt_key,receipt)
    print(json.dumps({
        "result":"pass","operation":"archive-proven-r2-lineage","source":args.source,
        "proven_landing_objects":len(selected),
        "proven_landing_bytes":sum(int(x["size"]) for x in selected),
        "archive_objects_verified":len(archived),
        "archive_objects_copied_this_run":sum(1 for x in archived if x["copied_this_run"]),
        "unproven_landing_objects":receipt["unproven_landing_objects"],
        "unproven_landing_bytes":receipt["unproven_landing_bytes"],
        "landing_deleted":False,
        "blocking_control_references":active,
        "lineage_receipt":f"s3://{lakehouse_bucket}/{receipt_key}",
    },sort_keys=True))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
