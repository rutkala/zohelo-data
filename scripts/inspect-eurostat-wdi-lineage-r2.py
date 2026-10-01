#!/usr/bin/env python3
"""Read-only exact Landing-to-Iceberg hash membership for Eurostat and WDI.

This script never archives, deletes, rewrites pointers, or mutates Iceberg metadata.
Landing object names are content-addressed by the production campaign store. The
script compares those identities to provenance columns queried directly through
Cloudflare R2 SQL so multi-GB table files do not need to be downloaded.
"""
from __future__ import annotations

import argparse
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


def landing(client,bucket:str,source:str)->dict[str,dict[str,Any]]:
    prefix=f"01_landing/{source}/"
    result={}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket,Prefix=prefix):
        for item in page.get("Contents",[]):
            key=str(item["Key"]); size=int(item.get("Size",0))
            if size<=0 or key.endswith("/"):
                continue
            name=PurePosixPath(key).name
            match=NAME_RE.fullmatch(name)
            if not match:
                raise RuntimeError(f"unexpected non-content-addressed Landing object: {key}")
            kind,digest=match.groups()
            if digest in result:
                raise RuntimeError(f"duplicate Landing SHA-256 identity: {digest}")
            result[digest]={"key":key,"size":size,"kind":kind}
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
    if isinstance(payload.get("result"),list):
        candidates.extend(payload["result"])
    elif isinstance(payload.get("result"),dict):
        candidates.append(payload["result"])
    candidates.append(payload)

    for candidate in candidates:
        if not isinstance(candidate,dict):
            continue
        value=candidate.get("results")
        if isinstance(value,list):
            if not value or isinstance(value[0],dict):
                return value
        if isinstance(value,dict):
            columns=value.get("columns"); rows=value.get("rows")
            if isinstance(columns,list) and isinstance(rows,list):
                return [dict(zip(columns,row)) for row in rows]
        rows=candidate.get("rows")
        if isinstance(rows,list) and (not rows or isinstance(rows[0],dict)):
            return rows
    raise RuntimeError(f"R2 SQL response shape is unsupported: {json.dumps(payload)[:1000]}")


def one_column(query:str,column:str)->set[str]:
    rows=sql(query)
    values=set()
    for row in rows:
        value=row.get(column)
        if value is None:
            continue
        if not isinstance(value,str) or not re.fullmatch(r"[0-9a-f]{64}",value):
            raise RuntimeError(f"invalid {column} value from R2 SQL: {value!r}")
        values.add(value)
    return values


def summarize(name:str,landing_set:set[str],proven:set[str],meta:dict[str,dict[str,Any]])->dict[str,Any]:
    represented=landing_set & proven
    unrepresented=landing_set-proven
    missing_raw=proven-landing_set
    return {
        "name":name,
        "landing_sha256":len(landing_set),
        "provenance_sha256":len(proven),
        "represented_landing_sha256":len(represented),
        "unrepresented_landing_sha256":len(unrepresented),
        "provenance_without_landing_sha256":len(missing_raw),
        "represented_bytes":sum(int(meta[d]["size"]) for d in represented),
        "unrepresented_bytes":sum(int(meta[d]["size"]) for d in unrepresented),
        "represented_sample":sorted(represented)[:12],
        "unrepresented_sample":sorted(unrepresented)[:12],
        "provenance_without_landing_sample":sorted(missing_raw)[:12],
    }


def inspect_eurostat(meta:dict[str,dict[str,Any]])->dict[str,Any]:
    bulk={digest for digest,item in meta.items() if "/bulk/" in item["key"]}
    responses={digest for digest,item in meta.items() if "/responses/" in item["key"]}
    if len(bulk)+len(responses)!=len(meta):
        raise RuntimeError("Eurostat Landing contains an unexpected path")
    full=one_column(
        "SELECT DISTINCT raw_sha256 FROM bronze.eurostat_full_observations "
        "WHERE raw_sha256 IS NOT NULL;",
        "raw_sha256",
    )
    standard=one_column(
        "SELECT DISTINCT response_sha256 FROM bronze.eurostat_observations "
        "WHERE response_sha256 IS NOT NULL;",
        "response_sha256",
    )
    return {
        "bulk":summarize("bulk",bulk,full,meta),
        "responses":summarize("responses",responses,standard,meta),
        "all_provenance_recoverable": (full|standard) <= set(meta),
    }


def inspect_wdi(meta:dict[str,dict[str,Any]])->dict[str,Any]:
    bulk={digest for digest,item in meta.items() if "/bulk/" in item["key"]}
    responses={digest for digest,item in meta.items() if "/responses/" in item["key"]}
    if len(bulk)!=1 or len(bulk)+len(responses)!=len(meta):
        raise RuntimeError("WDI Landing does not contain exactly one bulk object plus responses")
    modeled=one_column(
        "SELECT DISTINCT archive_sha256 FROM silver.wdi_observations "
        "WHERE archive_sha256 IS NOT NULL;",
        "archive_sha256",
    )
    return {
        "bulk":summarize("bulk",bulk,modeled,meta),
        "responses":{
            "landing_sha256":len(responses),
            "landing_bytes":sum(int(meta[d]["size"]) for d in responses),
            "modeled_provenance_column":"none_currently_observed_for_api_responses",
        },
        "modeled_archive_sha256":sorted(modeled),
    }


def main()->int:
    parser=argparse.ArgumentParser()
    parser.add_argument("--source",choices=SOURCES,required=True)
    args=parser.parse_args()
    client=s3(); bucket=req("R2_LANDING_BUCKET")
    meta=landing(client,bucket,args.source)
    result=inspect_eurostat(meta) if args.source=="eurostat" else inspect_wdi(meta)
    print(json.dumps({
        "result":"pass",
        "operation":"inspect-exact-landing-iceberg-hash-membership",
        "source":args.source,
        "landing_objects":len(meta),
        "landing_bytes":sum(int(item["size"]) for item in meta.values()),
        "writes_performed":False,
        **result,
    },sort_keys=True))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
