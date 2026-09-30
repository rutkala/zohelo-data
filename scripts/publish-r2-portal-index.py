#!/usr/bin/env python3
"""Publish the portal file index directly from the live Cloudflare R2 buckets.

The portal is internal and reflects live R2 state. No Google Drive inventory,
release pointer, or publication manifest is consulted.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
import os
from pathlib import PurePosixPath

import boto3
from botocore.config import Config

CURRENT_KEY="06_control/portal_index/v1/current.json"
FOLDER="application/vnd.google-apps.folder"
ROOT_ID="r2-root-zohelo-data"
EXCLUDED_LAKEHOUSE_PREFIXES=(
    "__r2_data_catalog/",
    "06_control/portal_index/",
    "06_control/drive_to_r2/",
)

def required(name):
    value=os.environ.get(name,"").strip()
    if not value: raise RuntimeError(f"Missing {name}")
    return value

def shard(value):
    return hashlib.sha256(value.encode()).hexdigest()[:2]

def compact(value):
    return json.dumps(value,ensure_ascii=False,separators=(",",":"),sort_keys=True).encode()

def folder_id(path):
    return "r2f-"+hashlib.sha256(("folder:"+path).encode()).hexdigest()[:32]

def object_id(bucket,key):
    return "r2o-"+hashlib.sha256((bucket+"\n"+key).encode()).hexdigest()[:40]

def iso(value):
    if value is None: return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

def mime_for(key):
    guessed,_=mimetypes.guess_type(key)
    if guessed: return guessed
    suffix=PurePosixPath(key).suffix.lower()
    return {
      ".parquet":"application/vnd.apache.parquet",
      ".jsonl":"application/x-ndjson",
      ".ndjson":"application/x-ndjson",
      ".gz":"application/gzip",
      ".bin":"application/octet-stream",
    }.get(suffix,"application/octet-stream")

def parent_path(path):
    parent=str(PurePosixPath(path).parent)
    return "" if parent=="." else parent

def add_record(ids,parents,identity,meta,extra=None):
    record={"meta":meta}
    if extra: record.update(extra)
    ids[shard(identity)][identity]=record
    for parent in meta.get("parents",[]):
        parents[shard(parent)].setdefault(parent,[]).append(meta)

def main():
    lakehouse=required("R2_LAKEHOUSE_BUCKET")
    landing=required("R2_LANDING_BUCKET")
    s3=boto3.client(
        "s3",endpoint_url=required("R2_S3_ENDPOINT").rstrip("/"),region_name="auto",
        aws_access_key_id=required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
        config=Config(signature_version="s3v4",retries={"max_attempts":8,"mode":"standard"}),
    )

    live=[]
    folders=set()
    digest=hashlib.sha256()
    source_files=0
    source_bytes=0

    for bucket in (landing,lakehouse):
        paginator=s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for item in page.get("Contents",[]):
                key=str(item["Key"])
                if bucket==lakehouse and any(key.startswith(p) for p in EXCLUDED_LAKEHOUSE_PREFIXES):
                    continue
                if key.endswith("/") and int(item.get("Size",0))==0:
                    parts=[p for p in key.strip("/").split("/") if p]
                    for i in range(1,len(parts)+1): folders.add("/".join(parts[:i]))
                    continue
                parts=[p for p in key.split("/") if p]
                if not parts: continue
                for i in range(1,len(parts)): folders.add("/".join(parts[:i]))
                size=int(item.get("Size",0))
                modified=iso(item.get("LastModified"))
                etag=str(item.get("ETag","")).strip('"')
                live.append((bucket,key,size,modified,etag))
                source_files+=1
                source_bytes+=size
                digest.update(f"{bucket}\0{key}\0{size}\0{modified}\0{etag}\n".encode())

    source_inventory_sha256=digest.hexdigest()
    source_run=source_inventory_sha256[:24]
    prefix=f"06_control/portal_index/v1/{source_run}"

    try:
        current=json.loads(s3.get_object(Bucket=lakehouse,Key=CURRENT_KEY)["Body"].read())
        if (
            current.get("format_version")==1
            and current.get("source_inventory_sha256")==source_inventory_sha256
            and current.get("index_mode")=="live-r2"
        ):
            print(json.dumps({"status":"current","source_run":source_run,"source_files":source_files}))
            return 0
    except Exception:
        pass

    ids={f"{i:02x}":{} for i in range(256)}
    parents={f"{i:02x}":{} for i in range(256)}

    root_meta={
      "id":ROOT_ID,"name":"zohelo-data","mimeType":FOLDER,
      "parents":["root"],"trashed":False,"capabilities":{"canDownload":False},
    }
    add_record(ids,parents,ROOT_ID,root_meta)

    for path in sorted(folders,key=lambda p:(p.count("/"),p)):
        identity=folder_id(path)
        parent=parent_path(path)
        meta={
          "id":identity,
          "name":PurePosixPath(path).name,
          "mimeType":FOLDER,
          "parents":[folder_id(parent) if parent else ROOT_ID],
          "trashed":False,
          "capabilities":{"canDownload":False},
        }
        add_record(ids,parents,identity,meta)

    for bucket,key,size,modified,etag in live:
        path=key.strip("/")
        parent=parent_path(path)
        identity=object_id(bucket,key)
        meta={
          "id":identity,
          "name":PurePosixPath(key).name,
          "mimeType":mime_for(key),
          "size":str(size),
          "modifiedTime":modified,
          "version":etag or None,
          "parents":[folder_id(parent) if parent else ROOT_ID],
          "trashed":False,
          "capabilities":{"canDownload":True},
        }
        meta={k:v for k,v in meta.items() if v is not None}
        add_record(ids,parents,identity,meta,{"r2":{"bucket":bucket,"key":key}})

    for table in parents.values():
        for rows in table.values():
            rows.sort(key=lambda x:(x.get("mimeType")!=FOLDER,str(x.get("name","")).lower(),str(x.get("id",""))))

    uploads=[]
    for part,value in ids.items(): uploads.append((f"{prefix}/ids/{part}.json",compact(value)))
    for part,value in parents.items(): uploads.append((f"{prefix}/parents/{part}.json",compact(value)))

    def put(item):
        key,body=item
        s3.put_object(Bucket=lakehouse,Key=key,Body=body,ContentType="application/json",
                      CacheControl="private, max-age=31536000, immutable")
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(put,uploads))

    pointer={
      "format_version":1,
      "index_mode":"live-r2",
      "generated_at_utc":datetime.now(timezone.utc).isoformat(),
      "source_run":source_run,
      "source_inventory_sha256":source_inventory_sha256,
      "source_root_id":ROOT_ID,
      "source_files":source_files,
      "source_bytes":source_bytes,
      "index_prefix":prefix,
      "id_shards":256,
      "parent_shards":256,
      "excluded_prefixes":list(EXCLUDED_LAKEHOUSE_PREFIXES),
    }
    s3.put_object(Bucket=lakehouse,Key=CURRENT_KEY,Body=compact(pointer),
                  ContentType="application/json",CacheControl="no-store")
    print(json.dumps({"status":"published",**pointer},sort_keys=True))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
