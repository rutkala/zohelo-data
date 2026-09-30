#!/usr/bin/env python3
"""Publish a compact sharded read-only portal index from the completed Drive-to-R2 migration."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip, hashlib, json, os
import boto3

MIGRATION_PREFIX="06_control/drive_to_r2/rclone/"
CURRENT_KEY="06_control/portal_index/v1/current.json"
FOLDER="application/vnd.google-apps.folder"

def required(name):
    value=os.environ.get(name,"").strip()
    if not value: raise RuntimeError(f"Missing {name}")
    return value

def shard(value):
    return hashlib.sha256(value.encode()).hexdigest()[:2]

def compact(value):
    return json.dumps(value,ensure_ascii=False,separators=(",",":"),sort_keys=True).encode()

def main():
    bucket=required("R2_LAKEHOUSE_BUCKET")
    s3=boto3.client("s3",endpoint_url=required("R2_S3_ENDPOINT").rstrip("/"),region_name="auto",
        aws_access_key_id=required("CLOUDFLARE_R2_ACCESS_KEY_ID"),
        aws_secret_access_key=required("CLOUDFLARE_R2_SECRET_ACCESS_KEY"))
    candidates=[]
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket,Prefix=MIGRATION_PREFIX):
        for item in page.get("Contents",[]):
            key=item["Key"]
            if not key.endswith("/inventory-and-map.json.gz"): continue
            segment=key[len(MIGRATION_PREFIX):].split("/",1)[0]
            try: run_id=int(segment.split("-",1)[0])
            except ValueError: continue
            candidates.append((run_id,segment,key))
    if not candidates: raise RuntimeError("No Drive-to-R2 inventory found")
    _,source_run,inventory_key=max(candidates)
    completed_key=f"{MIGRATION_PREFIX}{source_run}/completed.json"
    completed=json.loads(s3.get_object(Bucket=bucket,Key=completed_key)["Body"].read())
    if completed.get("result")!="copy_completed" or completed.get("errors")!=0:
        raise RuntimeError("Latest migration is not a completed zero-error copy")
    try:
        cur=json.loads(s3.get_object(Bucket=bucket,Key=CURRENT_KEY)["Body"].read())
        if cur.get("source_run")==source_run and cur.get("format_version")==1:
            print(json.dumps({"status":"current","source_run":source_run})); return 0
    except Exception:
        pass
    migration=json.loads(gzip.decompress(s3.get_object(Bucket=bucket,Key=inventory_key)["Body"].read()))
    inventory=migration["inventory"]; mapping=migration["r2"]
    objects=mapping.get("objects",{}); aliases=mapping.get("aliases",{})
    ids={f"{i:02x}":{} for i in range(256)}
    parents={f"{i:02x}":{} for i in range(256)}
    keep=("id","name","mimeType","size","createdTime","modifiedTime","version","md5Checksum","sha256Checksum","shortcutDetails")
    for node in inventory["nodes"]:
        identity=node["id"]; parent=node.get("parent_id")
        meta={k:node[k] for k in keep if k in node and node[k] is not None}
        meta["parents"]=[parent] if parent else []
        meta["trashed"]=False
        meta["capabilities"]={"canDownload":node.get("mimeType")!=FOLDER}
        record={"meta":meta}
        if identity in objects:
            a=objects[identity]; record["r2"]={"bucket":a["bucket"],"key":a["key"]}
        elif identity in aliases and aliases[identity].get("targetId"):
            record["alias_target_id"]=aliases[identity]["targetId"]
        ids[shard(identity)][identity]=record
        if parent: parents[shard(parent)].setdefault(parent,[]).append(meta)
    for table in parents.values():
        for rows in table.values(): rows.sort(key=lambda x:(str(x.get("name","")),str(x.get("id",""))))
    prefix=f"06_control/portal_index/v1/{source_run}"
    uploads=[]
    for part,value in ids.items(): uploads.append((f"{prefix}/ids/{part}.json",compact(value)))
    for part,value in parents.items(): uploads.append((f"{prefix}/parents/{part}.json",compact(value)))
    def put(item):
        key,body=item
        s3.put_object(Bucket=bucket,Key=key,Body=body,ContentType="application/json",
                      CacheControl="private, max-age=31536000, immutable")
    with ThreadPoolExecutor(max_workers=16) as pool: list(pool.map(put,uploads))
    pointer={"format_version":1,"generated_at_utc":datetime.now(timezone.utc).isoformat(),
      "source_run":source_run,"source_inventory_sha256":inventory.get("inventory_sha256"),
      "source_root_id":inventory.get("root_id"),"source_files":completed.get("source_files"),
      "source_bytes":completed.get("source_bytes"),"index_prefix":prefix,
      "id_shards":256,"parent_shards":256}
    s3.put_object(Bucket=bucket,Key=CURRENT_KEY,Body=compact(pointer),
                  ContentType="application/json",CacheControl="no-store")
    print(json.dumps({"status":"published",**pointer},sort_keys=True))
    return 0

if __name__=="__main__":
    raise SystemExit(main())
