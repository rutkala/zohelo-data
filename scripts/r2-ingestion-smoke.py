#!/usr/bin/env python3
"""Manual R2 ingestion smoke: fetch a live public source, write/read/delete in Landing."""
from datetime import datetime, timezone
import hashlib, json, os, urllib.request
import boto3
from botocore.config import Config

def req(n):
    v=os.environ.get(n,"").strip()
    if not v: raise RuntimeError(f"missing {n}")
    return v

url="https://api.nbp.pl/api/exchangerates/tables/A/?format=json"
with urllib.request.urlopen(url, timeout=30) as r:
    body=r.read()
if not body or not body.lstrip().startswith(b"["):
    raise RuntimeError("NBP response is not expected JSON")
payload=json.loads(body)
if not isinstance(payload,list) or not payload:
    raise RuntimeError("NBP response is empty")
sha=hashlib.sha256(body).hexdigest()
ts=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
key=f"01_landing/_smoke/nbp/{ts}-table-a.json"
bucket=req("R2_LANDING_BUCKET")
s3=boto3.client(
    "s3",endpoint_url=req("R2_S3_ENDPOINT"),region_name="auto",
    aws_access_key_id=req("CLOUDFLARE_R2_ACCESS_KEY_ID"),
    aws_secret_access_key=req("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4",retries={"max_attempts":6,"mode":"standard"})
)
s3.put_object(Bucket=bucket,Key=key,Body=body,ContentType="application/json",
              Metadata={"sha256":sha,"source":"nbp-api-r2-smoke"})
read=s3.get_object(Bucket=bucket,Key=key)["Body"].read()
if hashlib.sha256(read).hexdigest()!=sha:
    raise RuntimeError("R2 smoke checksum mismatch")
s3.delete_object(Bucket=bucket,Key=key)
try:
    s3.head_object(Bucket=bucket,Key=key)
except Exception:
    pass
else:
    raise RuntimeError("R2 smoke object still exists after cleanup")
print(json.dumps({"result":"pass","source":"NBP table A","bucket":bucket,"bytes":len(body),"sha256":sha}))
