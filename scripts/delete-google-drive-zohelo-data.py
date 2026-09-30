#!/usr/bin/env python3
import json, os, sys
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

ROOT_ID = "1b9ucISOOUXQd6Ku-6qp6g373w9HJ2WOf"

def req(name):
    v=os.environ.get(name,"").strip()
    if not v:
        raise RuntimeError(f"missing {name}")
    return v

creds=Credentials(
    token=None,
    refresh_token=req("GOOGLE_OAUTH_REFRESH_TOKEN"),
    token_uri="https://oauth2.googleapis.com/token",
    client_id=req("GOOGLE_OAUTH_CLIENT_ID"),
    client_secret=req("GOOGLE_OAUTH_CLIENT_SECRET"),
    scopes=["https://www.googleapis.com/auth/drive"],
)
svc=build("drive","v3",credentials=creds,cache_discovery=False)
try:
    meta=svc.files().get(fileId=ROOT_ID,fields="id,name,mimeType,trashed,parents").execute()
except HttpError as exc:
    if getattr(exc.resp,"status",None)==404:
        print(json.dumps({"result":"already_absent","id":ROOT_ID}))
        raise SystemExit(0)
    raise
if meta.get("name")!="zohelo-data" or meta.get("mimeType")!="application/vnd.google-apps.folder":
    raise RuntimeError(f"refusing to delete unexpected object: {meta}")
svc.files().delete(fileId=ROOT_ID,supportsAllDrives=True).execute()
try:
    svc.files().get(fileId=ROOT_ID,fields="id").execute()
except HttpError as exc:
    if getattr(exc.resp,"status",None)==404:
        print(json.dumps({"result":"deleted","id":ROOT_ID,"name":"zohelo-data"}))
        raise SystemExit(0)
    raise
raise RuntimeError("Drive root still exists after delete")
