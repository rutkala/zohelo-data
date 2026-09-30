const FOLDER = "application/vnd.google-apps.folder";
const INDEX_CURRENT = "06_control/portal_index/v1/current.json";
const ORIGINS = new Set([
  "https://data.zohelo.com",
  "https://rutkala.github.io",
  "http://localhost:5173",
  "http://127.0.0.1:5173",
]);

let currentCache = null;
const shardCache = new Map();
const authCache = new Map();

function cors(origin) {
  const h = new Headers({
    "access-control-allow-headers": "Authorization, Content-Type, Range",
    "access-control-allow-methods": "GET, HEAD, OPTIONS",
    "access-control-expose-headers": "Content-Length, Content-Range, ETag, Accept-Ranges",
    "vary": "Origin",
  });
  if (origin && ORIGINS.has(origin)) h.set("access-control-allow-origin", origin);
  return h;
}
function json(value, status, origin) {
  const h = cors(origin);
  h.set("content-type", "application/json; charset=utf-8");
  h.set("cache-control", "no-store");
  return new Response(JSON.stringify(value), { status, headers: h });
}
async function authorize(request, env) {
  const header = request.headers.get("authorization") || "";
  if (!header.startsWith("Bearer ")) return { ok:false, status:401, message:"Sign in required." };
  const token = header.slice(7).trim();
  if (!token) return { ok:false, status:401, message:"Sign in required." };
  const cached = authCache.get(token);
  if (cached && cached.expires > Date.now()) return cached.result;
  const r = await fetch("https://www.googleapis.com/oauth2/v3/userinfo", { headers:{ Authorization:"Bearer "+token } });
  if (!r.ok) {
    const result={ok:false,status:401,message:"Portal authorization expired. Sign in again."};
    authCache.set(token,{expires:Date.now()+15000,result});
    return result;
  }
  const info=await r.json();
  const email=typeof info.email==="string"?info.email.toLowerCase():"";
  const allowed=(env.ALLOWED_GOOGLE_EMAIL||"").trim().toLowerCase();
  const result=email&&allowed&&email===allowed
    ? {ok:true,status:200,message:"ok"}
    : {ok:false,status:403,message:"This Google account is not authorized for the Zohelo data portal."};
  authCache.set(token,{expires:Date.now()+300000,result});
  if (authCache.size>100) authCache.delete(authCache.keys().next().value);
  return result;
}
async function shardName(value) {
  const d=await crypto.subtle.digest("SHA-256",new TextEncoder().encode(value));
  return [...new Uint8Array(d).slice(0,1)].map(x=>x.toString(16).padStart(2,"0")).join("");
}
async function current(env) {
  if (currentCache && currentCache.expires>Date.now()) return currentCache.value;
  const o=await env.LAKEHOUSE.get(INDEX_CURRENT);
  if (!o) throw new Error("R2 portal index pointer is missing.");
  const value=await o.json();
  if (!value || value.format_version!==1 || typeof value.index_prefix!=="string") throw new Error("R2 portal index pointer is invalid.");
  currentCache={expires:Date.now()+60000,value};
  return value;
}
async function shard(env,kind,id) {
  const cfg=await current(env);
  const part=await shardName(id);
  const cacheKey=kind+":"+part+":"+cfg.source_run;
  if (shardCache.has(cacheKey)) return shardCache.get(cacheKey);
  const o=await env.LAKEHOUSE.get(cfg.index_prefix+"/"+kind+"/"+part+".json");
  if (!o) throw new Error("R2 portal index shard is missing.");
  const value=await o.json();
  shardCache.set(cacheKey,value);
  if (shardCache.size>48) shardCache.delete(shardCache.keys().next().value);
  return value;
}
async function byId(env,id) {
  const data=await shard(env,"ids",id);
  return data[id]||null;
}
async function children(env,parentId) {
  const data=await shard(env,"parents",parentId);
  return Array.isArray(data[parentId])?data[parentId]:[];
}
function unescapeQuery(value) {
  return value.replace(/\\'/g,"'").replace(/\\\\/g,"\\");
}
async function listFiles(url,env) {
  const q=url.searchParams.get("q")||"";
  const parentIds=[...q.matchAll(/'((?:\\.|[^'])+)' in parents/g)].map(m=>unescapeQuery(m[1]));
  if (!parentIds.length) return {files:[],incompleteSearch:false};
  const nameMatch=q.match(/name='((?:\\.|[^'])*)'/);
  const wantedName=nameMatch?unescapeQuery(nameMatch[1]):null;
  const wantsFolder=/mimeType\s*=\s*'application\/vnd\.google-apps\.folder'/.test(q);
  const excludesFolder=/mimeType\s*!=\s*'application\/vnd\.google-apps\.folder'/.test(q);
  const seen=new Set();
  const rows=[];
  for (const parentId of parentIds) {
    for (const meta of await children(env,parentId)) {
      if (!meta || seen.has(meta.id)) continue;
      seen.add(meta.id);
      if (wantedName!==null && meta.name!==wantedName) continue;
      if (wantsFolder && meta.mimeType!==FOLDER) continue;
      if (excludesFolder && meta.mimeType===FOLDER) continue;
      rows.push(meta);
    }
  }
  rows.sort((a,b)=>String(a.name).localeCompare(String(b.name))||String(a.id).localeCompare(String(b.id)));
  const pageSize=Math.min(Math.max(Number(url.searchParams.get("pageSize")||"1000"),1),1000);
  const offset=Math.max(Number(url.searchParams.get("pageToken")||"0"),0);
  const page=rows.slice(offset,offset+pageSize);
  const next=offset+pageSize<rows.length?String(offset+pageSize):undefined;
  return {files:page,nextPageToken:next,incompleteSearch:false};
}
async function resolveAddress(env,record) {
  if (!record) return null;
  if (record.r2) return record.r2;
  if (record.alias_target_id) return (await byId(env,record.alias_target_id))?.r2||null;
  return null;
}
function rangeSpec(header,size) {
  if (!header) return null;
  const m=/^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!m) return {invalid:true};
  if (m[1]) {
    const start=Number(m[1]), end=m[2]?Number(m[2]):size-1;
    if (!Number.isSafeInteger(start)||!Number.isSafeInteger(end)||start<0||end<start||start>=size) return {invalid:true};
    const last=Math.min(end,size-1);
    return {offset:start,length:last-start+1};
  }
  const suffix=Number(m[2]);
  if (!Number.isSafeInteger(suffix)||suffix<=0) return {invalid:true};
  const length=Math.min(suffix,size);
  return {offset:size-length,length};
}
async function media(request,env,record,origin) {
  const address=await resolveAddress(env,record);
  if (!address) return json({error:{status:"NOT_FOUND",message:"No R2 object is mapped to this file."}},404,origin);
  const bucket=address.bucket==="zohelo-landing-prod"?env.LANDING:env.LAKEHOUSE;
  const head=await bucket.head(address.key);
  if (!head) return json({error:{status:"NOT_FOUND",message:"Mapped R2 object is missing."}},404,origin);
  const range=rangeSpec(request.headers.get("range"),head.size);
  if (range?.invalid) {
    const h=cors(origin); h.set("content-range","bytes */"+head.size);
    return new Response(null,{status:416,headers:h});
  }
  const object=range?await bucket.get(address.key,{range:{offset:range.offset,length:range.length}}):await bucket.get(address.key);
  if (!object) return json({error:{status:"NOT_FOUND",message:"Mapped R2 object is missing."}},404,origin);
  const h=cors(origin);
  object.writeHttpMetadata(h);
  h.set("etag",object.httpEtag);
  h.set("accept-ranges","bytes");
  if (range) {
    h.set("content-length",String(range.length));
    h.set("content-range","bytes "+range.offset+"-"+(range.offset+range.length-1)+"/"+head.size);
  } else h.set("content-length",String(head.size));
  return new Response(request.method==="HEAD"?null:object.body,{status:range?206:200,headers:h});
}
export default {
  async fetch(request,env) {
    const origin=request.headers.get("origin")||"";
    if (origin && !ORIGINS.has(origin)) return new Response("Origin not allowed",{status:403});
    if (request.method==="OPTIONS") return new Response(null,{status:204,headers:cors(origin)});
    if (!["GET","HEAD"].includes(request.method)) return json({error:{status:"METHOD_NOT_ALLOWED"}},405,origin);
    const auth=await authorize(request,env);
    if (!auth.ok) return json({error:{status:auth.status===401?"UNAUTHENTICATED":"PERMISSION_DENIED",message:auth.message}},auth.status,origin);
    try {
      const url=new URL(request.url);
      if (url.pathname==="/health") return json({ok:true,storage:"cloudflare-r2"},200,origin);
      if (url.pathname==="/drive/v3/files") return json(await listFiles(url,env),200,origin);
      const match=/^\/drive\/v3\/files\/([^/]+)$/.exec(url.pathname);
      if (!match) return json({error:{status:"NOT_FOUND"}},404,origin);
      const id=decodeURIComponent(match[1]);
      const record=await byId(env,id);
      if (!record) return json({error:{status:"NOT_FOUND",message:"File ID is not present in the R2 migration index."}},404,origin);
      if (url.searchParams.get("alt")==="media") return media(request,env,record,origin);
      return json(record.meta,200,origin);
    } catch (error) {
      return json({error:{status:"INTERNAL",message:error instanceof Error?error.message:String(error)}},500,origin);
    }
  }
};
