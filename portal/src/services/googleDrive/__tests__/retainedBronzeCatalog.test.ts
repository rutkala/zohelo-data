import { describe, expect, it } from "vitest";
import { parseRetainedBronzeManifest, parseRetainedIndicatorIndex } from "../landingCatalog";
import { landingDatasets } from "@/store/slices/googleDriveSlice";
import type { LandingSnapshotPointer, RetainedBronzeManifest } from "../types";

const pairs = (values: string[][]) => values.map(([name, type]) => ({ name, type }));
const observations = pairs([
  ["indicator_id","BIGINT"],["przekroj_id","BIGINT"],
  ...Array.from({length:9},(_,i)=>[[`wymiar_${i+1}`,"BIGINT"],[`pozycja_${i+1}`,"BIGINT"]]).flat(),
  ["okres_id","INTEGER"],["sposob_prezentacji_miara_id","INTEGER"],["period_year","INTEGER"],
  ["wartosc_raw","VARCHAR"],["wartosc_numeric","DOUBLE"],["precyzja","INTEGER"],
  ["brak_wartosci_id","INTEGER"],["tajnosci_id","INTEGER"],["flaga_id","INTEGER"],
  ["raw_archive_file","VARCHAR"],["source_row_number","BIGINT"],["processed_at_utc","VARCHAR"],
]);
const schemas = {
  observations,
  dictionaries:pairs([["indicator_id","BIGINT"],["column_name","VARCHAR"],["dictionary_name","VARCHAR"],["element_id","BIGINT"],["element_name","VARCHAR"],["processed_at_utc","VARCHAR"]]),
  metadata:pairs([["indicator_id","BIGINT"],["metric_name","VARCHAR"],["metric_name_en","VARCHAR"],["description","VARCHAR"],["frequency","VARCHAR"],["measure_unit","VARCHAR"],["data_source","VARCHAR"],["legal_basis","VARCHAR"],["last_update","VARCHAR"],["processed_at_utc","VARCHAR"]]),
  taxonomy:pairs([["indicator_id","BIGINT"],["indicator_name","VARCHAR"],["indicator_name_en","VARCHAR"],["thematic_area","VARCHAR"],["domain","VARCHAR"],["taxonomy_path","VARCHAR"],["node_id","VARCHAR"],["parent_id","VARCHAR"],["processed_at_utc","VARCHAR"]]),
};
const part=(id:string,n=1,size=100)=>({id,name:`fragment-123e4567-e89b-42d3-a456-42661417${id.padStart(4,"0")}.parquet`,size,sha256:id.padStart(64,"a").slice(-64),row_count:1,part:n});
const file=(id:string,rows=1)=>{const value=part(id);return {id:value.id,name:value.name,size:value.size,sha256:value.sha256,row_count:rows};};
const pointer:LandingSnapshotPointer={format_version:1,source_id:"gus_dbw_retained_bronze",snapshot_id:"123e4567-e89b-42d3-a456-426614174000",manifest_file_id:"manifest-id",manifest_file_name:"manifest-123e4567-e89b-42d3-a456-426614174000.json",manifest_sha256:"a".repeat(64),manifest_size_bytes:100};

function fixture(){
  const raw={format_version:1,kind:"retained_bronze_snapshot",source_id:pointer.source_id,snapshot_id:pointer.snapshot_id,
    created_at_utc:"2026-09-21T00:00:00Z",code_sha:"b".repeat(40),status:"validated",layer:"02_bronze",
    coverage_status:"incomplete_retained_inventory",lineage_status:"unresolved_native_to_bronze",
    inventory_sha256:"c".repeat(64),audit_report_sha256:"d".repeat(64),audit_run_id:"run",
    indicator_count:1550,published_indicator_count:1,pending_indicator_count:1549,
    indicator_index:{id:"index-id",name:"fragment-223e4567-e89b-42d3-a456-426614174000.json",size:100,sha256:"e".repeat(64)},
    datasets:[{name:"observations",table_name:"br_dbw_observations",row_count:1,columns:observations,files:[]},
      ...(["dictionaries","metadata","taxonomy"] as const).map((name,i)=>({name,table_name:`br_dbw_${name==="taxonomy"?"indicators":name}`,row_count:name==="taxonomy"?1550:1,columns:schemas[name],files:[file(String(i+1),name==="taxonomy"?1550:1)]}))],
    observation_schema:observations,tests:{passed:true,rows_and_schemas_preserved:true}};
  const index={format_version:1,kind:"retained_bronze_indicator_index",source_id:pointer.source_id,
    inventory_sha256:"c".repeat(64),indicator_count:1550,published_indicator_count:1,pending_indicator_count:1549,
    indicators:Array.from({length:1550},(_,i)=>({indicator_id:i+1,indicator_name:`Wskaźnik ${i+1}`,
      indicator_name_en:`Indicator ${i+1}`,thematic_area:"Area",domain:"Domain",taxonomy_path:"Area > Domain",
      status:i===0?"published":"pending",row_count:i===0?1:0,parts:i===0?[part("9")]:[]}))};
  const manifest=parseRetainedBronzeManifest(raw,pointer) as RetainedBronzeManifest;
  return {manifest,index,raw};
}

describe("retained DBW Bronze catalogue",()=>{
  it("accepts logical content-addressed publication names",()=>{
    const {raw,index}=fixture();
    raw.indicator_index.name=`fragment-indicator-index-${"e".repeat(64)}.json`;
    raw.datasets.slice(1).forEach((dataset,i)=>{
      dataset.files[0].name=`fragment-${dataset.name}-1-${String(i+1).repeat(64).slice(0,64)}.parquet`;
    });
    index.indicators[0].parts[0].name=`fragment-observations-1-1-${"9".repeat(64)}.parquet`;
    const manifest=parseRetainedBronzeManifest(raw,pointer) as RetainedBronzeManifest;
    expect(parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(index)),manifest)).toHaveLength(1550);
  });

  it("validates all indicators and exposes only selected files",()=>{
    const {manifest,index}=fixture();
    manifest.indicators=parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(index)),manifest);
    const catalog=landingDatasets({snapshots:[{pointer,manifest,fingerprint:"f"}],issues:[],fingerprint:"f"});
    expect(manifest.indicators).toHaveLength(1550);
    expect(catalog.find((d)=>d.table_name==="br_dbw_observations__indicator_1")?.files.map(f=>f.id)).toEqual(["9"]);
    expect(catalog.find((d)=>d.table_name==="br_dbw_observations__indicator_2")?.files).toEqual([]);
    expect(catalog.find((d)=>d.table_name==="br_dbw_observations")?.files).toEqual([]);
  });

  it("rejects inconsistent counts, reused IDs, and schema drift",()=>{
    const {manifest,index}=fixture();
    index.published_indicator_count=2;
    expect(()=>parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(index)),manifest)).toThrow(/does not match|totals/);
    const duplicate=fixture(); duplicate.index.indicators[0].parts[0].id="1";
    expect(()=>parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(duplicate.index)),duplicate.manifest)).toThrow(/reuses/);
    const raw=structuredClone(fixture().raw);
    raw.observation_schema=[...raw.observation_schema]; raw.observation_schema[0]={name:"wrong",type:"BIGINT"};
    expect(()=>parseRetainedBronzeManifest(raw,pointer)).toThrow(/schema/);
  });

  it("splits a selection over 64 MiB into explicit part relations",()=>{
    const {manifest,index}=fixture();
    index.indicators[0].row_count=9; index.indicators[0].parts=Array.from({length:9},(_,i)=>part(String(100+i),i+1,8*1024*1024));
    manifest.row_count=9; manifest.datasets.find(d=>d.dataset_id==="observations")!.row_count=9;
    manifest.indicators=parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(index)),manifest);
    const catalog=landingDatasets({snapshots:[{pointer,manifest,fingerprint:"f"}],issues:[],fingerprint:"f"});
    expect(catalog.filter(d=>d.table_name.startsWith("br_dbw_observations__indicator_1__part_"))).toHaveLength(9);
    expect(catalog.some(d=>d.table_name==="br_dbw_observations__indicator_1")).toBe(false);
  });
  it("v2 references original Bronze IDs and names without requiring publication copies",()=>{
    const {raw,index}=fixture();
    raw.format_version=2;
    index.format_version=2;
    raw.datasets.slice(1).forEach(dataset=>{
      dataset.files[0].name=`${dataset.table_name}.parquet`;
    });
    index.indicators[0].parts[0].name="part_1.parquet";
    const manifest=parseRetainedBronzeManifest(raw,pointer) as RetainedBronzeManifest;
    manifest.indicators=parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(index)),manifest);
    const catalog=landingDatasets({snapshots:[{pointer,manifest,fingerprint:"v2"}],issues:[],fingerprint:"v2"});
    expect(catalog.find(d=>d.table_name==="br_dbw_observations__indicator_1")?.files[0].name).toBe("part_1.parquet");
    expect(catalog.find(d=>d.table_name==="br_dbw_metadata")?.files[0].name).toBe("br_dbw_metadata.parquet");
    expect(manifest.format_version).toBe(2);
  });

  it("original-file references are v2-only and bound to the right indicator",()=>{
    const {raw,index}=fixture();
    index.indicators[0].parts[0].name="part_1.parquet";
    const legacy=parseRetainedBronzeManifest(raw,pointer) as RetainedBronzeManifest;
    expect(()=>parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(index)),legacy)).toThrow();
    raw.format_version=2; index.format_version=2;
    const manifest=parseRetainedBronzeManifest(raw,pointer) as RetainedBronzeManifest;
    index.indicators[0].parts[0].name="part_2.parquet";
    expect(()=>parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(index)),manifest)).toThrow();
    index.indicators[0].parts[0].name="part_1.parquet";
    index.indicators[0].parts[0].size=8*1024*1024+1;
    expect(()=>parseRetainedIndicatorIndex(new TextEncoder().encode(JSON.stringify(index)),manifest)).toThrow();
    raw.datasets[1].files[0].name="br_dbw_metadata.parquet";
    expect(()=>parseRetainedBronzeManifest(raw,pointer)).toThrow();
  });

});
