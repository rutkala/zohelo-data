# Unified vectorized execution standard across all pipeline stages

Status: Accepted owner direction, 11 September 2026. Delivery evidence belongs in
[the canonical delivery record](../deliverables.md).

Data processing across all current and future sources, whether bulk archives or
API response envelopes, must use compiled C++ vectorized execution as the default
standard across every medallion stage. Single-threaded interpreted Python loops
(such as `json.loads` iterations over millions of rows) are prohibited for data
conversion, unpacking, flattening, and transformation.

## 1. Engine Standardization

DuckDB is the standardized in-process vectorized execution engine for the Zohelo
platform across all compute boundaries:
- **Ingestion & Bronze Streaming:** Streaming archive extraction, member file
  decompression, JSON/CSV parsing, and initial Bronze Parquet serialization must
  execute via DuckDB's compiled SIMD parsers (`read_json`, `read_csv`, `json_each`)
  or native Arrow pipelines.
- **Silver & Gold Transformation:** All business transformations, deduplication,
  revision history, and dimensional modeling execute in dbt on DuckDB.
- **Serving & Exploration:** The Portal executes DuckDB WASM directly in the
  client browser against remote Parquet files stored on Google Drive.

## 2. Execution Principles

1. **Vectorized Columnar Parsing over Row-by-Row Loops:**
   Never parse bulk JSON or CSV line-by-line in CPython. Temporary streaming chunks
   must be handed directly to DuckDB's native multi-threaded C++ reader, which
   projects schemas, parses nested arrays (`lateral json_each`), and writes
   compressed ZSTD Parquet in a single vectorized pass.
2. **Horizontal Scaling via Actions Matrix:**
   For bulk datasets composed of multiple independent member files or partitions,
   orchestration should use GitHub Actions job matrix parallelism (up to 20
   concurrent runners) to process independent shards in parallel, writing
   directly into Google Drive without local persistent disk footprint.
3. **API vs Bulk Separation:**
   Rate-limited REST APIs (NBP, GUS BDL) remain governed by external provider quotas
   and network latency during Landing extraction. Once raw responses land, their
   expansion into Bronze/Silver/Gold must strictly follow this vectorized standard.
