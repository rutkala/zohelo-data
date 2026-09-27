# Landing

`01_landing` contains immutable, verified source responses and their ingestion provenance. dbt reads this layer through source declarations and does not create or replace Landing objects. Python ingestion owns extraction, byte-for-byte retention, validation envelopes and transfer into this boundary.

The current NBP dbt source is `landing.nbp_batches`. Its local external relation is selected with `ZOHELO_NBP_BATCHES_PATH`; production publication builds supply the verified JSONL envelope assembled from retained raw responses.
