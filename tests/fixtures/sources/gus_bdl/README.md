# GUS BDL source fixtures

These small synthetic payloads reproduce the field names and envelopes checked
against the official BDL v1 OpenAPI document and bounded public examples. Numeric
observations are test values, not a retained production extract. Live acceptance
must retain its own exact response bytes and receipt. The data fixture intentionally
omits `page` and `pageSize`, matching the successful by-variable production envelope
that exposed the response-pagination compatibility case.
