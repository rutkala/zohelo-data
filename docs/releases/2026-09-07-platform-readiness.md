# Platform audit delivery — 7 September 2026

## Delivered code and portal

[PR #66](https://github.com/rutkala/zohelo-data/pull/66) is merged as
`2bfca0b78b6bc0bc3da1586856f38d34f3af49d9`. The merged tree
`c99dbce0cd6f8d37ad48ca7c1a5bd8d5dbb0f603` matches the reviewed PR head
`27cc957fe5183775475fb23f25249eb9c26a6841` exactly.

| Verification | Result / evidence |
| --- | --- |
| Python, data and real dbt/MetricFlow fixtures | **138 tests passed**; genuine dbt model/test builds, native daily metric queries, invalid-grain rejection, staged publication and portable restore. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165196539). |
| Portal production builds, lint and unit/engine tests | Both deployment layouts passed; **678 tests per configuration**. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165196537). |
| Browser verification | **14 flows per deployment layout**, including real dbt metric search, metadata, semantic-to-physical lineage, SQL editing/views and matching light/dark desktop/mobile colours. Same portal run above; screenshots were also inspected. |
| Fresh development container | Passed the actual environment build and startup/fixture checks. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165196579). |
| Portal deployment | Build and Pages deployment succeeded for the merged commit. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165675576). |
| Google connection | Read-only access check passed after merge. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165675596). |
| Live audit NBP publication | **Not run.** The merge omitted the publication marker; the data workflow was skipped. [Run](https://github.com/rutkala/zohelo-data/actions/runs/34165675555). |

GitHub confirms deployment. Direct public-site retrieval of the portal and
`portal-build.json` was blocked by this session's web-access service, so this
record does not claim a separate public-site HTTP check succeeded.

The first browser attempt caught a theme-transition timing issue in the new
test. The next caught a selector treating native search cards as anchors. Both
were corrected before merge; the checks on the final PR commit above passed. The
independent recovery review also fixed a real blocker: damage to current data
files no longer prevents promotion of a healthy retained release. Current
manifest integrity and full target verification remain mandatory. Recovery
failure tests use isolated stores; no live rollback drill was performed.

## Production acceptance is on hold

Automatic approval review rejected merging with an explicit live-publication
trigger. Its stated reason was that the audit/implementation instruction did
not explicitly authorize that production-data write. A subsequent merge without
the publication trigger was accepted. This is an approval hold, not a failing
data test or an unanswered source-methodology question.

The normal 02:00 UTC schedule is temporarily paused under **H-LIVE** so that it
cannot perform the rejected operation automatically before approval. Existing
published files are retained. No production data was deleted or rolled back.

The concrete remaining action is to restore the schedule and run the reviewed
NBP workflow: ingest/recheck source responses, build and validate a new release,
publish it to Drive, switch the current release reference after candidate
validation, then run fresh SQL and native MetricFlow checks, exact raw replay,
and the read-only capacity/storage inventory. Prior releases remain retained.

The five definitions are `nbp_table_a_mid`, `nbp_table_b_mid`, `nbp_table_c_bid`,
`nbp_table_c_ask`, and `nbp_gold_price_pln_per_gram_1000`. They are implemented and
tested at their daily source grain. Their first live release and the fresh
consumer's actual query results remain unverified until H-LIVE is cleared.

Previous verified live release evidence remains
[`84784104-e014-4550-bb0c-095d967230b5`](2026-09-07-medallion-portal.md):
15 tables and 429,795 cleaned NBP observations. Those historical results are not
substitutes for running the new semantic release.

## Current project record

[Deliverables and owner decisions](../deliverables.md) distinguish completed
audit/code/portal work from H-LIVE and the longer-term scope holds. The
[architecture](../architecture.md), [workflow audit](../audits/2026-09-07-workflows.md),
[source definitions](../nbp-business-definitions.md),
[operating guide](../nbp-platform-operations.md) and
[collaboration guide](../collaboration.md) are the supporting records.

No additional source or paid service was introduced. Commercial data reuse,
commercial hosting, automatic retention compaction and always-on semantic
serving have explicit reasons and reopening conditions in the delivery record.
