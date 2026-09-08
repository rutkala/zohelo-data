# Working together on Zohelo-data

`zohelo` is the ChatGPT umbrella project. This repository covers only `zohelo-data` and `data.zohelo.com`; other subprojects need separate scopes.

| Information | Home | Working rule |
| --- | --- | --- |
| Deliverables, status, questions, holds, owner decisions | [docs/deliverables.md](deliverables.md) | The assistant maintains it after verified work and records answers from chat. Read it first when resuming. |
| Discussion and screenshots | Focused chats in the `zohelo` project | One distinct outcome per chat; link the repository and delivery record. |
| Implementation/review | GitHub branch and pull request | Record changed behavior, checks and limits. Merged, deployed and data-verified are different states. |
| Operations/failures | GitHub Actions | Run summaries establish what actually happened. A chat promise is not a running job. |
| Current instructions | Versioned repository docs | Must work for a human with standard tools and no AI. Mark old evidence historical. |
| Data and release artifacts | Existing Drive protocol | Keep data, manifests and checksums together; chat attachments are not production storage. |
| Source research/definitions | Repository research and dbt YAML | Research methodology first; ask the owner about use cases official docs cannot settle. |

ChatGPT Projects organize related chats, files and instructions. Official guidance recommends separate chats for distinct outcomes. This does not guarantee a future chat sees the latest remote repository; refresh the GitHub record at task start. [Projects and chats](https://learn.chatgpt.com/docs/projects).

A sufficient new-chat handoff is:

> Work on rutkala/zohelo-data only. Read AGENTS.md and docs/deliverables.md from current main, check linked evidence and active Actions, then continue the named unfinished item. Keep status/questions in that file. Project management does not belong in the data portal.

Shared repository instructions help different coding tools apply consistent rules. [AGENTS.md guidance](https://learn.chatgpt.com/docs/agent-configuration/agents-md).

The owner's initial goal and boundaries authorize autonomous delivery within them. The assistant owns the complete goal and its necessary deliverables, including routine implementation, review, merge, deployment and production verification. It asks for input only when a consequential business choice, missing credential or boundary change is required; it does not silently reduce scope or present a partial implementation as the requested outcome.

**Done** means the user's outcome has been validated, changes are merged and deployed where applicable, and claimed source coverage is supported by actual collection and measured coverage. A branch, passing tests, a portal build, a sample response or a running backfill can be progress without being Done. At each intermediate handoff, update the single delivery record with verified progress, remaining work, and either the active external continuation or the concrete blocker. Distinguish merged code, deployed portal, verified data releases, externally running jobs and explicitly held work; do not imply work continues after the chat unless a specific job is running.

Every solution, including the portal, is improved from evidence about use, correctness, coverage, failures and cost. Reprioritize remaining authorized work toward usefulness and owner satisfaction while preserving operation by a human without AI, cost and secret controls, data reuse conditions, and serialized production writes. Record status and priority changes only in [docs/deliverables.md](deliverables.md). This applies while an authorized goal remains open and does not promise perpetual background work.

GitHub and Drive cover the present durable workflow. Extra task boards, email copies and portal forms would duplicate status without a demonstrated need. No additional paid service or connector is required now.

The owner can use GitHub Actions notifications for run failures; notification preferences are account settings. On 8 September 2026, the enabled daily ChatGPT task **Advance Zohelo-data delivery** was created to inspect real progress, investigate failures/stalls and advance the highest-priority feasible unfinished work within these boundaries. The half-hour Actions ingestion and the daily review are separate: Actions runs the pipelines without AI; the review provides periodic engineering follow-through. A configured review is not proof that its next execution has happened. Email/messages to others still need a recipient and authorization.

Source onboarding is already authorized under ADRs 0004–0006 and does not wait for a new source-selection discussion. For each selected product, delivery covers its available datasets, dimensions, geographies, history and metadata; large scopes advance in resumable batches toward catalogue and page exhaustion. Contracts and acceptance examples are intermediate deliverables, and status remains in the existing delivery record rather than a new project-management system.
