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

During work, report the concrete result under test and real blockers. At handoff, distinguish merged code, deployed portal, verified data release, externally running jobs and explicitly held work. Do not imply coding continues after the chat has ended.

GitHub and Drive cover the present durable workflow. Extra task boards, email copies and portal forms would duplicate status without a demonstrated need. No additional paid service or connector is required now.

The owner can use GitHub Actions notifications for run failures; notification preferences are account settings. No recurring AI monitoring job is claimed to be active. A separately requested automation needs a defined schedule/condition and actual setup; email/messages need a recipient and authorization.

The next-source discussion begins after critical current items are resolved or visibly held/cancelled with a reason and reopening condition in the delivery record. Its output is a source contract and acceptance example, not a new project-management system.
