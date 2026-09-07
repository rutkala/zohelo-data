# zohelo-data

Data Platform for zohelo.com

[Open the data portal](https://data.zohelo.com/) · [About](https://data.zohelo.com/about.html) · [Privacy](https://data.zohelo.com/privacy.html) · [Terms](https://data.zohelo.com/terms.html)

[Data catalogue](https://data.zohelo.com/?view=catalog) — Connected release data, source status, lineage, and published metric definitions. No metric definitions are published yet.

[Portal guide](docs/using-the-portal.md) — Connect to the release and run SQL, including joins, unions, and CTEs.

[Delivery plan and questions](docs/deliverables.md) — The single current record for project status, open owner questions, approved scope, and evidence. Reply in chat with a question ID; the assistant maintains this page.

[NBP platform operations](docs/nbp-platform-operations.md) · [Foundation audit](docs/audits/2026-09-06-foundation.md) · [Verified v2 release evidence](docs/releases/2026-09-07-nbp-platform.md) · [Development setup](docs/development.md) · [Google authorization](docs/google-authorization.md) · [Architecture proposal](docs/architecture.md) · [Agent instructions](AGENTS.md)

The NBP v2 data release is live as of 7 September 2026 with 15 physical tables, source and lineage metadata, and gold tables. Governed metrics remain unimplemented. The portal provides one release-bound dbt Data catalogue and automatically loads published tables referenced by schema-qualified SELECT queries, subject to the browser download limit. [Project status and open questions](docs/deliverables.md) are maintained in one place outside the portal.
