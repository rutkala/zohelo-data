# zohelo-data
Data Platform for zohelo.com

[Open the data portal](https://data.zohelo.com/).

[About Zohelo-data](https://data.zohelo.com/about.html) · [Privacy policy](https://data.zohelo.com/privacy.html) · [Terms of use](https://data.zohelo.com/terms.html).

[Delivery plan](docs/deliverables.md) — Approved scope for completing NBP, governed metrics and the business data catalogue.

[NBP platform operations](docs/nbp-platform-operations.md) — Merged entrypoint, workflow modes, release guard, bounds and current proof status.

[Foundation audit](docs/audits/2026-09-06-foundation.md) — Evidence, completed fixes and remaining release blockers.

[Development setup](docs/development.md) — Codespaces, consistent runtimes and credential-free validation.

[Google authorization](docs/google-authorization.md) — Which credentials each component needs, diagnostic results and account checks.

[Architecture proposal](docs/architecture.md) — Proposed design, open decisions, and implementation milestones.

[Agent instructions](AGENTS.md) — Shared setup, validation, and contribution guidance for coding agents.

PR57 merged the consolidated [`NBP data platform`](.github/workflows/daily-ingestion.yml) workflow and `python src/nbp_platform.py --mode ...` entrypoint on `main`. Data and portal checks passed; live bootstrap remains in progress, and the current live consumer remains on the validated v1 silver release until v2 coverage and publication are proven and explicitly confirmed.
