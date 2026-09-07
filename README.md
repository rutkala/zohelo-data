# zohelo-data
Data Platform for zohelo.com

[Open the data portal](https://data.zohelo.com/).

[About Zohelo-data](https://data.zohelo.com/about.html) · [Privacy policy](https://data.zohelo.com/privacy.html) · [Terms of use](https://data.zohelo.com/terms.html).

[Delivery plan](docs/deliverables.md) — Approved scope for completing NBP, governed metrics and the business data catalogue.

[NBP platform operations](docs/nbp-platform-operations.md) — Feature-branch entrypoint, workflow modes, release guard, bounds and current proof status.

[Foundation audit](docs/audits/2026-09-06-foundation.md) — Evidence, completed fixes and remaining release blockers.

[Development setup](docs/development.md) — Codespaces, consistent runtimes and credential-free validation.

[Google authorization](docs/google-authorization.md) — Which credentials each component needs, diagnostic results and account checks.

[Architecture proposal](docs/architecture.md) — Proposed design, open decisions, and implementation milestones.

[Agent instructions](AGENTS.md) — Shared setup, validation, and contribution guidance for coding agents.

The feature branch contains the consolidated [`NBP data platform`](.github/workflows/daily-ingestion.yml) workflow and `python src/nbp_platform.py --mode ...` entrypoint. The current live consumer remains on the validated v1 silver release until a v2 platform run is proven and explicitly confirmed.
