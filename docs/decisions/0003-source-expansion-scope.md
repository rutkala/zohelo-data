# Source expansion: global context and detailed Poland coverage

Status: Accepted owner requirements, 8 September 2026. Specific sources and technical
implementation choices remain subject to review.

## Context

The owner develops Zohelo-data from Poland. Products built on this data may serve a
worldwide audience, with Polish and English as the primary product languages. The next
step is broad source research before connection testing and onboarding.

## Accepted requirements

- Research multicountry statistical aggregators, international domain organizations,
  commercial aggregators/platforms, public corporate disclosures and government sources.
  For Poland, pursue broad domain coverage and the finest useful published granularity,
  including national, ministerial, agency, registry, local and commercial sources.
- Overlapping information is welcome. Preserve source identity, provenance and methodology;
  do not discard an independent source merely because another carries similar observations.
- Keep Zohelo-data documentation and descriptive metadata in English. Preserve original
  source names, codes and native labels alongside English descriptions where needed for
  identity and traceability. Downstream products may support Polish and English.
- Cover APIs, files, databases, web services and document publications. Assess bulk,
  partial, incremental and history capabilities separately for each concrete dataset.
- Design onboarding so recent collection can advance from its start boundary while
  available history is filled in the background. Historical work must not monopolize
  resources needed for fresh data. Respect shared provider quotas and source limitations.
- Plan an explicit silver, gold and semantic role for every admitted source. Preserve
  actual grains and connect compatible entities and dimensions. Source documents and
  reference data need appropriate representations rather than invented numerical metrics.
- Work in stages: research, owner review, bounded connection tests, detailed loading and
  modeling design, then implementation and acceptance.

## Consequences

The research inventory is broader than the initial production intake. Paid, restricted and
uncertain products remain visible as conditional candidates. The established initial intake
criteria—free/public access and permission for the intended commercial use—still apply;
researching a product does not authorize a subscription or establish redistribution rights.

The [research package](../source-research/README.md) proposes a multidimensional taxonomy,
loading patterns, modeling principles and review batches. Those proposals are not approved
source selections, implemented infrastructure, production availability promises or executable
metric definitions. The existing NBP contracts and schedules remain in force.

The [delivery record](../deliverables.md) remains the single project-status and owner-question
record. Research worksheets support discussion; they do not introduce a second task board
or review interface in the portal.
