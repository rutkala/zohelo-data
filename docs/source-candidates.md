# Candidate data sources

The current [global and Polish research package](source-research/README.md), dated
8 September 2026, covers 182 products/families with access, rights, loading, modeling,
overlap and evidence fields. Start there for the expanded landscape and proposed review
sequence. New connection testing and onboarding have not started.

## Initial Eurostat and WDI assessment

**Historical review date:** 2026-09-07. The two-source assessment below preceded the
expanded research. Neither source was approved, configured, ingested or prioritized in
that assessment; subsequent review recommendations are in the current research package.

| Candidate | Official access | Reuse evidence and attribution | Exceptions / limits to resolve before use |
| --- | --- | --- |
| [Eurostat statistics](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access) | Eurostat documents Statistics, SDMX and catalogue APIs. No API key requirement or numeric request-rate ceiling was identified in the reviewed guide. Use bounded queries, caching and backoff; validate practical limits with the selected endpoint. | Eurostat authorises commercial and non-commercial reuse of its published statistical data and metadata when the source is acknowledged. Its dataset citation pattern is `Source: [DOI], [access date]`; customised datasets use the datacode link and access date. [Copyright and re-use notice](https://ec.europa.eu/eurostat/web/main/help/copyright-notice) | Individual notices can impose conditions. Third-party material is excluded. Commercial reuse is restricted for data from countries outside EU/EFTA/official candidates unless stated otherwise, and for specified Swiss and Austrian detailed trade data. Modifications/translations must be identified and require the stated Eurostat disclaimer. Check each chosen dataset, geography and source metadata; this is not a blanket licence for embedded third-party data. |
| [World Bank World Development Indicators (WDI)](https://datacatalog.worldbank.org/search/dataset/0037712/world-development-indicators) | The [Indicators API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392-about-the-indicators-api-documentation) is V2 and says API keys are not required. The general terms prohibit excessive or abusive volume but publish no numeric ceiling in the reviewed materials; use pagination, caching, throttling and backoff. | The WDI catalogue classifies the dataset as Public and specifies **Creative Commons Attribution 4.0**. Retain the required CC BY attribution, licence link and change indication in any derived or redistributed output. | Confirm the selected indicator's metadata, source and licence at implementation time. The World Bank's general [terms](https://www.worldbank.org/ext/en/legal/terms-conditions) say dataset-specific terms apply; materials without an express dataset licence have different commercial and redistribution restrictions. Do not treat the WDI catalogue licence as permission for every third-party series referenced by an indicator. |

The evidence supports these as candidates under the requested access and reuse criteria, subject to dataset-level checks. It does not choose an owner use case, metric, commercial distribution model, refresh cadence, or source priority.
