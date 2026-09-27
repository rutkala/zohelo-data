# Archive

`05_archive` retains superseded source files for recovery and audit. It is a Python-managed storage boundary rather than a dbt transformation layer: no dbt model or source points at Archive, and archived objects do not become catalogue relations. Reprocessing starts from verified Landing inputs or an explicitly supported Bronze compatibility input.
