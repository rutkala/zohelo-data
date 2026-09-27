# Synthetic NBP fixtures

These small JSON files use the NBP response shape. **All values and publication identifiers are synthetic.** They are test inputs, not historical NBP observations or evidence of production coverage.

The fixture check creates temporary bronze Parquet, including an identical second copy to represent replayed inputs. It executes the actual A/B/C and gold-price dbt models and the current Table A projection mart, checks independently stated expected rows and date types, generates the artifacts used by portal deployment, and verifies missing inputs and conflicting gold values fail. All files and the DuckDB database are disposable. No Google credentials, Drive access or production entrypoints are used.

The gold-price fixture follows the NBP API contract: `data` is the publication date and `cena` is the PLN price for one gram of gold at 1000 millesimal fineness. Values are synthetic test inputs, not historical NBP observations or evidence of production coverage. The gold model accepts identical replay rows, but rejects conflicting same-date values when legacy files do not provide reliable revision provenance. This does not prove real-data completeness or provide MetricFlow coverage; those remain tracked in the delivery plan and audit.
