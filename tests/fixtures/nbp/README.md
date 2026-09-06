# Synthetic NBP fixtures

These small JSON files use the NBP response shape. **All values and publication identifiers are synthetic.** They are test inputs, not historical NBP observations or evidence of production coverage.

The fixture check creates temporary bronze Parquet, including an identical second copy to represent replayed inputs. It executes the actual A/B/C dbt models and the current Table A projection mart, checks independently stated expected rows and date types, generates the artifacts used by portal deployment, and verifies a missing input fails. All files and the DuckDB database are disposable. No Google credentials, Drive access or production entrypoints are used.

This checks existing behavior only. In particular, it does not endorse filename ordering as a correction policy, validate Table C's choice of date role, prove real-data completeness, or provide gold-price/MetricFlow coverage. Those are tracked in the delivery plan and audit.
