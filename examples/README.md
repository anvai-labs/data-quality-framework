# Data Quality Framework Examples

This directory contains example configurations and scripts demonstrating how to use the Data Quality Framework.

## Configuration Examples

- `basic_deequ_validation.conf` - Basic data quality checks using the Deequ engine
- `dqdl_validation.conf` - Vendor-specific DQDL compatibility rules through PyDeequ
- `schema_validation.conf` - Schema validation with datatype, nullable, and unique constraints
- `custom_constraints.conf` - Custom engine constraints (distinctness, rate-of-change, negative values)
- `multi_engine_pipeline.conf` - Multi-engine pipeline combining Deequ and schema validation
- `unity_catalog.conf` - Configuration using Databricks Unity Catalog
- `glue_catalog.conf` - Configuration using AWS Glue Data Catalog

## Sample Scripts

- `sample_spark_job.py` - Complete working example with a local Spark session

## Running Examples

Ensure you have the framework installed with Spark and Deequ support:

```bash
pip install data-quality-framework[spark,deequ]
```

Then run the sample job:

```bash
python examples/sample_spark_job.py
```

CI parses every checked-in `.conf` file with the same structural validator used by
`dq-validate`. DQDL provides compatibility for existing Deequ rulesets; portable typed
rules will be introduced through the engine-neutral kernel described in ADR-002.
