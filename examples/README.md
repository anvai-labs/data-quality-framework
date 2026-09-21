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
- `portable_counts.py` - Dependency-free portable count-plan demonstration using fixture counts
- `portable_spark_counts.py` - Native Spark execution of a translated HOCON count-plan subset

## Running Examples

Ensure you have the framework installed with Spark and Deequ support:

```bash
pip install data-quality-framework[spark,deequ]
```

Then run the sample job:

```bash
python examples/sample_spark_job.py
```

The local sample expects `lib/deequ-2.0.21-spark-3.5.jar`; download the matching artifact
from Maven Central or use the Maven-package setup in the root README. DQDL also requires
`software.amazon.glue:dqdl:1.0.0` on the classpath; see the
[operator reference](../docs/index.adoc). The sample intentionally contains a missing email
and exits nonzero to demonstrate failed-check handling. Configuration validation checks
structure; catalog examples still require the named tables, credentials, and compatible data.

CI parses every checked-in `.conf` file with the same structural validator used by
`dq-validate`. DQDL provides compatibility for existing Deequ rulesets; portable typed
rules are introduced through the engine-neutral kernel described in ADR-002. The
portable path currently covers size and completeness only (`dq.plan`,
`dq.spark_adapter`, and the HOCON subset translator `dq.portable_config`); other
constraints keep using their existing engines and fail closed on translation.
