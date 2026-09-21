# Data Quality Framework

[![Python versions](https://img.shields.io/pypi/pyversions/data-quality-framework.svg)](https://pypi.org/project/data-quality-framework/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/license/Apache-2.0)

A flexible, configuration-driven data quality framework for Apache Spark with pluggable validation engines.

## Features

- **Configuration-Driven**: Define validation rules in HOCON format
- **Multiple Engines**: Built-in support for Deequ, DQDL, Great Expectations, schema validation, and custom constraints
- **Extensible**: Easy to add custom engines and constraints via Python reflection
- **Spark Native**: Designed for distributed data processing with PySpark

## Installation

```bash
# Lightweight configuration/report helpers do not import Spark or cloud SDKs.
pip install data-quality-framework

# The framework orchestrator and built-in schema/custom engines require Spark and PyDeequ.
pip install data-quality-framework[spark,deequ]

# Add the catalog/auth integrations you use.
pip install data-quality-framework[spark,deequ,aws,databricks]
```

### From Source

```bash
# Clone the repository
git clone https://github.com/anvai-labs/data-quality-framework.git
cd data-quality-framework

# Install with Poetry
poetry install

# With optional extras
poetry install -E spark -E deequ -E aws
```

## Quick Start

```python
import os

os.environ["SPARK_VERSION"] = "3.5"  # Set before importing PyDeequ-backed engines.

from pyspark.sql import SparkSession
import pydeequ
from dq.dq_framework import DQFramework

# Resolve the matching Deequ runtime and its dependencies before creating Spark.
spark = (
    SparkSession.builder.appName("dq-example")
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    .getOrCreate()
)

# Define validation rules in HOCON format
config = """
dqframework {
  dqrules = [
    {
      name = "completeness_check"
      engine = "deequ"
      dataframes = ["default"]
      checks = [
        { constraint = "isComplete", column = "id", level = "Error" }
        { constraint = "isUnique", column = "id", level = "Error" }
      ]
    }
  ]
}
"""

# Create sample data
df = spark.createDataFrame([
    ("1", "Alice", 34),
    ("2", "Bob", 45),
    ("3", "Catherine", 29)
], ["id", "name", "age"])

# Run data quality checks
framework = DQFramework(spark, config, default_dataframe=df)
results = framework.run()

# Process results
for result in results:
    status = "PASS" if result['success'] else "FAIL"
    print(f"[{status}] {result['check']}")
```

### Promotion evidence

Automation can bind every outcome to immutable dataset and rule-set identities:

```bash
dq-framework rules.conf \
  --dataset-id bars-spy-2026q3 \
  --dataset-sha256 "${DATASET_SHA256}" \
  --report-json results/data-quality.json
```

The command exits nonzero when configuration is invalid, a configured table
cannot be loaded, an engine emits no outcomes, an outcome has no Boolean
`success` value, or any check fails. The versioned `dq-report/v1` document
contains the dataset and local rule-set digests, runtime identity, summary, and
all check outcomes. Evidence generation requires a local configuration file;
materialize remote configurations locally before validation so the exact bytes
can be hashed.

The facade and report builder validate summaries through an immutable
`dq.outcomes.CheckOutcome` boundary. Valid JSON extension fields are preserved;
unknown success values, non-finite numbers, non-string object keys, cyclic or
oversized diagnostics fail closed. Exports are independent dictionaries, so
runtime enrichment no longer mutates engine-owned results.

Limits: 10,000 outcomes, 1 MiB per encoded outcome, 16 MiB per batch/run,
10,000 diagnostic nodes per outcome, and nesting depth 16. Reduce diagnostic
payloads or split the validation job; results are never silently truncated.
These are evidence-boundary limits, not protection against upstream Spark
collection. Existing Spark `Row` details now export named dictionaries instead
of positional JSON arrays; consumers must use field names. `dq-report/v1` and
the public list-of-dictionaries result API remain unchanged.

```python
from dq.outcomes import CheckOutcome

outcome = CheckOutcome.from_legacy({"check": "complete", "success": True})
assert outcome.success
legacy = outcome.to_legacy()  # an independent mutable copy
```

This is modernization unit U3a, not the portable rule/metric planner. Engine-owned
repository writes still precede validation and are not admission evidence. The
framework does not yet provide a transactional PostgreSQL admission sink.

## Portable count plans (opt-in core API)

`dq.plan` defines frozen dataset/column references, rules, exact predicates,
metric requests, capability declarations, and canonical execution plans. The
first subset is size and completeness. Equivalent requests share metrics;
unsupported capabilities fail before evaluation. Existing HOCON/DQDL rules are
not translated automatically.

`dq.spark_adapter` executes such plans natively on Spark for size and
completeness. It validates capabilities and bindings first, computes every
required metric for a dataset in one shared aggregate action, returns only
bounded outcome snapshots, and follows counts/v1 semantics: present counts
exclude null and floating-point NaN, keep empty strings and string "NaN",
and completeness fails on an empty dataset. Column and dataset bindings must
match exactly (case-sensitively) or execution is refused before any Spark job.
`dq.portable_config` translates only the exactly representable HOCON subset
(`isComplete`, single-comparison `hasCompleteness`, integer-comparison
`hasSize`); every other constraint fails closed and stays on the legacy engine
path.

From a source checkout, run the dependency-free kernel demonstration:

```bash
python -m examples.portable_counts
```

It uses illustrative counts and a placeholder digest, not verified dataset
evidence. With the `spark` extra installed, `python -m
examples.portable_spark_counts` runs a translated HOCON subset against a local
Spark DataFrame. See the [portable semantics contract](docs/index.adoc)
before implementing an adapter. Existing engine configuration remains unchanged.

## Supported Engines

| Engine | Description | Use Case |
|--------|-------------|----------|
| **Deequ** | Amazon Deequ integration | Comprehensive data quality checks |
| **Schema Validation** | Spark schema-based validation | Datatype, nullable, unique, FK constraints |
| **Custom** | Custom constraint implementations | Business-specific validation rules |
| **Great Expectations** | GE framework integration | Expectation-based validation |

## Configuration

The framework uses [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) format for configuration. Configurations can be loaded from:

- Inline strings
- Local files (`file://path/to/config.conf`)
- S3 (`s3://bucket/path/to/config.conf`)
- ADLS Gen2 (`abfss://container@account.dfs.core.windows.net/path/config.conf`)
- HTTP/HTTPS URLs

### Example Configuration

```hocon
dqframework {
  # Define DataFrames by catalog table name
  dataframes {
    orders = "catalog.schema.orders"
    customers = "catalog.schema.customers"
  }

  # Define validation rules
  dqrules = [
    {
      name = "order_validation"
      engine = "deequ"
      dataframes = ["orders"]
      checks = [
        { constraint = "isComplete", column = "order_id", level = "Error" }
        { constraint = "isNonNegative", column = "amount", level = "Warning" }
        { constraint = "isContainedIn", column = "status", allowed_values = ["pending", "completed", "cancelled"] }
      ]
    }
    {
      name = "schema_check"
      engine = "schemavalidation"
      dataframes = ["customers"]
      checks = [
        { column = "email", datatype = "StringType", nullable = false }
        { column = "age", datatype = "IntegerType", nullable = true }
      ]
    }
  ]

  # Optional: Configure result persistence
  repository {
    path = "/path/to/metrics"
    format = "delta"
  }
}
```

## Extending the Framework

### Custom Engines

Create a new engine by implementing the `DQEngine` abstract base class:

```python
from dq.engine.dq_engine import DQEngine

class MyCustomEngine(DQEngine):
    def apply(self, dataframe, repository=None):
        results = []
        # Implement validation logic
        for check in self._config.get("checks", []):
            # Run check and append results
            results.append({
                "check": check["constraint_name"],
                "success": True,  # or False
                "details": {}
            })
        return results
```

Place your engine in `dq/engine/mycustom/mycustom_engine.py` and reference it in configuration as `engine = "mycustom"`.

## Requirements

- Python 3.12 and 3.13 (the tested source and CI matrix)
- Apache Spark 3.5.9
- PyDeequ 1.7.0
- Deequ JAR file (for Deequ engine): `lib/deequ-2.0.21-spark-3.5.jar`
- DQDL additionally needs `lib/dqdl-1.0.0.jar` on the Spark classpath
- Java 17 (CI runtime)

PyDeequ 1.7.0 also maps Spark 4.1, but this release deliberately certifies Spark 3.5.9 only.
Spark 4/Databricks Runtime 18 needs its own adapter and semantic-parity lane; see the runtime
boundary in the [operator reference](docs/index.adoc) and the
[engine-neutral-kernel ADR](docs/decisions/adr/ADR-002-engine-neutral-rule-kernel.adoc).

## Development

```bash
# Install development dependencies
poetry install --with dev

# Run tests
pytest dq/tests/ -v

# Run linting
black dq/
flake8 dq/

# Run security scan
bandit -r dq/ -x tests
```

## Documentation

- [Current architecture, configuration, and API reference](docs/index.adoc)
- [Active architecture modernization plan and debt ledger](docs/modernization-plan.adoc)
- [ADR-001: incremental typed execution kernel](docs/decisions/adr/ADR-001-incremental-typed-execution-kernel.adoc)
- [Databricks authentication and runtime boundary](docs/databricks-authentication.adoc)
- [Examples](examples/)

The remaining documents under `docs/` are explicitly labeled planning records. They describe
possible future work and are not statements about implemented behavior.

CI runs the Spark-backed suite on Python 3.12 and 3.13; enforces independent 55% line and
branch floors plus changed-code coverage; audits dependencies; builds the package; and rejects
broken documentation links.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.
