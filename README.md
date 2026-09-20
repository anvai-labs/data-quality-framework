# Data Quality Framework

[![Python versions](https://img.shields.io/pypi/pyversions/data-quality-framework.svg)](https://pypi.org/project/data-quality-framework/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/license/Apache-2.0)

A flexible, configuration-driven data quality framework for Apache Spark with pluggable validation engines.

## Features

- **Configuration-Driven**: Define validation rules in HOCON format
- **Multiple Engines**: Built-in support for Deequ, Great Expectations, schema validation, and custom constraints
- **Extensible**: Easy to add custom engines and constraints via Python reflection
- **Spark Native**: Designed for distributed data processing with PySpark

## Installation

```bash
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
from pyspark.sql import SparkSession
from dq.dq_framework import DQFramework

# Create Spark session
spark = SparkSession.builder.appName("dq-example").getOrCreate()

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
- Deequ JAR file (for Deequ engine): `lib/deequ-2.0.8-spark-3.5.jar`

PyDeequ 1.6.0 currently maps Spark versions only through Spark 3.5. The repository does not
claim Spark 4 or Databricks Runtime 18 compatibility yet; see the runtime boundary below.

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
