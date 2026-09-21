# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Centralized table, column, and catalog identifiers into validated value objects
  (`dq.identifiers`) used at every catalog and SQL boundary; malformed or injected
  identifiers fail closed before any Spark call, and Unity catalog SQL statements
  use quoted names (TD-ARCH-2)

## [2.2.0] - 2026-09-21

### Added

- Added a native Spark adapter (`dq.spark_adapter`) that executes portable size and
  completeness plans through one shared aggregate per dataset, rejecting unsupported
  capabilities and ambiguous bindings before execution
- Added a fail-closed HOCON translation subset (`dq.portable_config`) covering
  `isComplete`, single-comparison `hasCompleteness`, and integer-comparison `hasSize`
- Added a differential certification suite running the same size and completeness
  rules through PyDeequ and the native Spark adapter; decisions agree on nulls,
  decimals, empty inputs, and exact thresholds, and the intentional NaN divergence
  is pinned and documented
- Added the groups/v1 kernel contract (ADR-004): grouped distinct-count bounds as
  exact, threshold-independent metrics with a grouped-distinct rule kind and
  vacuous-pass empty-input semantics matching the legacy constraint
- Extended the native Spark adapter to execute groups/v1 plans through one
  aggregate per distinct grouping, and the translation subset to convert
  `DistinctnessByGroup` checks into `.min`/`.max` grouped-distinct rules; a
  differential suite proves the portable path decides identically to the
  legacy constraint, including on empty input and one-sided thresholds
- Added a bounded PyDeequ DQDL compatibility engine and executable example
- Added ADR-002 for an engine-neutral rule and metric kernel with Spark, PyDeequ,
  Great Expectations, and Arrow/DataFusion adapters

### Changed

- Custom-engine constraints now decide through distributed aggregations and return
  one bounded summary per column instead of collecting per-group or per-row rows to
  the driver; zero and null baselines are reported as skipped pairs instead of
  crashing, and the logging-only count action was removed
- Split the custom engine into four stateless constraint strategy objects behind the
  `CustomEngine` facade, froze its constraint surface as a permanent adapter contract
  (ADR-003), and added fail-closed validation of reference table and column
  identifiers before Spark SQL is built
- Renamed the custom constraints to canonical names (`GroupedDistinctBounds`,
  `ConsecutivePercentChange`, `ColumnNamesInReferenceTable`, `NoNegativeValues`);
  the previous names remain accepted aliases that emit their historical rows and
  a deprecation warning
- Replaced domain-specific test fixture vocabulary with neutral structural names
- Upgraded PyDeequ from 1.6.0 to 1.7.0 and the Spark 3.5 Deequ JAR from 2.0.8
  to 2.0.21; CI now uses Java 17 for Spark integration
- Made the top-level package and configuration helpers lazy with respect to Spark,
  PyDeequ, and AWS dependencies; package metadata is the sole version source
- Made structural validation engine-aware and validated every checked-in HOCON example

### Security

- Replaced Python evaluation of configured assertions and generated PyDeequ
  suggestions with a restricted, allowlisted expression interpreter

### Documentation

- Added the active architecture modernization plan, technical-debt ledger, and
  incremental typed-kernel ADRs

## [2.1.0] - 2026-09-20

### Added

- Python 3.13 package and Spark integration coverage
- Versioned, dataset-bound `dq-report/v1` execution evidence

### Changed

- Supported Python range is now 3.12 through 3.13; Python 3.10 and 3.11 are no
  longer supported
- Empty rules, missing configured DataFrames, empty engine outcomes, and
  ambiguous success values now fail closed

## [2.0.0] - 2025-02-03

### Added
- Open source release under Apache 2.0 license
- GitHub Actions CI/CD (lint, test matrix across Python 3.9-3.12, build, release)
- Pluggable catalog system with auto-detection (Spark, Hive, Unity Catalog, AWS Glue)
- CLI entry points: `dq-framework` and `dq-validate`
- Custom exceptions hierarchy (`DQFrameworkError` and subclasses)
- Comprehensive example configurations in `examples/`
- CONTRIBUTING.md, SECURITY.md, CODE_OF_CONDUCT.md
- Dependabot for automated dependency updates

### Changed
- Restructured source layout for proper packaging
- Replaced `print()` statements with `logging` throughout engine code
- Added type hints and docstrings to public APIs
- Added input validation for engine names and table identifiers
- Made pyproject.toml the single source of truth for dependencies
- Bumped version to 2.0.0 to signal clean break for OSS release

### Removed
- Internal deployment configurations and proprietary references
- Build artifacts and legacy setuptools files

### Fixed
- SQL injection risk in DataFrame resolution (now uses `spark.table()`)
- Missing timeout on HTTP config fetching
