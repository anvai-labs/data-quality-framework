# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added a bounded PyDeequ DQDL compatibility engine and executable example
- Added ADR-002 for an engine-neutral rule and metric kernel with Spark, PyDeequ,
  Great Expectations, and Arrow/DataFusion adapters

### Changed

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
