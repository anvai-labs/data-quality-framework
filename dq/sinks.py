# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Outcome sink port and storage implementations (TD-ARCH-7/U6).

Engines no longer own persistence semantics: the framework builds one sink
from the ``dqframework.repository`` configuration and hands it to engines,
which pass their metric and verification artifacts back through
``OutcomeSink.save``. Sinks are append-only and keyed: every write carries
the caller's idempotency key, ``InMemorySink`` enforces key uniqueness so
callers can prove their key discipline, and ``RepositorySink`` writes files
and catalog tables without forcing the DataFrame into one partition —
storage tuning belongs to deployment configuration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Optional

from dq.exceptions import ConfigurationError, RepositoryError
from dq.identifiers import ColumnName, TableName

_FILE_FORMATS = ("parquet", "csv", "json", "delta", "orc")


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Bounded summary of one sink write."""

    metric_type: str
    key: int
    targets: tuple[str, ...]


class OutcomeSink(ABC):
    """Port for persisting engine artifacts under an idempotency key.

    Implementations must be append-only for a given target: a write with a
    fresh key adds evidence, and a write repeating a key must fail rather
    than silently duplicate.
    """

    @abstractmethod
    def save(self, df, metric_type: str, key: int) -> WriteResult:
        """Persist one artifact under ``(metric_type, key)``.

        Args:
            df: Bounded engine artifact (metrics or verification rows).
            metric_type: Artifact kind, e.g. ``metrics`` or ``verifications``.
            key: Idempotency key (execution timestamp in millis).

        Returns:
            A bounded :class:`WriteResult` describing the write.
        """
        raise NotImplementedError

    def run_scope(self, run_key: int, identity=None):
        """Optionally scope saves into one durable, all-or-nothing run.

        The default is per-save autocommit. Durable sinks override this to
        make every ``save`` inside the scope join one transaction that
        commits on clean exit and rolls back on any exception, so a partial
        run never becomes visible evidence (ADR-006).
        """
        return nullcontext()


class InMemorySink(OutcomeSink):
    """Idempotent bounded sink for tests and caller-side characterization.

    Artifacts are collected into memory (bounded: engine artifacts are
    per-column summaries), and a second write with the same
    ``(metric_type, key)`` raises :class:`RepositoryError` instead of
    silently duplicating evidence.
    """

    def __init__(self):
        self._writes = {}

    def save(self, df, metric_type: str, key: int) -> WriteResult:
        record = (metric_type, int(key))
        if record in self._writes:
            raise RepositoryError(
                f"duplicate sink write for {metric_type!r} with key {key}"
            )
        self._writes[record] = [row.asDict(recursive=True) for row in df.collect()]
        return WriteResult(metric_type, int(key), (f"memory:{metric_type}",))

    def rows(self, metric_type: str, key: int) -> list:
        """Return the stored rows for one write."""
        return self._writes[(metric_type, int(key))]

    def writes(self) -> dict:
        """Return a copy of all writes keyed by (metric_type, key)."""
        return dict(self._writes)


class RepositorySink(OutcomeSink):
    """File and catalog sink driven by ``dqframework.repository`` configuration.

    The configuration is validated eagerly at construction: a dataset name,
    a whitelisted format, and at least one file path or a two-part catalog
    table are required before any Spark call. Writes are append-only; a
    catalog table is created on first write and appended afterwards.
    """

    def __init__(self, repoconfig):
        dataset = repoconfig.get("dataset", None)
        if dataset is None:
            raise ConfigurationError("repository dataset name is required")
        file_format = repoconfig.get("format", "delta")
        if file_format not in _FILE_FORMATS:
            raise ConfigurationError(
                f"repository format {file_format!r} must be one of "
                f"{list(_FILE_FORMATS)}"
            )
        file_config = repoconfig.get("file", {})
        self._paths = tuple(file_config.get("paths", []) or ())
        # The tenant namespace is stamped on file writes; catalog tables keep
        # their deployment-era shape and gain the column on recreation.
        self._namespace = ColumnName.parse(
            repoconfig.get("namespace", None) or "default",
            label="tenant namespace",
        )
        catalog_config = repoconfig.get("catalog", {})
        tables = []
        for table in catalog_config.get("tables", []) or ():
            if table is None:
                raise ConfigurationError(
                    "repository table name is not provided in the configuration"
                )
            parsed = TableName.parse(table, label="repository table")
            if len(parsed.parts) != 2:
                raise ConfigurationError(
                    f"repository table {table!r} must be two-part (database.table)"
                )
            tables.append(str(parsed))
        self._tables = tuple(tables)
        if not self._paths and not self._tables:
            raise ConfigurationError(
                "repository requires at least one file path or catalog table"
            )
        self._dataset = dataset
        self._format = file_format

    def save(self, df, metric_type: str, key: int) -> WriteResult:
        from pyspark.sql import functions as F

        year = F.year(F.from_unixtime(F.lit(key / 1000)))
        namespace = str(self._namespace)
        enriched = (
            df.withColumn("dqts", F.lit(key))
            .withColumn("dataset", F.lit(self._dataset))
            .withColumn("tenant_namespace", F.lit(namespace))
            .withColumn("year", F.lit(year))
        )
        # Preflight every catalog target BEFORE any write so a schema
        # mismatch fails closed instead of leaving partial evidence
        # (review finding F06).
        for table in self._tables:
            table_with_suffix = f"{table}_{metric_type}"
            if df.sparkSession.catalog.tableExists(table_with_suffix):
                table_columns = {
                    column.name
                    for column in df.sparkSession.catalog.listColumns(table_with_suffix)
                }
                missing = [
                    column for column in enriched.columns if column not in table_columns
                ]
                if missing:
                    raise RepositoryError(
                        f"evidence table {table_with_suffix!r} is missing "
                        f"columns {sorted(missing)}; migrate the table "
                        "before writing"
                    )
        targets = []
        for path in self._paths:
            target = f"{path}/{metric_type}"
            enriched.write.mode("append").format(self._format).partitionBy(
                "dataset", "year"
            ).save(target)
            targets.append(target)
        for table in self._tables:
            table_with_suffix = f"{table}_{metric_type}"
            exists = df.sparkSession.catalog.tableExists(table_with_suffix)
            if exists:
                enriched.write.mode("append").format(self._format).option(
                    "mergeSchema", "true"
                ).insertInto(table_with_suffix)
            else:
                enriched.write.mode("overwrite").format(self._format).saveAsTable(
                    table_with_suffix
                )
            targets.append(table_with_suffix)
        return WriteResult(metric_type, int(key), tuple(targets))


def sink_from_config(repoconfig) -> OutcomeSink | None:
    """Build the production sink from repository configuration.

    Returns ``None`` for empty configuration, which callers treat as
    "do not persist". ``type = "postgres"`` builds the durable sink
    (ADR-006); any other type fails closed.
    """
    if not repoconfig:
        return None
    kind = repoconfig.get("type", None)
    if kind is None:
        return RepositorySink(repoconfig)
    if kind == "postgres":
        from dq.postgres_sink import PostgresOutcomeSink

        return PostgresOutcomeSink.from_config(repoconfig)
    raise ConfigurationError(f"repository sink type {kind!r} is not supported")
