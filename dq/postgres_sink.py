# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Durable PostgreSQL outcome sink (ADR-006).

Driver-neutral by dependency inversion: this module depends only on the
DB-API protocol. A ``connect`` callable is injected (deployment owns
credentials — by convention a DSN read from an environment variable), and no
PostgreSQL driver is imported here. One run is one transaction: saves inside
:meth:`PostgresOutcomeSink.run_scope` join a single transaction that commits
on clean exit and rolls back on any exception, so a partial run never
becomes visible evidence. Writes are append-only and idempotent — primary
keys on the run key and artifact sequence with ``ON CONFLICT DO NOTHING``
mean first write wins and retries are safe.
"""

from __future__ import annotations

from contextlib import contextmanager
import json

from dq.exceptions import ConfigurationError
from dq.identifiers import ColumnName, TableName
from dq.sinks import OutcomeSink, WriteResult

DEFAULT_DSN_VARIABLE = "DQ_POSTGRES_DSN"


class PostgresOutcomeSink(OutcomeSink):
    """Persist runs and bounded artifacts durably in PostgreSQL.

    Args:
        connect: DB-API ``connect()`` callable (injected; deployment-owned).
        dataset: Dataset label stamped onto every row.
        schema: PostgreSQL schema holding the evidence tables.
        runs_table / artifacts_table: Evidence table names.
    """

    def __init__(
        self,
        connect,
        dataset: str,
        schema: str = "dq",
        runs_table: str = "runs",
        artifacts_table: str = "artifacts",
    ):
        self._connect = connect
        self._dataset = dataset
        self._schema = TableName.parse(schema, label="postgres schema", max_parts=1)
        self._runs_table = ColumnName.parse(runs_table, label="runs table")
        self._artifacts_table = ColumnName.parse(
            artifacts_table, label="artifacts table"
        )
        self._active = None  # (connection, cursor) while a run scope is open

    @classmethod
    def from_config(cls, repoconfig, env=None) -> PostgresOutcomeSink:
        """Build the sink from ``repository`` configuration.

        Reads the DSN from the environment variable named by ``dsn_env``
        (default ``DQ_POSTGRES_DSN``) and fails closed when it is absent —
        credentials never live in configuration files.
        """
        import os

        environment = env if env is not None else os.environ
        variable = repoconfig.get("dsn_env", None) or DEFAULT_DSN_VARIABLE
        dsn = environment.get(variable, None)
        if not dsn:
            raise ConfigurationError(
                f"environment variable {variable!r} must contain the "
                "PostgreSQL DSN for the durable sink"
            )
        dataset = repoconfig.get("dataset", None)
        if dataset is None:
            raise ConfigurationError("repository dataset name is required")

        def connect():
            import psycopg

            return psycopg.connect(dsn)

        return cls(
            connect,
            dataset=dataset,
            schema=repoconfig.get("schema", None) or "dq",
            runs_table=repoconfig.get("runs_table", None) or "runs",
            artifacts_table=repoconfig.get("artifacts_table", None) or "artifacts",
        )

    def ddl_statements(self):
        """Reference DDL for operators; provisioning belongs to deployment."""
        schema = self._schema.quoted_pg
        return [
            f"CREATE TABLE IF NOT EXISTS {schema}.{self._runs_table.quoted_pg} ("
            "run_key BIGINT PRIMARY KEY, dataset TEXT NOT NULL, "
            "started_millis BIGINT NOT NULL, status TEXT NOT NULL, "
            "identity JSONB NOT NULL DEFAULT '{}'::jsonb)",
            f"CREATE TABLE IF NOT EXISTS {schema}.{self._artifacts_table.quoted_pg} ("
            "run_key BIGINT NOT NULL REFERENCES "
            f"{schema}.{self._runs_table.quoted_pg}(run_key), "
            "artifact_type TEXT NOT NULL, seq INTEGER NOT NULL, "
            "dataset TEXT NOT NULL, payload JSONB NOT NULL, "
            f"PRIMARY KEY (run_key, artifact_type, seq))",
        ]

    def run_scope(self, run_key: int, identity=None):
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"INSERT INTO {self._qualified_runs()} "
                "(run_key, dataset, started_millis, status, identity) "
                "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (run_key) DO NOTHING",
                (
                    int(run_key),
                    self._dataset,
                    int(run_key),
                    "completed",
                    json.dumps(dict(identity or {})),
                ),
            )
        except Exception:
            connection.rollback()
            connection.close()
            raise
        return self._run_session(connection, cursor, run_key, identity)

    def _run_session(self, connection, cursor, run_key, identity):
        @contextmanager
        def session():
            previous = self._active
            self._active = (connection, cursor)
            try:
                yield self
            except Exception:
                self._active = previous
                connection.rollback()
                connection.close()
                raise
            self._active = previous
            connection.commit()
            connection.close()

        return session()

    def save(self, df, metric_type: str, key: int) -> WriteResult:
        rows = [row.asDict(recursive=True) for row in df.collect()]
        if self._active is not None:
            connection, cursor = self._active
            self._insert_artifacts(cursor, metric_type, key, rows)
            return WriteResult(
                metric_type, int(key), (f"postgres:{self._qualified_artifacts()}",)
            )
        connection = self._connect()
        try:
            cursor = connection.cursor()
            self._insert_artifacts(cursor, metric_type, key, rows)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return WriteResult(
            metric_type, int(key), (f"postgres:{self._qualified_artifacts()}",)
        )

    def _insert_artifacts(self, cursor, metric_type, key, rows):
        statement = (
            f"INSERT INTO {self._qualified_artifacts()} "
            "(run_key, artifact_type, seq, dataset, payload) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (run_key, artifact_type, seq) DO NOTHING"
        )
        for sequence, row in enumerate(rows):
            cursor.execute(
                statement,
                (int(key), metric_type, sequence, self._dataset, json.dumps(row)),
            )

    def _qualified_runs(self) -> str:
        return f"{self._schema.quoted_pg}.{self._runs_table.quoted_pg}"

    def _qualified_artifacts(self) -> str:
        return f"{self._schema.quoted_pg}.{self._artifacts_table.quoted_pg}"

    @property
    def dataset(self) -> str:
        return self._dataset
