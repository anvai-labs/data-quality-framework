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
keys on namespace plus run key and artifact sequence with ``ON CONFLICT DO
NOTHING`` mean first write wins and retries are safe.

Threading: one sink instance owns one run scope at a time and is not
thread-safe; share instances across threads only between scopes.
"""

from __future__ import annotations

from contextlib import contextmanager
import json

from dq.exceptions import ConfigurationError, RepositoryError
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
        namespace: str = "default",
        retention_days: int | None = None,
    ):
        self._connect = connect
        self._dataset = dataset
        self._schema = TableName.parse(schema, label="postgres schema", max_parts=1)
        self._runs_table = ColumnName.parse(runs_table, label="runs table")
        self._artifacts_table = ColumnName.parse(
            artifacts_table, label="artifacts table"
        )
        self._namespace = ColumnName.parse(namespace, label="tenant namespace")
        if retention_days is not None and (
            type(retention_days) is not int or retention_days < 1
        ):
            raise ConfigurationError(
                "retention_days must be a positive integer when provided"
            )
        self._retention_days = retention_days
        self._active = None  # (connection, cursor, run_key) while a scope is open

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
            namespace=repoconfig.get("namespace", None) or "default",
            retention_days=repoconfig.get("retention_days", None),
        )

    def ddl_statements(self):
        """Reference DDL for operators; provisioning belongs to deployment.

        Includes the tenant namespace column and the admission views
        (SPEC-001/006): `runs_completed` and `latest_per_dataset`. Views use
        CREATE OR REPLACE so re-running provisioning is idempotent.
        """
        schema = self._schema.quoted_pg
        runs = f"{schema}.{self._runs_table.quoted_pg}"
        artifacts = f"{schema}.{self._artifacts_table.quoted_pg}"
        return [
            f"CREATE TABLE IF NOT EXISTS {runs} ("
            "namespace TEXT NOT NULL DEFAULT 'default', "
            "run_key BIGINT NOT NULL, dataset TEXT NOT NULL, "
            "started_millis BIGINT NOT NULL, status TEXT NOT NULL, "
            "identity JSONB NOT NULL DEFAULT '{}'::jsonb, "
            "PRIMARY KEY (namespace, run_key))",
            f"CREATE TABLE IF NOT EXISTS {artifacts} ("
            "namespace TEXT NOT NULL DEFAULT 'default', "
            "run_key BIGINT NOT NULL, artifact_type TEXT NOT NULL, "
            "seq INTEGER NOT NULL, dataset TEXT NOT NULL, payload JSONB NOT NULL, "
            f"PRIMARY KEY (namespace, run_key, artifact_type, seq))",
            f"CREATE OR REPLACE VIEW {schema}.runs_completed AS "
            f"SELECT * FROM {runs} WHERE status = 'completed'",
            f"CREATE OR REPLACE VIEW {schema}.latest_per_dataset AS "
            "SELECT * FROM (SELECT r.*, ROW_NUMBER() OVER ("
            "PARTITION BY r.namespace, r.dataset "
            "ORDER BY r.started_millis DESC, r.run_key DESC) AS rn "
            f"FROM {runs} r WHERE r.status = 'completed') ranked "
            "WHERE ranked.rn = 1",
        ]

    def verify_schema(self) -> dict:
        """Check both evidence tables exist and return their row counts.

        Fails closed with ``RepositoryError`` when the schema is absent.
        """
        schema = str(self._schema)
        connection = self._connect()
        try:
            cursor = connection.cursor()
            for table in (str(self._runs_table), str(self._artifacts_table)):
                cursor.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema, table),
                )
                found = cursor.fetchone()
                if not found or not found[0]:
                    raise RepositoryError(
                        f"evidence table {schema}.{table} does not exist; "
                        "apply ddl_statements() first"
                    )
            counts = {}
            for table in (str(self._runs_table), str(self._artifacts_table)):
                cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                counts[table] = int(cursor.fetchone()[0])
            return counts
        except RepositoryError:
            raise
        except Exception as error:
            raise RepositoryError(
                f"evidence schema check failed for {schema!r}: {error}"
            ) from error
        finally:
            connection.close()

    def prune(
        self,
        before_millis: int | None = None,
        namespace: str | None = None,
        all_namespaces: bool = False,
    ) -> tuple[int, int]:
        """Delete artifacts and runs older than the cutoff.

        Deleting evidence requires explicit retention configuration
        (``retention_days``). Scope defaults to this sink's namespace;
        pass ``all_namespaces=True`` (with ``namespace=None``) to prune
        across namespaces. Artifacts are deleted before runs.

        Returns:
            (artifacts deleted, runs deleted).
        """
        if self._retention_days is None:
            raise RepositoryError(
                "pruning evidence requires explicit retention_days " "configuration"
            )
        if before_millis is None:
            import time

            before_millis = (
                time.time_ns() // 1_000_000 - self._retention_days * 86_400_000
            )
        scoped = namespace is not None or not all_namespaces
        target = str(self._namespace) if namespace is None else str(namespace)
        connection = self._connect()
        try:
            cursor = connection.cursor()
            if scoped:
                cursor.execute(
                    f"DELETE FROM {self._qualified_artifacts()} AS a "
                    "USING " + self._qualified_runs() + " AS r "
                    "WHERE a.namespace = r.namespace "
                    "AND a.run_key = r.run_key "
                    "AND r.namespace = %s AND r.started_millis < %s",
                    (target, before_millis),
                )
                artifacts_deleted = cursor.rowcount
                cursor.execute(
                    f"DELETE FROM {self._qualified_runs()} "
                    "WHERE namespace = %s AND started_millis < %s",
                    (target, before_millis),
                )
            else:
                cursor.execute(
                    f"DELETE FROM {self._qualified_artifacts()} AS a "
                    "USING " + self._qualified_runs() + " AS r "
                    "WHERE a.namespace = r.namespace "
                    "AND a.run_key = r.run_key "
                    "AND r.started_millis < %s",
                    (before_millis,),
                )
                artifacts_deleted = cursor.rowcount
                cursor.execute(
                    f"DELETE FROM {self._qualified_runs()} "
                    "WHERE started_millis < %s",
                    (before_millis,),
                )
            runs_deleted = cursor.rowcount
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return int(artifacts_deleted or 0), int(runs_deleted or 0)

    def run_scope(self, run_key: int, identity=None):
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(
                f"INSERT INTO {self._qualified_runs()} "
                "(namespace, run_key, dataset, started_millis, status, identity) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (namespace, run_key) DO NOTHING",
                (
                    str(self._namespace),
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
            self._active = (connection, cursor, run_key)
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

    def save(self, df, metric_type: str, key: int | None = None) -> WriteResult:
        """Persist one artifact under the authoritative run key.

        Inside a run scope the scope's run key is authoritative and the
        caller's key is advisory: all artifacts of one run join that run
        regardless of engine-side timestamps (review finding F01). Outside
        a scope an explicit key is required.
        """
        rows = [row.asDict(recursive=True) for row in df.collect()]
        if self._active is not None:
            connection, cursor, run_key = self._active
            self._insert_artifacts(cursor, metric_type, run_key, rows)
            return WriteResult(
                metric_type, run_key, (f"postgres:{self._qualified_artifacts()}",)
            )
        if key is None:
            raise RepositoryError(
                "sinks outside a run scope require an explicit run key"
            )
        connection = self._connect()
        try:
            cursor = connection.cursor()
            self._insert_artifacts(cursor, metric_type, int(key), rows)
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
            "(namespace, run_key, artifact_type, seq, dataset, payload) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (namespace, run_key, artifact_type, seq) DO NOTHING"
        )
        for sequence, row in enumerate(rows):
            cursor.execute(
                statement,
                (
                    str(self._namespace),
                    int(key),
                    metric_type,
                    sequence,
                    self._dataset,
                    json.dumps(row),
                ),
            )

    def _qualified_runs(self) -> str:
        return f"{self._schema.quoted_pg}.{self._runs_table.quoted_pg}"

    def _qualified_artifacts(self) -> str:
        return f"{self._schema.quoted_pg}.{self._artifacts_table.quoted_pg}"

    @property
    def dataset(self) -> str:
        return self._dataset
