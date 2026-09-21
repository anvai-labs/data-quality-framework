# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Durable PostgreSQL sink contracts over a fake DB-API connection (ADR-006).

No driver is imported: the sink works against any DB-API connection, and
these tests prove statement text, quoted identifiers, transaction
lifecycle, namespace scoping, retention pruning, and idempotent statement
shapes with a recording fake.
"""

import json

import pytest

from dq.exceptions import ConfigurationError, RepositoryError
from dq.postgres_sink import DEFAULT_DSN_VARIABLE, PostgresOutcomeSink


class FakeCursor:
    def __init__(self, factory):
        self.factory = factory
        self.rowcount = 0

    def execute(self, statement, params=None):
        self.factory.executed.append((statement, params))

    def fetchone(self):
        return self.factory.results.pop(0) if self.factory.results else (0,)


class FakeConnection:
    def __init__(self, factory):
        self.factory = factory
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self.factory)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FakeRow:
    def __init__(self, values):
        self._values = values

    def asDict(self, recursive=True):
        return dict(self._values)


class FakeDataFrame:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return [FakeRow(row) for row in self._rows]


class ConnectionFactory:
    """Creates fake connections and records them for lifecycle assertions."""

    def __init__(self):
        self.executed = []
        self.results = []
        self.connections = []

    def __call__(self):
        connection = FakeConnection(self)
        self.connections.append(connection)
        return connection

    def append_result(self, *values):
        self.results.append(values)


@pytest.fixture
def factory():
    return ConnectionFactory()


def make_sink(factory, **overrides):
    settings = {"dataset": "bars", "retention_days": 30}
    settings.update(overrides)
    return PostgresOutcomeSink(factory, **settings)


def delete_calls(executed):
    return [
        (statement, params)
        for statement, params in executed
        if statement.startswith("DELETE")
    ]


def test_constructs_from_environment_dsn_and_fails_closed_without_it():
    config = {"type": "postgres", "dataset": "bars", "schema": "dq"}
    with pytest.raises(ConfigurationError, match="DQ_POSTGRES_DSN"):
        PostgresOutcomeSink.from_config(config, env={})
    sink = PostgresOutcomeSink.from_config(
        config, env={DEFAULT_DSN_VARIABLE: "postgresql://dq"}
    )
    assert sink.dataset == "bars"


def test_config_requires_dataset():
    with pytest.raises(ConfigurationError, match="dataset name is required"):
        PostgresOutcomeSink.from_config(
            {"type": "postgres"}, env={DEFAULT_DSN_VARIABLE: "postgresql://dq"}
        )


def test_ddl_statements_use_quoted_pg_identifiers_namespaces_and_views():
    sink = PostgresOutcomeSink(lambda: None, dataset="bars", schema="dq")
    statements = sink.ddl_statements()
    assert "namespace TEXT NOT NULL DEFAULT 'default'" in statements[0]
    assert "PRIMARY KEY (namespace, run_key)" in statements[0]
    assert "PRIMARY KEY (namespace, run_key, artifact_type, seq)" in statements[1]
    assert "runs_completed" in statements[2]
    assert "latest_per_dataset" in statements[3]


def test_verify_schema_reports_counts_and_fails_closed(factory):
    sink = make_sink(factory)
    factory.results.extend([(1,), (1,), (3,), (7,)])
    assert sink.verify_schema() == {"runs": 3, "artifacts": 7}

    missing = ConnectionFactory()
    missing.append_result((0,))
    sink_missing = PostgresOutcomeSink(missing, dataset="bars")
    with pytest.raises(RepositoryError, match="does not exist"):
        sink_missing.verify_schema()


def test_prune_requires_explicit_retention_configuration(factory):
    sink = make_sink(factory)
    sink._retention_days = None
    with pytest.raises(RepositoryError, match="retention_days"):
        sink.prune(before_millis=1234)


def test_prune_is_namespace_scoped_and_deletes_artifacts_first(factory):
    sink = make_sink(factory, namespace="team_a")
    sink.prune(before_millis=1000)
    delete_statements = delete_calls(factory.executed)
    assert len(delete_statements) == 2
    artifacts_statement, artifacts_params = delete_statements[0]
    assert artifacts_statement.startswith('DELETE FROM "dq"."artifacts"')
    assert artifacts_params[0] == "team_a"
    runs_statement, runs_params = delete_statements[1]
    assert runs_statement.startswith('DELETE FROM "dq"."runs"')
    assert runs_params[0] == "team_a"


def test_prune_scoping_never_touches_other_namespaces(factory):
    sink = PostgresOutcomeSink(
        factory, dataset="bars", namespace="team_a", retention_days=30
    )
    sink.prune(before_millis=1000, namespace="team_a")
    delete_params = [params for _, params in delete_calls(factory.executed)]
    assert all(
        params[0] == "team_a" for params in delete_params
    ), "a namespace-scoped prune must never touch other namespaces"


def test_save_stamps_namespace_and_artifact_columns(factory):
    sink = PostgresOutcomeSink(factory, dataset="bars", namespace="team_a")
    result = sink.save(FakeDataFrame([{"instance": "x", "value": 1}]), "metrics", 42)
    statement, params = factory.executed[0]
    assert statement.startswith('INSERT INTO "dq"."artifacts"')
    assert "namespace" in statement.split("VALUES")[0]
    assert params[0] == "team_a"
    assert params[2] == "metrics"
    assert params[4] == "bars"
    assert json.loads(params[5]) == {"instance": "x", "value": 1}
    assert result.targets == ('postgres:"dq"."artifacts"',)


def test_run_scope_stamps_namespace_and_commits(factory):
    sink = PostgresOutcomeSink(factory, dataset="bars", namespace="team_a")
    identity = {"dataset_sha256": "a" * 64}
    with sink.run_scope(1234, identity):
        sink.save(FakeDataFrame([{"value": 1}]), "metrics", 1234)
    connection = factory.connections[0]
    assert connection.commits == 1
    assert connection.closed is True
    run_statement, run_params = factory.executed[0]
    assert run_statement.startswith('INSERT INTO "dq"."runs"')
    assert run_params[0] == "team_a"
    assert json.loads(run_params[5]) == identity


def test_psycopg_is_imported_lazily_at_connect_time(monkeypatch):
    import sys

    class FakePsycopg:
        @staticmethod
        def connect(dsn):
            return "sentinel-connection"

    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)
    sink = PostgresOutcomeSink.from_config(
        {
            "type": "postgres",
            "dataset": "bars",
            "dsn_env": "DQ_POSTGRES_DSN",
        },
        env={DEFAULT_DSN_VARIABLE: "postgresql://dq"},
    )
    assert sink._connect() == "sentinel-connection"


def test_sink_from_config_dispatches_postgres_type(monkeypatch):
    import sys

    class FakePsycopg:
        @staticmethod
        def connect(dsn):
            return "sentinel-connection"

    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)
    monkeypatch.setenv("DQ_POSTGRES_DSN", "postgresql://dq")
    from dq.postgres_sink import PostgresOutcomeSink as Implementation
    from dq.sinks import sink_from_config

    sink = sink_from_config(
        {
            "type": "postgres",
            "dataset": "bars",
            "dsn_env": "DQ_POSTGRES_DSN",
        },
    )
    assert isinstance(sink, Implementation)


def test_retention_days_must_be_positive(factory):
    with pytest.raises(ConfigurationError, match="positive integer"):
        PostgresOutcomeSink(factory, dataset="bars", retention_days=0)


def test_prune_derives_cutoff_from_retention_days(factory):
    import time

    sink = PostgresOutcomeSink(factory, dataset="bars", retention_days=7)
    before = time.time_ns() // 1_000_000
    sink.prune(namespace="team_a")
    cutoff_statement = [
        params
        for statement, params in factory.executed
        if statement.startswith("DELETE") and "started_millis <" in statement
    ][0]
    assert cutoff_statement[-1] <= before


def test_prune_all_namespaces_drops_the_namespace_filter(factory):
    sink = PostgresOutcomeSink(factory, dataset="bars", retention_days=7)
    sink.prune(before_millis=1000, all_namespaces=True)
    delete_calls_all = delete_calls(factory.executed)
    assert len(delete_calls_all) == 2
    # Correlation joins runs to their own artifacts; no namespace literal
    # may appear as a filter parameter — the cutoff is the only bound.
    assert all(params == (1000,) for _, params in delete_calls_all)


def test_verify_schema_wraps_database_failures(factory):
    class ExplodingCursor:
        def execute(self, statement, params=None):
            raise RuntimeError("connection reset")

    class ExplodingConnection:
        def cursor(self):
            return ExplodingCursor()

        def rollback(self):
            pass

        def close(self):
            pass

    sink = PostgresOutcomeSink(lambda: ExplodingConnection(), dataset="bars")
    with pytest.raises(RepositoryError, match="evidence schema check failed"):
        sink.verify_schema()
