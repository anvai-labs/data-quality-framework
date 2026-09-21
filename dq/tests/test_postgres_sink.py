# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Durable PostgreSQL sink contracts over a fake DB-API connection (ADR-006).

No driver is imported: the sink works against any DB-API connection, and
these tests prove statement text, quoted identifiers, transaction
lifecycle, and idempotent statement shapes with a recording fake.
"""

import json

import pytest

from dq.exceptions import ConfigurationError
from dq.postgres_sink import DEFAULT_DSN_VARIABLE, PostgresOutcomeSink


class FakeCursor:
    def __init__(self, executed):
        self._executed = executed

    def execute(self, statement, params=None):
        self._executed.append((statement, params))


class FakeConnection:
    def __init__(self, executed):
        self.executed = executed
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self.executed)

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
        self.connections = []

    def __call__(self):
        connection = FakeConnection(self.executed)
        self.connections.append(connection)
        return connection


@pytest.fixture
def factory():
    return ConnectionFactory()


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


def test_ddl_statements_use_quoted_pg_identifiers():
    sink = PostgresOutcomeSink(lambda: None, dataset="bars", schema="dq")
    statements = sink.ddl_statements()
    assert 'CREATE TABLE IF NOT EXISTS "dq"."runs"' in statements[0]
    assert 'CREATE TABLE IF NOT EXISTS "dq"."artifacts"' in statements[1]
    assert "REFERENCES" in statements[1]


def test_save_outside_scope_autocommits_one_transaction(factory):
    sink = PostgresOutcomeSink(factory, dataset="bars")
    result = sink.save(FakeDataFrame([{"instance": "x", "value": 1}]), "metrics", 42)
    connection = factory.connections[0]
    assert connection.commits == 1
    assert connection.closed is True
    assert result.targets == ('postgres:"dq"."artifacts"',)
    statement, params = factory.executed[0]
    assert statement.startswith('INSERT INTO "dq"."artifacts"')
    assert "ON CONFLICT (run_key, artifact_type, seq) DO NOTHING" in statement
    assert params[0] == 42
    assert params[1] == "metrics"
    assert params[3] == "bars"
    assert json.loads(params[4]) == {"instance": "x", "value": 1}


def test_run_scope_commits_identity_and_artifacts_together(factory):
    sink = PostgresOutcomeSink(factory, dataset="bars")
    identity = {"dataset_sha256": "a" * 64}
    with sink.run_scope(1234, identity):
        sink.save(FakeDataFrame([{"value": 1}]), "metrics", 1234)
        sink.save(FakeDataFrame([{"value": 0}]), "verifications", 1234)
    assert len(factory.connections) == 1, "all saves join one connection"
    connection = factory.connections[0]
    assert connection.commits == 1
    assert connection.closed is True
    run_statement, run_params = factory.executed[0]
    assert run_statement.startswith('INSERT INTO "dq"."runs"')
    assert json.loads(run_params[4]) == identity
    artifact_inserts = [
        params
        for statement, params in factory.executed
        if statement.startswith('INSERT INTO "dq"."artifacts"')
    ]
    assert len(artifact_inserts) == 2


def test_run_scope_rolls_back_on_failure(factory):
    sink = PostgresOutcomeSink(factory, dataset="bars")
    with pytest.raises(RuntimeError, match="engine exploded"):
        with sink.run_scope(1234):
            raise RuntimeError("engine exploded")
    connection = factory.connections[0]
    assert connection.rollbacks == 1
    assert connection.commits == 0
    assert connection.closed is True


def test_run_scopes_are_independent(factory):
    sink = PostgresOutcomeSink(factory, dataset="bars")
    with sink.run_scope(1):
        pass
    with sink.run_scope(2):
        pass
    assert len(factory.connections) == 2


def test_psycopg_is_imported_lazily_at_connect_time(monkeypatch):
    """The framework never imports psycopg; the deployment's connect does."""
    import sys
    from dq.sinks import sink_from_config

    released = []

    class FakePsycopg:
        @staticmethod
        def connect(dsn):
            released.append(dsn)
            return "sentinel-connection"

    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)
    sink = PostgresOutcomeSink.from_config(
        {
            "type": "postgres",
            "dataset": "bars",
            "schema": "dq",
            "dsn_env": "DQ_POSTGRES_DSN",
        },
        env={DEFAULT_DSN_VARIABLE: "postgresql://dq"},
    )
    assert sink._connect() == "sentinel-connection"
    assert released == ["postgresql://dq"]
    assert "psycopg" not in sys.modules or sys.modules["psycopg"] is FakePsycopg


def test_sink_from_config_dispatches_postgres_type(monkeypatch):
    import sys
    from dq.sinks import sink_from_config

    class FakePsycopg:
        @staticmethod
        def connect(dsn):
            return "sentinel-connection"

    monkeypatch.setitem(sys.modules, "psycopg", FakePsycopg)
    monkeypatch.setenv("DQ_POSTGRES_DSN", "postgresql://dq")
    sink = sink_from_config(
        {
            "type": "postgres",
            "dataset": "bars",
            "dsn_env": "DQ_POSTGRES_DSN",
        },
    )
    from dq.postgres_sink import PostgresOutcomeSink

    assert isinstance(sink, PostgresOutcomeSink)


def test_sink_from_config_rejects_unknown_types():
    from dq.exceptions import ConfigurationError
    from dq.sinks import sink_from_config

    with pytest.raises(ConfigurationError, match="sink type"):
        sink_from_config({"type": "oracle", "dataset": "bars"})


def test_save_failure_outside_scope_rolls_back_and_closes(factory):
    sink = PostgresOutcomeSink(factory, dataset="bars")

    class ExplodingCursor:
        def execute(self, statement, params=None):
            raise RuntimeError("database unavailable")

    class ExplodingConnection:
        def cursor(self):
            return ExplodingCursor()

        def commit(self):
            raise AssertionError("a failed insert must not commit")

        def rollback(self):
            self.rolled_back = True

        def close(self):
            self.closed = True

        closed = False

    connection = ExplodingConnection()
    factory.connections.append(connection)
    sink._connect = lambda: connection
    with pytest.raises(RuntimeError, match="database unavailable"):
        sink.save(FakeDataFrame([{"value": 1}]), "metrics", 42)
    assert connection.rolled_back is True
    assert connection.closed is True
