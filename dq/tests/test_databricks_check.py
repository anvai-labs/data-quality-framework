from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("databricks.sdk")
from databricks.sdk.service.jobs import RunResultState, RunType

from dq.engine.databricks.databricks_check import DatabricksCheck


def test_uses_unified_auth_without_a_static_token():
    with patch(
        "dq.engine.databricks.databricks_check.WorkspaceClient"
    ) as workspace_client:
        DatabricksCheck(databricks_url="https://workspace.example.com/")

    workspace_client.assert_called_once_with(host="https://workspace.example.com")


def test_preserves_explicit_token_compatibility_without_storing_it():
    with patch(
        "dq.engine.databricks.databricks_check.WorkspaceClient"
    ) as workspace_client:
        check = DatabricksCheck("secret-token", "https://workspace.example.com")

    workspace_client.assert_called_once_with(
        host="https://workspace.example.com", token="secret-token"
    )
    assert not hasattr(check, "_databricks_token")


@pytest.mark.parametrize(
    "url",
    [
        "",
        "http://workspace.example.com",
        "https:///missing-host",
        "https://user@workspace.example.com",
        "https://workspace.example.com/path",
        "https://workspace.example.com?token=secret",
        "https://workspace.example.com#fragment",
    ],
)
def test_rejects_unsafe_workspace_urls(url):
    with pytest.raises(ValueError):
        DatabricksCheck(databricks_url=url)


@pytest.mark.parametrize("token", ["", " ", "line\nbreak", "two words"])
def test_rejects_unsafe_tokens(token):
    with pytest.raises(ValueError):
        DatabricksCheck(token, "https://workspace.example.com")


def test_rejects_ambiguous_client_and_credentials():
    with pytest.raises(ValueError):
        DatabricksCheck(
            "secret-token", "https://workspace.example.com", workspace_client=Mock()
        )


def test_injected_client_records_positive_missed_invocations():
    scheduled_job = SimpleNamespace(
        creator_user_name="owner@example.com",
        job_id=17,
        settings=SimpleNamespace(
            schedule=SimpleNamespace(quartz_cron_expression="0 0 * * * ?")
        ),
    )
    successful_run = SimpleNamespace(
        run_id=23,
        run_duration=42,
        execution_duration=40,
        run_name="daily-quality",
        state=SimpleNamespace(result_state=RunResultState.SUCCESS),
    )
    failed_run = SimpleNamespace(
        run_id=24,
        run_duration=10,
        execution_duration=8,
        run_name="daily-quality",
        state=None,
    )
    unscheduled_job = SimpleNamespace(
        creator_user_name="owner@example.com",
        job_id=16,
        settings=SimpleNamespace(schedule=None),
    )
    other_owner_job = SimpleNamespace(
        creator_user_name="other@example.com",
        job_id=15,
        settings=SimpleNamespace(schedule=None),
    )
    jobs = Mock()
    jobs.list.return_value = [other_owner_job, unscheduled_job, scheduled_job]
    jobs.list_runs.return_value = [successful_run, failed_run]
    workspace = SimpleNamespace(jobs=jobs)
    spark = _FakeSpark()

    with (
        patch("dq.engine.databricks.databricks_check.QuartzCron") as quartz_cron,
        patch("dq.engine.databricks.databricks_check.F", _FakeFunctions),
    ):
        quartz_cron.return_value.next_triggers.return_value = ["one", "two", "three"]
        DatabricksCheck(workspace_client=workspace).check_scheduled_jobs_timeliness(
            spark,
            "sales",
            ["daily-quality"],
            "owner@example.com",
            1_700_000_000_000,
        )

    assert spark.rows[-1] == [
        "daily-quality",
        "Job id: 17, expected to run 3 but actual run was 2",
        "Timeliness.MissedInvocations",
        1,
    ]
    assert spark.frame.writer.saved_table == "sales_dq.dq_metrics"
    jobs.list_runs.assert_called_once_with(
        job_id=17, start_time_from=1_700_000_000_000, run_type=RunType.JOB_RUN
    )


class _FakeFunctions:
    @staticmethod
    def lit(value):
        return value

    @staticmethod
    def from_unixtime(value):
        return value

    @staticmethod
    def year(value):
        return value


class _FakeWriter:
    def __init__(self):
        self.saved_table = None

    def mode(self, _value):
        return self

    def format(self, _value):
        return self

    def option(self, _key, _value):
        return self

    def saveAsTable(self, table):
        self.saved_table = table


class _FakeFrame:
    def __init__(self):
        self.writer = _FakeWriter()

    @property
    def write(self):
        return self.writer

    def withColumn(self, _name, _value):
        return self

    def coalesce(self, _partitions):
        return self


class _FakeSpark:
    def __init__(self):
        self.rows = None
        self.frame = _FakeFrame()
        self.catalog = SimpleNamespace(tableExists=lambda _table: False)

    def createDataFrame(self, rows, _columns):
        self.rows = rows
        return self.frame


pytestmark = pytest.mark.spark
