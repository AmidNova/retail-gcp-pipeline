"""Unit tests for the monitoring helpers (no Airflow run, no network)."""

from dataclasses import dataclass
from unittest import mock

import pytest

from include.monitoring import alerting
from include.monitoring.pipeline_health import insert_run_sql, summarize_task_states

REPORTING_TASK = "record_pipeline_health"


@dataclass
class FakeTaskInstance:
    task_id: str
    state: str | None


def test_summary_of_a_successful_run():
    summary = summarize_task_states(
        [
            FakeTaskInstance("load_raw", "success"),
            FakeTaskInstance("check_raw", "success"),
            FakeTaskInstance("transform.dim_dates.run", "success"),
            FakeTaskInstance("transform.dim_dates.test", "success"),
            FakeTaskInstance(REPORTING_TASK, "running"),
        ],
        exclude_task_id=REPORTING_TASK,
    )

    assert summary.tasks_total == 4
    assert summary.tasks_failed == 0
    assert summary.dbt_test_tasks_passed == 1
    assert summary.soda_scans_passed == 1
    assert not summary.run_failed


def test_summary_lists_root_failures_and_counts_downstream_ones():
    summary = summarize_task_states(
        [
            FakeTaskInstance("check_raw", "failed"),
            FakeTaskInstance("transform.dim_dates.run", "upstream_failed"),
            FakeTaskInstance("transform.dim_dates.test", "upstream_failed"),
        ],
        exclude_task_id=REPORTING_TASK,
    )

    assert summary.run_failed
    assert summary.tasks_failed == 3
    assert summary.failed_task_ids == ("check_raw",)
    assert summary.soda_scans_failed == 1
    assert summary.dbt_test_tasks_failed == 0


def test_failed_dbt_test_task_is_counted():
    summary = summarize_task_states(
        [FakeTaskInstance("report.rpt_monthly_revenue.test", "failed")],
        exclude_task_id=REPORTING_TASK,
    )

    assert summary.dbt_test_tasks_failed == 1
    assert summary.failed_task_ids == ("report.rpt_monthly_revenue.test",)


def test_insert_uses_query_parameters_only():
    sql = insert_run_sql("my-project")

    assert "@dag_run_id" in sql
    assert "`my-project.retail_monitoring.pipeline_runs`" in sql


def test_failure_message_names_the_failed_tasks():
    message = alerting.failure_message(
        "retail", "manual__1", ["check_raw", "load_raw"], "http://logs"
    )

    assert "check_raw, load_raw" in message
    assert "`retail`" in message


def test_slack_webhook_must_be_a_slack_url():
    with pytest.raises(ValueError):
        alerting.post_to_slack("https://example.com/hook", "text")


def _failure_context():
    dag_run = mock.Mock(dag_id="retail", run_id="manual__1")
    dag_run.get_task_instances.return_value = [FakeTaskInstance("check_raw", "failed")]
    return {"dag_run": dag_run, "task_instance": mock.Mock(log_url="http://logs")}


def test_callback_without_webhook_only_logs(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with mock.patch.object(alerting, "post_to_slack") as post:
        alerting.notify_dag_failure(_failure_context())

    post.assert_not_called()


def test_callback_never_raises_when_slack_is_down(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/x")
    with mock.patch.object(alerting, "post_to_slack", side_effect=OSError("down")):
        alerting.notify_dag_failure(_failure_context())
