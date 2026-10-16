"""Pipeline health: one row per DAG run in <project>.retail_monitoring.pipeline_runs.

The functions here are pure (no Airflow, no network) so they can be unit
tested; the DAG task gathers the inputs and runs the SQL.
"""

from dataclasses import dataclass
from typing import Iterable, Protocol

MONITORING_DATASET = "retail_monitoring"
RUNS_TABLE = "pipeline_runs"
FAILED_STATES = frozenset({"failed", "upstream_failed"})


class TaskInstanceLike(Protocol):
    task_id: str
    state: str | None


@dataclass(frozen=True)
class TaskSummary:
    tasks_total: int
    tasks_failed: int
    failed_task_ids: tuple[str, ...]
    dbt_test_tasks_passed: int
    dbt_test_tasks_failed: int
    soda_scans_passed: int
    soda_scans_failed: int

    @property
    def run_failed(self) -> bool:
        return self.tasks_failed > 0


def summarize_task_states(
    task_instances: Iterable[TaskInstanceLike], exclude_task_id: str
) -> TaskSummary:
    """Count task outcomes of a DAG run, leaving out the reporting task itself.

    tasks_failed counts failed and upstream_failed tasks; failed_task_ids only
    lists the tasks that actually failed (the root causes).
    """
    tasks = [ti for ti in task_instances if ti.task_id != exclude_task_id]
    dbt_tests = [ti for ti in tasks if ti.task_id.endswith(".test")]
    soda_scans = [ti for ti in tasks if ti.task_id.startswith("check_")]
    return TaskSummary(
        tasks_total=len(tasks),
        tasks_failed=sum(ti.state in FAILED_STATES for ti in tasks),
        failed_task_ids=tuple(
            sorted(ti.task_id for ti in tasks if ti.state == "failed")
        ),
        dbt_test_tasks_passed=sum(ti.state == "success" for ti in dbt_tests),
        dbt_test_tasks_failed=sum(ti.state == "failed" for ti in dbt_tests),
        soda_scans_passed=sum(ti.state == "success" for ti in soda_scans),
        soda_scans_failed=sum(ti.state == "failed" for ti in soda_scans),
    )


def runs_table(project_id: str) -> str:
    return f"`{project_id}.{MONITORING_DATASET}.{RUNS_TABLE}`"


def create_runs_table_sql(project_id: str, location: str) -> str:
    return f"""
        CREATE SCHEMA IF NOT EXISTS `{project_id}.{MONITORING_DATASET}`
        OPTIONS (location = '{location}', description = 'Pipeline observability');

        CREATE TABLE IF NOT EXISTS {runs_table(project_id)} (
            dag_run_id STRING NOT NULL,
            logical_date TIMESTAMP,
            run_started_at TIMESTAMP,
            recorded_at TIMESTAMP NOT NULL,
            state STRING NOT NULL,
            tasks_total INT64,
            tasks_failed INT64,
            failed_task_ids ARRAY<STRING>,
            dbt_test_tasks_passed INT64,
            dbt_test_tasks_failed INT64,
            soda_scans_passed INT64,
            soda_scans_failed INT64,
            soda_checks_warned INT64,
            raw_rows INT64,
            fct_rows INT64
        )
        OPTIONS (description = 'One row per run of the retail DAG, written by its last task');
    """


def insert_run_sql(project_id: str) -> str:
    """Parameterised INSERT: values are passed as query parameters, never formatted in."""
    return f"""
        INSERT INTO {runs_table(project_id)} (
            dag_run_id, logical_date, run_started_at, recorded_at, state,
            tasks_total, tasks_failed, failed_task_ids,
            dbt_test_tasks_passed, dbt_test_tasks_failed,
            soda_scans_passed, soda_scans_failed, soda_checks_warned,
            raw_rows, fct_rows
        )
        VALUES (
            @dag_run_id, @logical_date, @run_started_at, CURRENT_TIMESTAMP(), @state,
            @tasks_total, @tasks_failed, @failed_task_ids,
            @dbt_test_tasks_passed, @dbt_test_tasks_failed,
            @soda_scans_passed, @soda_scans_failed, @soda_checks_warned,
            @raw_rows, @fct_rows
        )
    """
