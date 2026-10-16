"""Daily freshness monitor for the raw data.

The retail DAG reloads the raw table every week. Checking freshness inside
that DAG would always pass (the table was just written); this separate daily
DAG detects that the weekly load did not happen. Thresholds are in the dbt
source definition (warn after 8 days, error after 15 days).
"""

from datetime import datetime, timedelta

from airflow.decorators import dag
from cosmos.operators.local import DbtSourceLocalOperator

from include.dbt.cosmos_config import (
    DBT_EXECUTION_CONFIG,
    DBT_PROFILE_CONFIG,
    DBT_PROJECT_PATH,
)
from include.monitoring.alerting import notify_dag_failure


@dag(
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["retail", "monitoring"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    on_failure_callback=notify_dag_failure,
)
def retail_freshness():
    DbtSourceLocalOperator(
        task_id="raw_source_freshness",
        project_dir=str(DBT_PROJECT_PATH),
        profile_config=DBT_PROFILE_CONFIG,
        dbt_executable_path=DBT_EXECUTION_CONFIG.dbt_executable_path,
        install_deps=True,
        env={"PYTHONPATH": ""},
    )


retail_freshness()
