"""Retail ELT pipeline, weekly.

CSV -> GCS -> BigQuery raw -> Soda gate -> dbt (staging, intermediate, marts)
-> Soda -> dbt reports -> Soda -> dbt ML features -> BigQuery ML
-> pipeline health row (always runs, even after a failure).

Every environment-specific value comes from env vars (see .env.example).
"""

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.constants import LoadMode

from include.dbt.cosmos_config import (
    AIRFLOW_HOME,
    DBT_EXECUTION_CONFIG,
    DBT_PROFILE_CONFIG,
    DBT_PROJECT_CONFIG,
)
from include.monitoring.alerting import notify_dag_failure
from include.monitoring.pipeline_health import (
    create_runs_table_sql,
    insert_run_sql,
    summarize_task_states,
)

GCP_CONN_ID = "gcp"
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BUCKET = os.environ["GCS_BUCKET"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
RAW_DATASET = os.environ.get("BQ_RAW_DATASET", "retail_raw")
# dbt prod target writes <dataset>_<layer>: see profiles.yml / dbt_project.yml
ML_DATASET = "retail_ml"

RAW_TABLE = f"{PROJECT_ID}.{RAW_DATASET}.online_retail"
FCT_TABLE = f"{PROJECT_ID}.retail_marts.fct_invoice_lines"
SODA_TASK_IDS = ["check_raw", "check_marts", "check_reports"]

LOCAL_CSV = str(AIRFLOW_HOME / "include" / "dataset" / "online_retail.csv")
GCS_OBJECT = "raw/online_retail.csv"
SODA_PYTHON = str(AIRFLOW_HOME / "soda_venv" / "bin" / "python")

# Explicit raw schema: no type inference, and a malformed file fails the load
# instead of landing as NULLs.
RAW_SCHEMA = [
    {"name": "Invoice", "type": "STRING", "mode": "REQUIRED"},
    {"name": "StockCode", "type": "STRING"},
    {"name": "Description", "type": "STRING"},
    {"name": "Quantity", "type": "INT64"},
    {"name": "InvoiceDate", "type": "STRING", "mode": "REQUIRED"},
    {"name": "Price", "type": "NUMERIC"},
    {"name": "Customer_id", "type": "FLOAT64"},
    {"name": "Country", "type": "STRING"},
]


def bigquery_job(task_id: str, sql: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={"query": {"query": sql, "useLegacySql": False}},
    )


def dbt_group(group_id: str, paths: list[str]) -> DbtTaskGroup:
    return DbtTaskGroup(
        group_id=group_id,
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_PROFILE_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=[f"path:{path}" for path in paths],
            # A test with several parents (relationships, reconciliation) gets
            # its own task, run once all its parents are built.
            should_detach_multiple_parents_tests=True,
        ),
    )


def ml_table(name: str) -> str:
    return f"`{PROJECT_ID}.{ML_DATASET}.{name}`"


def table_row_count(client, table_id: str) -> int | None:
    """Row count from table metadata (free, no query). None if the table is missing."""
    from google.api_core.exceptions import NotFound

    try:
        return client.get_table(table_id).num_rows
    except NotFound:
        return None


@dag(
    start_date=datetime(2026, 10, 1),
    schedule="@weekly",
    catchup=False,
    # Runs rewrite the same tables: never two at once.
    max_active_runs=1,
    tags=["retail"],
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    # One alert per failed run, not one per task and per retry.
    on_failure_callback=notify_dag_failure,
)
def retail():
    ensure_monitoring_table = bigquery_job(
        "ensure_monitoring_table", create_runs_table_sql(PROJECT_ID, BQ_LOCATION)
    )

    create_raw_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_raw_dataset",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        dataset_id=RAW_DATASET,
        location=BQ_LOCATION,
        if_exists="ignore",
    )

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        gcp_conn_id=GCP_CONN_ID,
        src=LOCAL_CSV,
        dst=GCS_OBJECT,
        bucket=BUCKET,
        mime_type="text/csv",
    )

    load_raw = GCSToBigQueryOperator(
        task_id="load_raw",
        gcp_conn_id=GCP_CONN_ID,
        bucket=BUCKET,
        source_objects=[GCS_OBJECT],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW_DATASET}.online_retail",
        schema_fields=RAW_SCHEMA,
        source_format="CSV",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        max_bad_records=0,
        write_disposition="WRITE_TRUNCATE",
        location=BQ_LOCATION,
    )

    @task.external_python(python=SODA_PYTHON)
    def check_raw():
        from include.soda.check_function import check

        return check("check_raw", "raw", "retail_raw")

    transform = dbt_group(
        "transform",
        ["seeds", "models/staging", "models/intermediate", "models/marts"],
    )

    @task.external_python(python=SODA_PYTHON)
    def check_marts():
        from include.soda.check_function import check

        return check("check_marts", "marts", "retail_marts")

    report = dbt_group("report", ["models/reports"])

    @task.external_python(python=SODA_PYTHON)
    def check_reports():
        from include.soda.check_function import check

        return check("check_reports", "reports", "retail_reports")

    ml_features = dbt_group("ml_features", ["models/ml"])

    # BigQuery ML is BigQuery-specific, so it stays outside dbt; outputs use a
    # bqml_ prefix in the ML dataset.
    train_revenue_forecast = bigquery_job(
        "train_revenue_forecast",
        f"""
        CREATE OR REPLACE MODEL {ml_table("bqml_revenue_forecast_model")}
        OPTIONS(
            model_type = 'ARIMA_PLUS',
            time_series_timestamp_col = 'date_day',
            time_series_data_col = 'revenue',
            data_frequency = 'DAILY',
            holiday_region = 'GB',
            auto_arima = TRUE
        ) AS
        SELECT date_day, revenue FROM {ml_table("ml_daily_revenue")}
        """,
    )

    forecast_revenue = bigquery_job(
        "forecast_revenue",
        f"""
        CREATE OR REPLACE TABLE {ml_table("bqml_revenue_forecast")} AS
        SELECT
            DATE(forecast_timestamp) AS date_day,
            forecast_value AS forecast_revenue,
            prediction_interval_lower_bound AS lower_bound,
            prediction_interval_upper_bound AS upper_bound
        FROM ML.FORECAST(
            MODEL {ml_table("bqml_revenue_forecast_model")},
            STRUCT(30 AS horizon, 0.8 AS confidence_level)
        )
        """,
    )

    train_customer_segments = bigquery_job(
        "train_customer_segments",
        f"""
        CREATE OR REPLACE MODEL {ml_table("bqml_customer_segments_model")}
        OPTIONS(model_type = 'KMEANS', num_clusters = 4, standardize_features = TRUE) AS
        SELECT recency_days, frequency, monetary FROM {ml_table("ml_customer_rfm")}
        """,
    )

    score_customer_segments = bigquery_job(
        "score_customer_segments",
        f"""
        CREATE OR REPLACE TABLE {ml_table("bqml_customer_segments")} AS
        SELECT
            customer_id,
            recency_days,
            frequency,
            monetary,
            CAST(CENTROID_ID AS STRING) AS segment_id
        FROM ML.PREDICT(
            MODEL {ml_table("bqml_customer_segments_model")},
            TABLE {ml_table("ml_customer_rfm")}
        )
        """,
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def record_pipeline_health(**context):
        """Write one row to retail_monitoring.pipeline_runs, then fail the run if
        any task failed (this task is the only leaf, so it decides the run state)."""
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud import bigquery

        dag_run, ti = context["dag_run"], context["ti"]
        summary = summarize_task_states(
            dag_run.get_task_instances(), exclude_task_id=ti.task_id
        )
        soda_results = ti.xcom_pull(task_ids=SODA_TASK_IDS) or []
        soda_checks_warned = sum(result["warned"] for result in soda_results if result)

        client = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION).get_client(
            project_id=PROJECT_ID
        )
        param = bigquery.ScalarQueryParameter
        parameters = [
            param("dag_run_id", "STRING", dag_run.run_id),
            param("logical_date", "TIMESTAMP", dag_run.logical_date),
            param("run_started_at", "TIMESTAMP", dag_run.start_date),
            param("state", "STRING", "failed" if summary.run_failed else "success"),
            param("tasks_total", "INT64", summary.tasks_total),
            param("tasks_failed", "INT64", summary.tasks_failed),
            bigquery.ArrayQueryParameter(
                "failed_task_ids", "STRING", list(summary.failed_task_ids)
            ),
            param("dbt_test_tasks_passed", "INT64", summary.dbt_test_tasks_passed),
            param("dbt_test_tasks_failed", "INT64", summary.dbt_test_tasks_failed),
            param("soda_scans_passed", "INT64", summary.soda_scans_passed),
            param("soda_scans_failed", "INT64", summary.soda_scans_failed),
            param("soda_checks_warned", "INT64", soda_checks_warned),
            param("raw_rows", "INT64", table_row_count(client, RAW_TABLE)),
            param("fct_rows", "INT64", table_row_count(client, FCT_TABLE)),
        ]
        client.query(
            insert_run_sql(PROJECT_ID),
            job_config=bigquery.QueryJobConfig(query_parameters=parameters),
            location=BQ_LOCATION,
        ).result()

        if summary.run_failed:
            raise AirflowFailException(
                f"Failed tasks: {', '.join(summary.failed_task_ids)}"
            )

    chain(
        ensure_monitoring_table,
        create_raw_dataset,
        upload_csv_to_gcs,
        load_raw,
        check_raw(),
        transform,
        check_marts(),
        report,
        check_reports(),
        ml_features,
        [train_revenue_forecast, train_customer_segments],
    )
    train_revenue_forecast >> forecast_revenue
    train_customer_segments >> score_customer_segments
    [forecast_revenue, score_customer_segments] >> record_pipeline_health()


retail()
