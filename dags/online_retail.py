from airflow.decorators import (
    dag,
    task,
)  # DAG and task decorators for interfacing with the TaskFlow API
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from astro import sql as aql
from astro.files import File
from airflow.models.baseoperator import chain
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from include.dbt.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG, DBT_EXECUTION_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

PROJECT_ID = 'retail-pipeline-317c24'
BQ_LOCATION = 'US'


def _bq_job(task_id, sql):
    """Run a BigQuery (BQML) DDL/DML statement via the gcp connection."""
    return BigQueryInsertJobOperator(
        task_id=task_id,
        gcp_conn_id='gcp',
        location=BQ_LOCATION,
        configuration={'query': {'query': sql, 'useLegacySql': False}},
    )


@dag(
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
    tags=['retail'],
)
def retail():

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket='retail-pipeline-317c24-retail-raw',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail',
        gcp_conn_id='gcp',

    )

    gcs_to_raw = aql.load_file(

        task_id='gcs_to_raw',
        input_file=File(
            'gs://retail-pipeline-317c24-retail-raw/raw/online_retail.csv',
            conn_id='gcp',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='raw_online_retail',
            conn_id='gcp',
            metadata=Metadata(schema='retail')
        ),
        use_native_support=False,
    )

    # Reference table (country name -> ISO code) needed by dim_customer.
    load_country = aql.load_file(
        task_id='load_country',
        input_file=File(
            '/usr/local/airflow/include/dataset/country.csv',
            filetype=FileType.CSV,
        ),
        output_table=Table(
            name='country',
            conn_id='gcp',
            metadata=Metadata(schema='retail'),
        ),
        use_native_support=False,
    )


    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    

    transform = DbtTaskGroup(
        group_id = 'transform',
        project_config = DBT_PROJECT_CONFIG,
        profile_config = DBT_CONFIG,
        execution_config = DBT_EXECUTION_CONFIG,
        operator_args = {'install_deps': True},
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            select=['path:models/transform']
        ))

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    

    report = DbtTaskGroup(
        group_id = 'report',
        project_config = DBT_PROJECT_CONFIG,
        profile_config = DBT_CONFIG,
        execution_config = DBT_EXECUTION_CONFIG,
        operator_args = {'install_deps': True},
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            select=['path:models/report']
        ))


    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    # --- Machine learning ---------------------------------------------------
    # dbt builds the feature tables (ca_daily, customer_rfm); BigQuery ML then
    # trains the models and writes the scored/forecast tables.
    ml_features = DbtTaskGroup(
        group_id='ml_features',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,
        operator_args={'install_deps': True},
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/ml'],
        ))

    train_ca_forecast = _bq_job('train_ca_forecast', f"""
        CREATE OR REPLACE MODEL `{PROJECT_ID}.retail.ca_forecast`
        OPTIONS(
            model_type='ARIMA_PLUS',
            time_series_timestamp_col='jour',
            time_series_data_col='ca',
            data_frequency='DAILY',
            holiday_region='GB',
            auto_arima=TRUE
        ) AS
        SELECT jour, ca FROM `{PROJECT_ID}.retail.ca_daily`
    """)

    forecast_ca = _bq_job('forecast_ca', f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.retail.ca_forecast_results` AS
        SELECT
            DATE(forecast_timestamp) AS jour,
            forecast_value AS ca_prevu,
            prediction_interval_lower_bound AS borne_basse,
            prediction_interval_upper_bound AS borne_haute
        FROM ML.FORECAST(MODEL `{PROJECT_ID}.retail.ca_forecast`,
                         STRUCT(30 AS horizon, 0.8 AS confidence_level))
    """)

    train_customer_segments = _bq_job('train_customer_segments', f"""
        CREATE OR REPLACE MODEL `{PROJECT_ID}.retail.customer_segments_model`
        OPTIONS(model_type='KMEANS', num_clusters=4, standardize_features=TRUE) AS
        SELECT recency, frequency, monetary FROM `{PROJECT_ID}.retail.customer_rfm`
    """)

    score_customer_segments = _bq_job('score_customer_segments', f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.retail.customer_segments` AS
        SELECT
            r.customer_id, r.recency, r.frequency, r.monetary,
            CAST(p.CENTROID_ID AS STRING) AS segment
        FROM ML.PREDICT(MODEL `{PROJECT_ID}.retail.customer_segments_model`,
                        TABLE `{PROJECT_ID}.retail.customer_rfm`) p
        JOIN `{PROJECT_ID}.retail.customer_rfm` r USING(customer_id)
    """)

    chain(
        upload_csv_to_gcs,
        create_retail_dataset,
        gcs_to_raw,
        load_country,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report(),
        ml_features,
        train_ca_forecast,
        forecast_ca,
        train_customer_segments,
        score_customer_segments,
    )

retail()