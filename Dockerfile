FROM apache/airflow:2.7.3-python3.11

# Keep the /usr/local/airflow paths that the DAG, dbt, and Soda configs hardcode.
# Apache Airflow uses /opt/airflow as AIRFLOW_HOME, so symlink the old Astro path to it.
USER root
RUN ln -s /opt/airflow /usr/local/airflow
USER airflow

# Airflow + provider/library deps (cosmos, astro-sdk, google provider)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# The base image sets PIP_USER=true; disable it so installs land inside the venvs.
ENV PIP_USER=false

# Soda in an isolated virtual environment (used by the external_python tasks)
RUN python -m venv /opt/airflow/soda_venv && \
    /opt/airflow/soda_venv/bin/pip install --no-cache-dir \
        soda-core-bigquery==3.0.45 \
        soda-core-scientific==3.0.45

# dbt in an isolated virtual environment (invoked by Cosmos via ExecutionConfig)
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery==1.5.3
