FROM apache/airflow:2.7.3-python3.11

# Airflow-side libraries (providers, Cosmos). Airflow itself is pinned so pip
# can never upgrade it as a side effect.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# The base image sets PIP_USER=true; disable it so installs land inside the venvs.
ENV PIP_USER=false

# Soda in its own venv (run by @task.external_python)
RUN python -m venv /opt/airflow/soda_venv && \
    /opt/airflow/soda_venv/bin/pip install --no-cache-dir \
        soda-core-bigquery==3.0.45 \
        soda-core-scientific==3.0.45

# dbt in its own venv (run by Cosmos through ExecutionConfig). Same versions as
# requirements-dev.txt so local, CI and Airflow run the same dbt.
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.11.16 \
        dbt-bigquery==1.11.1
