# include/dbt/cosmos_config.py

from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='retail',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt/',
)

# dbt lives in an isolated venv (see Dockerfile); point Cosmos at it explicitly
# instead of relying on a dbt binary on the Airflow env PATH.
DBT_EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path='/usr/local/airflow/dbt_venv/bin/dbt',
)