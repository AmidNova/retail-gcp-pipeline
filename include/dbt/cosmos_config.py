"""Cosmos configuration shared by every dbt task group of the retail DAG."""

import os
from pathlib import Path

from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
DBT_PROJECT_PATH = AIRFLOW_HOME / "include" / "dbt"

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    install_dbt_deps=True,
    # Keep the dbt venv isolated: when Airflow runs as root (init container),
    # the image puts Airflow's site-packages on PYTHONPATH, and dbt would then
    # import Airflow's older protobuf.
    # buildable: a test is selected with a layer only when all its parents are
    # built by that layer or upstream, so cross-layer tests (reports vs fact,
    # ML features vs dimensions) run in the downstream group, not before it.
    env_vars={"PYTHONPATH": "", "DBT_INDIRECT_SELECTION": "buildable"},
)

# profiles.yml reads everything else (project, key path, location) from env vars.
DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="retail",
    target_name=os.environ.get("DBT_TARGET", "prod"),
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

# dbt lives in its own venv (see Dockerfile) to keep its dependencies away
# from Airflow's.
DBT_EXECUTION_CONFIG = ExecutionConfig(
    dbt_executable_path=str(AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"),
)
