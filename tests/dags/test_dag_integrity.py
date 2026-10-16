"""DAG integrity tests, run inside the Airflow image (see `make test-dags`).

The DagBag is built once per module: parsing renders the Cosmos task groups
with `dbt ls`, which takes several seconds per group.
"""

import pytest
from airflow.models import DagBag

DAG_ID = "retail"
MIN_RETRIES = 2


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(include_examples=False)


@pytest.fixture(scope="module")
def retail_dag(dag_bag):
    return dag_bag.get_dag(DAG_ID)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_retail_dag_is_loaded(retail_dag):
    assert retail_dag is not None


def test_every_dag_has_tags(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"


def test_tasks_retry(retail_dag):
    for task in retail_dag.tasks:
        assert task.retries >= MIN_RETRIES, f"{task.task_id} retries={task.retries}"


def test_soda_gate_runs_between_load_and_dbt(retail_dag):
    check_raw = retail_dag.get_task("check_raw")
    assert "load_raw" in check_raw.upstream_task_ids
    assert any(
        task_id.startswith("transform.") for task_id in check_raw.downstream_task_ids
    )


def test_dbt_layers_are_rendered(retail_dag):
    task_ids = set(retail_dag.task_ids)
    for model in (
        "stg_retail__invoice_lines",
        "fct_invoice_lines",
        "rpt_monthly_revenue",
        "ml_customer_rfm",
    ):
        assert any(model in task_id for task_id in task_ids), f"no task for {model}"


def test_health_is_recorded_last_even_after_failures(retail_dag):
    health = retail_dag.get_task("record_pipeline_health")
    assert health.trigger_rule == "all_done"
    assert retail_dag.leaves == [health]


def test_cross_layer_tests_run_after_their_parents(retail_dag):
    reconcile = next(
        t for t in retail_dag.tasks if "assert_reports_reconcile_with_fact" in t.task_id
    )
    assert reconcile.task_id.startswith("report.")


def test_ml_training_waits_for_features(retail_dag):
    train = retail_dag.get_task("train_revenue_forecast")
    assert any(
        task_id.startswith("ml_features.") for task_id in train.upstream_task_ids
    )
