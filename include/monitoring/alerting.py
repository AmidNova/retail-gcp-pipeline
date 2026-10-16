"""DAG-level failure alerting.

One alert per failed DAG run (not one per task and per retry). Always logged
as structured JSON; also posted to Slack when SLACK_WEBHOOK_URL is set.
"""

import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_PREFIX = "https://hooks.slack.com/"
WEBHOOK_TIMEOUT_SECONDS = 10


def failure_message(
    dag_id: str, run_id: str, failed_task_ids: list[str], log_url: str
) -> str:
    tasks = ", ".join(failed_task_ids) if failed_task_ids else "unknown"
    return f":red_circle: DAG `{dag_id}` failed (run `{run_id}`). Failed tasks: {tasks}. Logs: {log_url}"


def post_to_slack(webhook_url: str, text: str) -> None:
    if not webhook_url.startswith(SLACK_WEBHOOK_PREFIX):
        raise ValueError("SLACK_WEBHOOK_URL must be a Slack incoming webhook URL")
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps({"text": text}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=WEBHOOK_TIMEOUT_SECONDS):
        pass


def notify_dag_failure(context: dict) -> None:
    """Airflow on_failure_callback for a DAG. Never raises: alerting must not mask the failure."""
    dag_run = context["dag_run"]
    failed_task_ids = sorted(
        ti.task_id for ti in dag_run.get_task_instances(state="failed")
    )
    log_url = context["task_instance"].log_url if context.get("task_instance") else ""
    text = failure_message(dag_run.dag_id, dag_run.run_id, failed_task_ids, log_url)

    logger.error(
        json.dumps(
            {
                "event": "dag_run_failed",
                "dag_id": dag_run.dag_id,
                "run_id": dag_run.run_id,
                "failed_tasks": failed_task_ids,
            }
        )
    )

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return
    try:
        post_to_slack(webhook_url, text)
    except Exception:  # noqa: BLE001 - an alerting outage must not hide the original failure
        logger.exception("Could not post the failure alert to Slack")
