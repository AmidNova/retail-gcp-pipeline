"""Provision the Metabase dashboard as code (idempotent).

Creates or updates, through the Metabase API:
- the BigQuery connection's dataset filter, so the new layers are browsable;
- a "Retail Platform" collection with one native question per entry of QUESTIONS;
- a "Retail Platform" dashboard laid out from the same entries.

Re-running updates questions in place and rebuilds the dashboard layout.
Standard library only.

Env vars: MB_ADMIN_EMAIL, MB_ADMIN_PASSWORD, GCP_PROJECT_ID,
MB_URL (default http://localhost:3002), MB_DATABASE_NAME (default "Retail BQ").

Usage: python scripts/provision_metabase.py   (or: make metabase)
"""

import json
import os
import urllib.request
from dataclasses import dataclass, field

COLLECTION_NAME = "Retail Platform"
DASHBOARD_NAME = "Retail Platform"
# Datasets visible in Metabase's data browser (native questions can query any).
DATASET_FILTER = "retail,retail_marts,retail_reports,retail_ml,retail_monitoring"


@dataclass(frozen=True)
class Question:
    name: str
    display: str
    sql: str
    position: tuple[int, int, int, int]  # col, row, width, height (18-column grid)
    settings: dict = field(default_factory=dict)
    description: str = ""


QUESTIONS = [
    # --- Pipeline health -------------------------------------------------------
    Question(
        name="Dernier run : état",
        display="scalar",
        position=(0, 0, 4, 3),
        sql="""
            SELECT state
            FROM `{project}.retail_monitoring.pipeline_runs`
            ORDER BY recorded_at DESC LIMIT 1
        """,
    ),
    Question(
        name="Heures depuis le dernier run réussi",
        display="scalar",
        position=(4, 0, 4, 3),
        sql="""
            SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(recorded_at), HOUR) AS hours
            FROM `{project}.retail_monitoring.pipeline_runs`
            WHERE state = 'success'
        """,
    ),
    Question(
        name="Lignes brutes chargées (dernier run)",
        display="scalar",
        position=(8, 0, 4, 3),
        sql="""
            SELECT raw_rows
            FROM `{project}.retail_monitoring.pipeline_runs`
            ORDER BY recorded_at DESC LIMIT 1
        """,
    ),
    Question(
        name="Chiffre d'affaires total (£)",
        display="scalar",
        position=(12, 0, 3, 3),
        sql="""
            SELECT ROUND(SUM(revenue)) AS revenue_gbp
            FROM `{project}.retail_reports.rpt_monthly_revenue`
        """,
        description="Ventes des clients identifiés, hors retours et achats invités.",
    ),
    Question(
        name="% des clients qui font 80 % du CA",
        display="scalar",
        position=(15, 0, 3, 3),
        sql="""
            SELECT ROUND(100 * AVG(IF(is_in_top_80pct_revenue, 1, 0)), 1) AS pct_customers
            FROM `{project}.retail_reports.rpt_customer_revenue_concentration`
        """,
        settings={"column_settings": {'["name","pct_customers"]': {"suffix": " %"}}},
    ),
    Question(
        name="Santé du pipeline (historique des runs)",
        display="table",
        position=(0, 3, 18, 4),
        sql="""
            SELECT
              recorded_at,
              dag_run_id,
              state,
              tasks_failed,
              ARRAY_TO_STRING(failed_task_ids, ', ') AS failed_tasks,
              dbt_test_tasks_passed,
              dbt_test_tasks_failed,
              soda_scans_passed,
              soda_scans_failed,
              soda_checks_warned,
              raw_rows,
              fct_rows,
              TIMESTAMP_DIFF(recorded_at, run_started_at, MINUTE) AS duration_min
            FROM `{project}.retail_monitoring.pipeline_runs`
            ORDER BY recorded_at DESC
            LIMIT 50
        """,
        description="Une ligne par run du DAG retail, écrite par sa dernière tâche.",
    ),
    # --- Sales -----------------------------------------------------------------
    Question(
        name="CA mensuel (saisonnalité)",
        display="bar",
        position=(0, 7, 9, 6),
        sql="""
            SELECT month_start_date, ROUND(revenue) AS revenue_gbp
            FROM `{project}.retail_reports.rpt_monthly_revenue`
            ORDER BY month_start_date
        """,
        settings={
            "graph.dimensions": ["month_start_date"],
            "graph.metrics": ["revenue_gbp"],
        },
    ),
    Question(
        name="Prévision du CA à 30 jours (ARIMA_PLUS)",
        display="line",
        position=(9, 7, 9, 6),
        sql="""
            WITH history AS (
              SELECT
                date_day,
                revenue,
                CAST(NULL AS FLOAT64) AS forecast_revenue,
                CAST(NULL AS FLOAT64) AS lower_bound,
                CAST(NULL AS FLOAT64) AS upper_bound
              FROM `{project}.retail_ml.ml_daily_revenue`
              WHERE date_day >= DATE_SUB(
                (SELECT MAX(date_day) FROM `{project}.retail_ml.ml_daily_revenue`),
                INTERVAL 120 DAY)
            ),
            forecast AS (
              SELECT date_day, CAST(NULL AS FLOAT64) AS revenue, forecast_revenue,
                     lower_bound, upper_bound
              FROM `{project}.retail_ml.bqml_revenue_forecast`
            )
            SELECT * FROM history
            UNION ALL
            SELECT * FROM forecast
            ORDER BY date_day
        """,
        settings={
            "graph.dimensions": ["date_day"],
            "graph.metrics": [
                "revenue",
                "forecast_revenue",
                "lower_bound",
                "upper_bound",
            ],
            "graph.y_axis.auto_split": False,
        },
        description="120 derniers jours d'historique, puis 30 jours de prévision (intervalle à 80 %).",
    ),
    Question(
        name="CA par pays",
        display="map",
        position=(0, 13, 9, 6),
        sql="""
            SELECT country_iso_code, country_name, ROUND(revenue) AS revenue_gbp
            FROM `{project}.retail_reports.rpt_revenue_by_country`
            WHERE country_iso_code IS NOT NULL
        """,
        settings={
            "map.type": "region",
            "map.region": "world_countries",
            "map.dimension": "country_iso_code",
            "map.metric": "revenue_gbp",
        },
    ),
    Question(
        name="Top 10 produits par CA",
        display="row",
        position=(9, 13, 9, 6),
        sql="""
            SELECT description AS product, ROUND(revenue) AS revenue_gbp
            FROM `{project}.retail_reports.rpt_product_sales`
            ORDER BY revenue_gbp DESC
            LIMIT 10
        """,
        settings={"graph.dimensions": ["product"], "graph.metrics": ["revenue_gbp"]},
    ),
    # --- Customers -------------------------------------------------------------
    Question(
        name="Courbe de Pareto du CA",
        display="line",
        position=(0, 19, 6, 6),
        sql="""
            -- One point per 0.1 % of customers: stays under Metabase's 2,000-row cap.
            SELECT
              ROUND(100 * customer_share, 1) AS customer_rank_pct,
              ROUND(MAX(100 * cumulative_revenue_share), 2) AS cumulative_revenue_pct
            FROM `{project}.retail_reports.rpt_customer_revenue_concentration`
            GROUP BY customer_rank_pct
            ORDER BY customer_rank_pct
        """,
        settings={
            "graph.dimensions": ["customer_rank_pct"],
            "graph.metrics": ["cumulative_revenue_pct"],
            "graph.x_axis.scale": "linear",
        },
    ),
    Question(
        name="Segments clients RFM (KMeans)",
        display="scatter",
        position=(6, 19, 6, 6),
        sql="""
            SELECT recency_days, monetary, frequency, segment_id
            FROM `{project}.retail_ml.bqml_customer_segments`
            -- Deterministic 1-in-3 sample: stays under Metabase's 2,000-row cap.
            WHERE monetary > 0 AND MOD(ABS(FARM_FINGERPRINT(customer_id)), 3) = 0
        """,
        description="Échantillon déterministe d'un client sur trois (limite de 2 000 lignes de Metabase).",
        settings={
            "graph.dimensions": ["recency_days", "segment_id"],
            "graph.metrics": ["monetary"],
            "scatter.bubble": "frequency",
            "graph.y_axis.scale": "log",
        },
    ),
    Question(
        name="Profil des segments",
        display="table",
        position=(12, 19, 6, 6),
        sql="""
            SELECT
              segment_id,
              COUNT(*) AS customers,
              ROUND(AVG(recency_days)) AS avg_recency_days,
              ROUND(AVG(frequency), 1) AS avg_invoices,
              ROUND(AVG(monetary)) AS avg_revenue_gbp
            FROM `{project}.retail_ml.bqml_customer_segments`
            GROUP BY segment_id
            ORDER BY avg_revenue_gbp DESC
        """,
        description="Les numéros de segment changent à chaque réentraînement : lire le profil, pas l'id.",
    ),
]


class Metabase:
    def __init__(self, url: str, email: str, password: str):
        self.url = url.rstrip("/")
        self.session = None
        self.session = self.call(
            "POST", "/api/session", {"username": email, "password": password}
        )["id"]

    def call(self, method: str, path: str, body: dict | None = None):
        headers = {"Content-Type": "application/json"}
        if self.session:
            headers["X-Metabase-Session"] = self.session
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.url + path, data=data, headers=headers, method=method
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = response.read()
        return json.loads(payload) if payload else None

    def items(self, collection_id: int, model: str) -> list[dict]:
        result = self.call(
            "GET", f"/api/collection/{collection_id}/items?models={model}"
        )
        return result["data"] if isinstance(result, dict) else result


def find_database_id(mb: Metabase, name: str) -> int:
    for database in mb.call("GET", "/api/database")["data"]:
        if database["name"] == name:
            return database["id"]
    raise SystemExit(f"Metabase database {name!r} not found")


def widen_dataset_filter(mb: Metabase, database_id: int) -> None:
    details = mb.call("GET", f"/api/database/{database_id}")["details"]
    if details.get("dataset-filters-patterns") == DATASET_FILTER:
        return
    # Secrets come back obfuscated from GET; Metabase keeps the stored value
    # when the obfuscated placeholder is sent back unchanged.
    new_details = {
        **details,
        "dataset-filters-type": "inclusion",
        "dataset-filters-patterns": DATASET_FILTER,
    }
    mb.call("PUT", f"/api/database/{database_id}", {"details": new_details})
    mb.call("POST", f"/api/database/{database_id}/sync_schema")
    print(f"Dataset filter set to {DATASET_FILTER}, schema sync started")


def get_or_create_collection(mb: Metabase) -> int:
    for collection in mb.call("GET", "/api/collection"):
        if collection.get("name") == COLLECTION_NAME and not collection.get("archived"):
            return collection["id"]
    return mb.call(
        "POST", "/api/collection", {"name": COLLECTION_NAME, "color": "#509EE3"}
    )["id"]


def card_body(
    question: Question, project: str, database_id: int, collection_id: int
) -> dict:
    return {
        "name": question.name,
        "description": question.description or None,
        "display": question.display,
        "collection_id": collection_id,
        "visualization_settings": question.settings,
        "dataset_query": {
            "type": "native",
            "database": database_id,
            "native": {
                "query": question.sql.format(project=project).strip(),
                "template-tags": {},
            },
        },
    }


def upsert_cards(
    mb: Metabase, project: str, database_id: int, collection_id: int
) -> dict[str, int]:
    existing = {item["name"]: item["id"] for item in mb.items(collection_id, "card")}
    card_ids = {}
    for question in QUESTIONS:
        body = card_body(question, project, database_id, collection_id)
        if question.name in existing:
            card_ids[question.name] = existing[question.name]
            mb.call("PUT", f"/api/card/{existing[question.name]}", body)
        else:
            card_ids[question.name] = mb.call("POST", "/api/card", body)["id"]
    return card_ids


def build_dashboard(mb: Metabase, collection_id: int, card_ids: dict[str, int]) -> int:
    dashboards = [
        d for d in mb.items(collection_id, "dashboard") if d["name"] == DASHBOARD_NAME
    ]
    if dashboards:
        dashboard_id = dashboards[0]["id"]
    else:
        dashboard_id = mb.call(
            "POST",
            "/api/dashboard",
            {
                "name": DASHBOARD_NAME,
                "collection_id": collection_id,
                "description": "Ventes, prévision, segments clients et santé du pipeline.",
            },
        )["id"]
    # Rebuild the layout from QUESTIONS so the code stays the source of truth.
    for dashcard in mb.call("GET", f"/api/dashboard/{dashboard_id}")["ordered_cards"]:
        mb.call(
            "DELETE", f"/api/dashboard/{dashboard_id}/cards?dashcardId={dashcard['id']}"
        )
    for question in QUESTIONS:
        col, row, width, height = question.position
        mb.call(
            "POST",
            f"/api/dashboard/{dashboard_id}/cards",
            {
                "cardId": card_ids[question.name],
                "col": col,
                "row": row,
                "size_x": width,
                "size_y": height,
            },
        )
    return dashboard_id


def main() -> None:
    project = os.environ["GCP_PROJECT_ID"]
    url = os.environ.get("MB_URL", "http://localhost:3002")
    mb = Metabase(url, os.environ["MB_ADMIN_EMAIL"], os.environ["MB_ADMIN_PASSWORD"])

    database_id = find_database_id(mb, os.environ.get("MB_DATABASE_NAME", "Retail BQ"))
    widen_dataset_filter(mb, database_id)
    collection_id = get_or_create_collection(mb)
    card_ids = upsert_cards(mb, project, database_id, collection_id)
    dashboard_id = build_dashboard(mb, collection_id, card_ids)
    print(f"{len(card_ids)} questions, dashboard: {url}/dashboard/{dashboard_id}")


if __name__ == "__main__":
    main()
