#!/usr/bin/env bash
#
# setup_databricks.sh — Create what the Databricks bundle reads from: the
# retail catalog, its bronze/silver/gold schemas, the landing volume, and the
# two CSVs in it.
#
# Idempotent: safe to re-run (IF NOT EXISTS, uploads overwrite).
#
# The catalog is created in SQL through a warehouse: on Free Edition,
# `databricks catalogs create` fails ("Metastore storage root URL does not
# exist") because only SQL falls back to the account's Default Storage.
#
# Needs the Databricks CLI, logged in (`databricks auth login --host ...`).
#
# Usage:
#   ./scripts/setup_databricks.sh
#   WAREHOUSE_ID=xxxxxxxx ./scripts/setup_databricks.sh   # pick a warehouse
#
set -euo pipefail

# ---- Config (override via env) ---------------------------------------------
CATALOG="${CATALOG:-retail}"
SALES_CSV="${SALES_CSV:-include/dataset/online_retail.csv}"
CODES_CSV="${CODES_CSV:-include/dbt/seeds/non_merchandise_stock_codes.csv}"
LANDING="dbfs:/Volumes/${CATALOG}/bronze/landing"
# First warehouse of the workspace by default (Free Edition has one).
WAREHOUSE_ID="${WAREHOUSE_ID:-$(databricks warehouses list -o json \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)[0]["id"])')}"

echo "==> Warehouse: $WAREHOUSE_ID"
echo "==> Catalog:   $CATALOG"

[[ -f "$SALES_CSV" ]] || { echo "missing $SALES_CSV: run make dataset" >&2; exit 1; }

# Runs one statement on the warehouse and fails if it does not succeed.
run_sql() {
  local body
  body=$(python3 -c 'import json, sys; print(json.dumps({"warehouse_id": sys.argv[1], "statement": sys.argv[2], "wait_timeout": "50s"}))' \
    "$WAREHOUSE_ID" "$1")
  databricks api post /api/2.0/sql/statements --json "$body" | python3 -c '
import json, sys
status = json.load(sys.stdin)["status"]
if status["state"] != "SUCCEEDED":
    sys.exit(status["state"] + ": " + status.get("error", {}).get("message", ""))
'
  echo "    ok: $1"
}

# ---- 1. Catalog, schemas, volume -------------------------------------------
run_sql "CREATE CATALOG IF NOT EXISTS ${CATALOG} COMMENT 'Online Retail II medallion (bronze/silver/gold)'"
for schema in bronze silver gold; do
  run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${schema}"
done
run_sql "CREATE VOLUME IF NOT EXISTS ${CATALOG}.bronze.landing"

# ---- 2. Landing files --------------------------------------------------------
# One folder per source: Auto Loader reads every file in online_retail/.
databricks fs mkdirs "${LANDING}/online_retail"
databricks fs mkdirs "${LANDING}/reference"
databricks fs cp "$SALES_CSV" "${LANDING}/online_retail/online_retail.csv" --overwrite
databricks fs cp "$CODES_CSV" "${LANDING}/reference/non_merchandise_stock_codes.csv" --overwrite

echo "==> Done. Next: make databricks-deploy && make databricks-run"
