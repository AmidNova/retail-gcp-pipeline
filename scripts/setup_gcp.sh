#!/usr/bin/env bash
#
# setup_gcp.sh — Provision all GCP resources for the retail pipeline.
#
# Idempotent: safe to re-run. Creates a project, links billing, enables APIs,
# creates a GCS bucket, a service account with the required roles, and a key.
#
# Usage:
#   PROJECT_ID=retail-pipeline-xxxxxx BILLING_ACCOUNT=XXXXXX-XXXXXX-XXXXXX ./scripts/setup_gcp.sh
#
set -euo pipefail

# ---- Config (override via env) ---------------------------------------------
PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
BILLING_ACCOUNT="${BILLING_ACCOUNT:?set BILLING_ACCOUNT}"
LOCATION="${LOCATION:-US}"                       # BigQuery + GCS multi-region
BUCKET="${BUCKET:-${PROJECT_ID}-retail-raw}"     # globally unique
SA_NAME="${SA_NAME:-airflow-pipeline}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KEY_PATH="${KEY_PATH:-include/gcp/service_account.json}"

echo "==> Project:  $PROJECT_ID"
echo "==> Billing:  $BILLING_ACCOUNT"
echo "==> Location: $LOCATION"
echo "==> Bucket:   gs://$BUCKET"
echo "==> SA:       $SA_EMAIL"

# ---- 1. Project ------------------------------------------------------------
if ! gcloud projects describe "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud projects create "$PROJECT_ID" --name="Retail GCP Pipeline"
fi
gcloud config set project "$PROJECT_ID"

# ---- 2. Billing ------------------------------------------------------------
gcloud billing projects link "$PROJECT_ID" --billing-account="$BILLING_ACCOUNT"

# ---- 3. APIs ---------------------------------------------------------------
gcloud services enable bigquery.googleapis.com storage.googleapis.com

# ---- 4. GCS bucket ---------------------------------------------------------
if ! gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://$BUCKET" --location="$LOCATION" --uniform-bucket-level-access
fi

# ---- 5. Service account + roles -------------------------------------------
if ! gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --display-name="Airflow retail pipeline"
fi
for role in roles/bigquery.admin roles/storage.admin; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" --role="$role" --condition=None >/dev/null
done

# ---- 6. Key ----------------------------------------------------------------
mkdir -p "$(dirname "$KEY_PATH")"
if [ ! -f "$KEY_PATH" ]; then
  gcloud iam service-accounts keys create "$KEY_PATH" --iam-account="$SA_EMAIL"
fi

echo "==> DONE. Key at $KEY_PATH (gitignored)."
