#!/usr/bin/env bash
#
# setup_ci_wif.sh — Keyless auth from GitHub Actions to GCP with Workload
# Identity Federation. Idempotent: safe to re-run.
#
# GitHub's OIDC token is exchanged for a short-lived token of a dedicated CI
# service account. Only workflows of GITHUB_REPO can impersonate it, and no
# JSON key exists anywhere.
#
# CI service account permissions (least privilege):
#   roles/bigquery.user       run jobs, create its own datasets (and own them)
#   roles/bigquery.dataViewer read raw and prod tables (sources, --defer)
#
# Usage:
#   PROJECT_ID=my-project GITHUB_REPO=owner/repo ./scripts/setup_ci_wif.sh
#
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
GITHUB_REPO="${GITHUB_REPO:?set GITHUB_REPO (owner/repo)}"
POOL_ID="${POOL_ID:-github}"
PROVIDER_ID="${PROVIDER_ID:-github-oidc}"
SA_NAME="${SA_NAME:-dbt-ci}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')"

echo "==> APIs"
gcloud services enable iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com \
  --project "$PROJECT_ID"

echo "==> Workload identity pool: $POOL_ID"
if ! gcloud iam workload-identity-pools describe "$POOL_ID" \
    --project "$PROJECT_ID" --location global >/dev/null 2>&1; then
  gcloud iam workload-identity-pools create "$POOL_ID" \
    --project "$PROJECT_ID" --location global --display-name "GitHub Actions"
fi

echo "==> OIDC provider: $PROVIDER_ID (restricted to $GITHUB_REPO)"
if ! gcloud iam workload-identity-pools providers describe "$PROVIDER_ID" \
    --project "$PROJECT_ID" --location global --workload-identity-pool "$POOL_ID" >/dev/null 2>&1; then
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_ID" \
    --project "$PROJECT_ID" --location global --workload-identity-pool "$POOL_ID" \
    --display-name "GitHub OIDC" \
    --issuer-uri "https://token.actions.githubusercontent.com" \
    --attribute-mapping "google.subject=assertion.sub,attribute.repository=assertion.repository" \
    --attribute-condition "assertion.repository == '${GITHUB_REPO}'"
fi

echo "==> Service account: $SA_EMAIL"
if ! gcloud iam service-accounts describe "$SA_EMAIL" --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT_ID" \
    --display-name "dbt CI (GitHub Actions, keyless)"
fi
for role in roles/bigquery.user roles/bigquery.dataViewer; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member "serviceAccount:$SA_EMAIL" --role "$role" --condition None >/dev/null
done

echo "==> Allow $GITHUB_REPO to impersonate $SA_EMAIL"
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" --project "$PROJECT_ID" \
  --role roles/iam.workloadIdentityUser \
  --member "principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/attribute.repository/${GITHUB_REPO}" \
  >/dev/null

PROVIDER="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_ID}/providers/${PROVIDER_ID}"
echo "==> DONE. Set these GitHub repository variables (not secrets, nothing here is sensitive):"
echo "    GCP_PROJECT_ID=$PROJECT_ID"
echo "    GCP_WIF_PROVIDER=$PROVIDER"
echo "    GCP_CI_SERVICE_ACCOUNT=$SA_EMAIL"
