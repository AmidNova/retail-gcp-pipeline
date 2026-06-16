# Retail GCP Pipeline

Pipeline de données retail end-to-end sur **Google Cloud Platform** — du CSV brut aux dashboards analytiques.

## Stack

| Outil | Rôle |
|---|---|
| Apache Airflow (Docker Compose) | Orchestration |
| Google Cloud Storage | Stockage fichiers bruts |
| BigQuery | Data Warehouse |
| dbt (BigQuery adapter) | Transformations ELT |
| Soda | Data quality checks |
| Metabase | Visualisation |
| Docker Compose | Environnement local |
| GitHub Actions | CI/CD |

## Architecture

```
online_retail.csv
      │
      ▼
[Airflow DAG]
      │
      ├──► GCS (raw/online_retail.csv)
      │
      ├──► BigQuery (retail.raw_online_retail)
      │
      ├──► Soda check_load ✓
      │
      ├──► dbt transform
      │         ├── dim_customer
      │         ├── dim_datetime
      │         ├── dim_product
      │         └── fct_invoices
      │
      ├──► Soda check_transform ✓
      │
      ├──► dbt report
      │         ├── report_customer_invoices
      │         ├── report_product_invoices
      │         └── report_year_invoices
      │
      └──► Soda check_report ✓
```

## Dataset

[Online Retail II (UCI)](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci) — ~500k transactions e-commerce UK, 2009-2011.

Colonnes : `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer_id`, `Country`

> ⚠️ Le CSV (~89 Mo) n'est **pas** versionné. Télécharge-le depuis [Kaggle](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci) et place-le dans `include/dataset/online_retail.csv` avant de lancer le DAG.

## Modèles dbt

**Dimension tables**

| Modèle | Description |
|---|---|
| `dim_customer` | Clients uniques avec pays et code ISO |
| `dim_datetime` | Calendrier (heure, jour, mois, année, jour de semaine) |
| `dim_product` | Catalogue produits dédupliqué |

**Fact table**

| Modèle | Description |
|---|---|
| `fct_invoices` | Lignes de facture avec clés dimensionnelles et montant total |

**Report tables**

| Modèle | Description |
|---|---|
| `report_customer_invoices` | CA et nb factures par pays |
| `report_product_invoices` | Top produits par CA |
| `report_year_invoices` | Évolution CA par année |

## Lancer le projet

### Pré-requis
- Docker + Docker Compose
- Compte GCP avec BigQuery et GCS activés
- Service account JSON avec rôles BigQuery Admin + Storage Admin, placé dans `include/gcp/service_account.json`

### Setup

```bash
# 1. Créer le bucket GCS
gsutil mb gs://retail-pipeline-317c24-retail-raw

# 2. Placer le service account JSON
#    include/gcp/service_account.json  (lisible par le conteneur : chmod 644)
#    La connexion Airflow `gcp` est injectée via AIRFLOW_CONN_GCP (docker-compose.yaml),
#    pas besoin de la créer dans l'UI.

# 3. Construire et démarrer la stack
docker compose up -d        # build l'image au 1er lancement

# 4. Accéder à Airflow
open http://localhost:8088
# login: admin / admin
# Metabase : http://localhost:3002

# 5. Activer et trigger le DAG : retail
docker compose exec -T airflow-scheduler airflow dags trigger retail
```

> Migré depuis Astro CLI vers Apache Airflow vanilla (`apache/airflow:2.7.3`, LocalExecutor).
> dbt et Soda tournent dans des venvs isolés (`dbt_venv`, `soda_venv`) dans l'image.

## CI/CD

Le workflow GitHub Actions (`.github/workflows/ci.yml`) tourne sur chaque PR :
- `dbt compile` (vérification syntaxe, sans GCP)
- `dbt test` (sur `main` uniquement, nécessite les secrets GCP)

**Secrets à configurer dans GitHub :**
- `GCP_PROJECT_ID`
- `GCP_KEYFILE_JSON`
