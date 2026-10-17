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

## Port Databricks (Lakeflow, medallion)

Le même pipeline porté sur **Databricks Free Edition** dans `databricks/` : Lakeflow Declarative Pipeline (SQL) en serverless, déployé en Declarative Automation Bundle.

```
/Volumes/retail/bronze/landing/online_retail/*.csv
      │  Auto Loader (read_files en STREAM : seuls les nouveaux fichiers)
      ▼
bronze.online_retail          brut, tout en STRING
      ▼
silver.invoice_lines          typé + flags (FAIL si parsing cassé, WARN invités/annulations)
silver.sales_lines            périmètre ventes (DROP ROW : qté/prix ≤ 0, sans client, non-marchandise)
      ▼
gold.daily_revenue            CA journalier sans trou
gold.customer_rfm             Recency / Frequency / Monetary
gold.customer_revenue_concentration   Pareto clients
```

Les règles de qualité sont des **expectations** Lakeflow (équivalent des tests dbt + Soda), visibles dans l'onglet data quality du pipeline.

```bash
databricks auth login --host https://<workspace>.cloud.databricks.com
make databricks-smoke    # SQL du pipeline sur Spark local, avant déploiement
make databricks-setup    # catalog, schémas, volume, upload des CSV
make databricks-deploy   # bundle validate + deploy
make databricks-run      # lance le job retail_weekly
```

Puis `databricks/validation/validation.sql` dans l'éditeur SQL. Run du 2026-10-17, identique à BigQuery et au smoke test :

| Table | Lignes |
|---|---|
| bronze.online_retail | 1 067 371 |
| silver.sales_lines | 802 651 |
| gold.daily_revenue | 739 |
| gold.customer_rfm | 5 852 |

CA 17 434 465 GBP, 36 594 factures, 23,1 % des clients font 80 % du CA.

> Le job est en pause (quota Free Edition) : les runs se lancent à la main. Le catalog est créé en SQL car `databricks catalogs create` échoue sur Free Edition (pas de storage root).

## CI/CD

Le workflow GitHub Actions (`.github/workflows/ci.yml`) tourne sur chaque PR :
- `dbt compile` (vérification syntaxe, sans GCP)
- `dbt test` (sur `main` uniquement, nécessite les secrets GCP)

**Secrets à configurer dans GitHub :**
- `GCP_PROJECT_ID`
- `GCP_KEYFILE_JSON`
