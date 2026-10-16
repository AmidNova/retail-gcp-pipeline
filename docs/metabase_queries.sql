-- =====================================================================
-- Metabase query pack — Retail GCP Pipeline
-- Paste each block into a new Metabase "SQL question" (BigQuery database).
-- Tables come from the prod dbt target (retail_<layer> datasets), the
-- BigQuery ML jobs (retail_ml.bqml_*) and the monitoring table.
-- Replace your-project with your GCP project id.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 0. PIPELINE HEALTH — one row per run of the retail DAG
--    Viz: Table (conditional formatting on state, tasks_failed)
-- ---------------------------------------------------------------------
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
FROM `your-project.retail_monitoring.pipeline_runs`
ORDER BY recorded_at DESC
LIMIT 50;


-- 0b. LAST RUN — single-value cards (state, rows loaded, hours since last success)
SELECT
  (SELECT state FROM `your-project.retail_monitoring.pipeline_runs`
   ORDER BY recorded_at DESC LIMIT 1) AS last_run_state,
  (SELECT raw_rows FROM `your-project.retail_monitoring.pipeline_runs`
   ORDER BY recorded_at DESC LIMIT 1) AS last_run_raw_rows,
  TIMESTAMP_DIFF(
    CURRENT_TIMESTAMP(),
    (SELECT MAX(recorded_at) FROM `your-project.retail_monitoring.pipeline_runs`
     WHERE state = 'success'),
    HOUR) AS hours_since_last_success;


-- ---------------------------------------------------------------------
-- 1. REVENUE FORECAST — last 120 days of history + 30-day ARIMA_PLUS forecast
--    Viz: Line chart, X = date_day; series revenue, forecast_revenue,
--    lower_bound, upper_bound (bounds dashed)
-- ---------------------------------------------------------------------
WITH history AS (
  SELECT date_day, revenue, NULL AS forecast_revenue, NULL AS lower_bound, NULL AS upper_bound
  FROM `your-project.retail_ml.ml_daily_revenue`
  WHERE date_day >= DATE_SUB(
    (SELECT MAX(date_day) FROM `your-project.retail_ml.ml_daily_revenue`), INTERVAL 120 DAY)
),
forecast AS (
  SELECT date_day, NULL AS revenue, forecast_revenue, lower_bound, upper_bound
  FROM `your-project.retail_ml.bqml_revenue_forecast`
)
SELECT * FROM history
UNION ALL
SELECT * FROM forecast
ORDER BY date_day;


-- ---------------------------------------------------------------------
-- 2. CUSTOMER SEGMENTS — RFM scatter (KMeans, k=4)
--    Viz: Scatter, X = recency_days, Y = monetary (log scale),
--    bubble size = frequency, color = segment_id.
--    Segment ids are not stable across retrainings: name them from their
--    average RFM (block 2b), not from a hard-coded id.
-- ---------------------------------------------------------------------
SELECT customer_id, recency_days, frequency, monetary, segment_id
FROM `your-project.retail_ml.bqml_customer_segments`;


-- 2b. SEGMENT PROFILE — size and average RFM per segment
SELECT
  segment_id,
  COUNT(*)                    AS customers,
  ROUND(AVG(recency_days))    AS avg_recency_days,
  ROUND(AVG(frequency), 1)    AS avg_invoices,
  ROUND(AVG(monetary))        AS avg_revenue_gbp,
  ROUND(SUM(monetary))        AS total_revenue_gbp
FROM `your-project.retail_ml.bqml_customer_segments`
GROUP BY segment_id
ORDER BY avg_revenue_gbp DESC;


-- ---------------------------------------------------------------------
-- 3. REVENUE BY COUNTRY — world map
--    Viz: Region map. Metabase matches ISO alpha-2 codes -> country_iso_code.
-- ---------------------------------------------------------------------
SELECT country_iso_code, country_name, ROUND(revenue) AS revenue_gbp
FROM `your-project.retail_reports.rpt_revenue_by_country`
ORDER BY revenue_gbp DESC;


-- ---------------------------------------------------------------------
-- 4. TOP 10 PRODUCTS BY REVENUE — Viz: Row chart
-- ---------------------------------------------------------------------
SELECT description AS product, ROUND(revenue) AS revenue_gbp
FROM `your-project.retail_reports.rpt_product_sales`
ORDER BY revenue_gbp DESC
LIMIT 10;


-- ---------------------------------------------------------------------
-- 5. MONTHLY REVENUE — seasonality. Viz: Line or bar, X = month_start_date
-- ---------------------------------------------------------------------
SELECT month_start_date, ROUND(revenue) AS revenue_gbp, invoice_count
FROM `your-project.retail_reports.rpt_monthly_revenue`
ORDER BY month_start_date;


-- ---------------------------------------------------------------------
-- 6. PARETO — revenue concentration. Viz: Line, X = customer_share,
--    Y = cumulative_revenue_share. The share of customers making 80% of
--    revenue is block 6b.
-- ---------------------------------------------------------------------
SELECT
  ROUND(100 * customer_share, 2)           AS customer_rank_pct,
  ROUND(100 * cumulative_revenue_share, 2) AS cumulative_revenue_pct
FROM `your-project.retail_reports.rpt_customer_revenue_concentration`
ORDER BY revenue_rank;


-- 6b. Share of customers making 80% of revenue (single value)
SELECT ROUND(100 * AVG(IF(is_in_top_80pct_revenue, 1, 0)), 1) AS pct_customers_for_80pct_revenue
FROM `your-project.retail_reports.rpt_customer_revenue_concentration`;
