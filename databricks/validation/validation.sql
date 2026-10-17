-- Validation queries, to run in the Databricks SQL editor after a green run.
-- Expected values come from the BigQuery pipeline (prod run of 2026-10-17)
-- and from databricks/tests/local_smoke_test.py on the same CSV.

-- 1. Row counts per layer -------------------------------------------------
-- expected: bronze 1,067,371 | silver.invoice_lines 1,067,371 |
--           silver.sales_lines 802,651 | gold.daily_revenue 739 |
--           gold.customer_rfm 5,852 | gold.customer_revenue_concentration 5,852
SELECT 'bronze.online_retail' AS table_name, count(*) AS row_count FROM retail.bronze.online_retail
UNION ALL SELECT 'silver.invoice_lines', count(*) FROM retail.silver.invoice_lines
UNION ALL SELECT 'silver.sales_lines', count(*) FROM retail.silver.sales_lines
UNION ALL SELECT 'gold.daily_revenue', count(*) FROM retail.gold.daily_revenue
UNION ALL SELECT 'gold.customer_rfm', count(*) FROM retail.gold.customer_rfm
UNION ALL SELECT 'gold.customer_revenue_concentration', count(*) FROM retail.gold.customer_revenue_concentration;

-- 2. Rows per expectation, recomputed from silver --------------------------
-- Same rules as the expectations, counted on all lines (a line that breaks two
-- rules counts twice). expected: no customer 243,007 | cancellation 19,494 |
-- quantity <= 0 22,950 | price <= 0 6,207 | non-merchandise 5,912 |
-- dropped in total 264,720
SELECT
  count_if(lines.customer_id IS NULL) AS no_customer_id,
  count_if(lines.is_cancellation) AS cancellations,
  count_if(NOT lines.quantity > 0) AS quantity_not_positive,
  count_if(NOT lines.unit_price > 0) AS price_not_positive,
  count_if(codes.stock_code IS NOT NULL) AS non_merchandise,
  count(*) - (SELECT count(*) FROM retail.silver.sales_lines) AS dropped_in_total
FROM retail.silver.invoice_lines AS lines
LEFT JOIN retail.silver.non_merchandise_stock_codes AS codes
  ON lines.stock_code = codes.stock_code;

-- 3. Expectation metrics as recorded by the pipeline (event log) -----------
-- Replace <pipeline_id> with the id shown on the pipeline page.
-- Only the pipeline's run-as user (you) can read its event log by default.
WITH latest_update AS (
  SELECT origin.update_id AS update_id
  FROM event_log('<pipeline_id>')
  WHERE event_type = 'create_update'
  ORDER BY timestamp DESC
  LIMIT 1
),
expectations AS (
  SELECT explode(from_json(
    details:flow_progress:data_quality:expectations,
    'array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>'
  )) AS e
  FROM event_log('<pipeline_id>')
  WHERE event_type = 'flow_progress'
    AND origin.update_id = (SELECT update_id FROM latest_update)
)
SELECT
  e.dataset,
  e.name AS expectation,
  sum(e.passed_records) AS passed_records,
  sum(e.failed_records) AS failed_records
FROM expectations
GROUP BY e.dataset, e.name
ORDER BY e.dataset, e.name;

-- 4. Business figures, to compare with BigQuery ----------------------------
-- expected: 36,594 invoices | 5,852 customers | revenue 17,434,464.747 |
--           26,055 exact duplicates | 23.1 % of customers make 80 % of revenue
SELECT
  count(DISTINCT invoice_id) AS invoices,
  count(DISTINCT customer_id) AS customers,
  sum(line_amount) AS revenue_gbp,
  count_if(is_exact_duplicate) AS exact_duplicates,
  (SELECT round(100 * avg(CASE WHEN is_in_top_80pct_revenue THEN 1 ELSE 0 END), 1)
     FROM retail.gold.customer_revenue_concentration) AS pct_customers_making_80pct_revenue
FROM retail.silver.sales_lines;

-- 5. Integrity checks that expectations cannot express (they see one row) ---
-- Every column must be 0.
WITH silver AS (
  SELECT sum(line_amount) AS revenue FROM retail.silver.sales_lines
),
daily AS (
  SELECT
    sum(revenue) AS revenue,
    datediff(max(date_day), min(date_day)) + 1 - count(*) AS missing_days
  FROM retail.gold.daily_revenue
),
rfm AS (
  SELECT count(*) - count(DISTINCT customer_id) AS duplicate_customers
  FROM retail.gold.customer_rfm
),
pareto AS (
  SELECT max(cumulative_revenue_share) AS final_share
  FROM retail.gold.customer_revenue_concentration
)
SELECT
  CAST(abs(daily.revenue - silver.revenue) > 0.01 AS INT) AS gold_revenue_differs_from_silver,
  daily.missing_days AS missing_days_in_daily_revenue,
  rfm.duplicate_customers AS duplicate_customers_in_rfm,
  CAST(abs(pareto.final_share - 1) > 0.000001 AS INT) AS pareto_does_not_end_at_100pct
FROM silver
CROSS JOIN daily
CROSS JOIN rfm
CROSS JOIN pareto;
