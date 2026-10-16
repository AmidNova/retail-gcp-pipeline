-- =====================================================================
-- Metabase viz query pack — Retail GCP Pipeline
-- Paste each block into a new Metabase "SQL question" (BigQuery database).
-- Each block notes the recommended visualization + settings.
-- Project: retail-pipeline-317c24   Dataset: retail
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. REVENUE FORECAST — history + 30-day ARIMA_PLUS prediction
--    Viz: Line chart
--    X = jour, Y = ca  (add ca_prevu, borne_basse, borne_haute as extra series)
--    Color the forecast series differently; show the confidence band.
--    History limited to the last 120 days so the 30-day forecast stays readable.
-- ---------------------------------------------------------------------
WITH history AS (
  SELECT DATE(jour) AS jour, ca AS ca, NULL AS ca_prevu, NULL AS borne_basse, NULL AS borne_haute
  FROM `retail-pipeline-317c24.retail.ca_daily`
  WHERE DATE(jour) >= DATE_SUB(
    (SELECT MAX(DATE(jour)) FROM `retail-pipeline-317c24.retail.ca_daily`),
    INTERVAL 120 DAY
  )
),
forecast AS (
  SELECT DATE(jour) AS jour, NULL AS ca, ca_prevu, borne_basse, borne_haute
  FROM `retail-pipeline-317c24.retail.ca_forecast_results`
)
SELECT * FROM history
UNION ALL
SELECT * FROM forecast
ORDER BY jour;


-- ---------------------------------------------------------------------
-- 1b. FORECAST ONLY — clean 30-day view with confidence interval
--     Viz: Line chart, 3 series (ca_prevu solid, bornes dashed)
-- ---------------------------------------------------------------------
SELECT jour, ca_prevu, borne_basse, borne_haute
FROM `retail-pipeline-317c24.retail.ca_forecast_results`
ORDER BY jour;


-- ---------------------------------------------------------------------
-- 2. CUSTOMER SEGMENTS — RFM scatter (KMeans k=4)
--    Viz: Scatter / Bubble
--    X = recency, Y = monetary, bubble size = frequency, color = segment_label
--    Log scale on Y helps (whales dwarf the rest).
-- ---------------------------------------------------------------------
SELECT
  customer_id,
  recency,
  frequency,
  monetary,
  CASE segment
    WHEN '2' THEN 'Whales'
    WHEN '1' THEN 'Loyal / Active'
    WHEN '4' THEN 'Dormant'
    WHEN '3' THEN 'Churned'
    ELSE segment
  END AS segment_label
FROM `retail-pipeline-317c24.retail.customer_segments`;


-- ---------------------------------------------------------------------
-- 2b. SEGMENT SUMMARY — size + average RFM per segment
--     Viz: Bar chart (X = segment_label, Y = customers) or a table
-- ---------------------------------------------------------------------
SELECT
  CASE segment
    WHEN '2' THEN 'Whales'
    WHEN '1' THEN 'Loyal / Active'
    WHEN '4' THEN 'Dormant'
    WHEN '3' THEN 'Churned'
    ELSE segment
  END AS segment_label,
  COUNT(*)                          AS customers,
  ROUND(AVG(recency))               AS avg_recency_days,
  ROUND(AVG(frequency), 1)          AS avg_orders,
  ROUND(AVG(monetary))              AS avg_revenue_gbp,
  ROUND(SUM(monetary))              AS total_revenue_gbp
FROM `retail-pipeline-317c24.retail.customer_segments`
GROUP BY segment_label
ORDER BY avg_revenue_gbp DESC;


-- ---------------------------------------------------------------------
-- 3. REVENUE BY COUNTRY — world map
--    Viz: Map (Region map). Metabase matches on ISO-2 codes -> use `iso`.
-- ---------------------------------------------------------------------
SELECT
  d.iso,
  d.country,
  ROUND(SUM(f.total)) AS revenue_gbp
FROM `retail-pipeline-317c24.retail.fct_invoices` f
JOIN `retail-pipeline-317c24.retail.dim_customer` d ON f.customer_id = d.customer_id
GROUP BY d.iso, d.country
ORDER BY revenue_gbp DESC;


-- ---------------------------------------------------------------------
-- 4. TOP 10 PRODUCTS BY REVENUE
--    Viz: Row chart (horizontal bar)
-- ---------------------------------------------------------------------
SELECT
  p.description AS product,
  ROUND(SUM(f.total)) AS revenue_gbp
FROM `retail-pipeline-317c24.retail.fct_invoices` f
JOIN `retail-pipeline-317c24.retail.dim_product` p ON f.product_id = p.product_id
GROUP BY product
ORDER BY revenue_gbp DESC
LIMIT 10;


-- ---------------------------------------------------------------------
-- 5. MONTHLY REVENUE TREND — seasonality (strong Sep–Nov peak)
--    Viz: Bar or line chart, X = month
-- ---------------------------------------------------------------------
SELECT
  DATE_TRUNC(dt.datetime, MONTH) AS month,
  ROUND(SUM(f.total))            AS revenue_gbp
FROM `retail-pipeline-317c24.retail.fct_invoices` f
JOIN `retail-pipeline-317c24.retail.dim_datetime` dt ON f.datetime_id = dt.datetime_id
GROUP BY month
ORDER BY month;


-- ---------------------------------------------------------------------
-- 6. PARETO — revenue concentration (top ~23% customers = ~80% revenue)
--    Viz: Line chart, X = customer_rank_pct, Y = cumulative_pct
-- ---------------------------------------------------------------------
WITH ranked AS (
  SELECT
    customer_id,
    monetary,
    SUM(monetary) OVER (ORDER BY monetary DESC) AS running_total,
    SUM(monetary) OVER ()                       AS grand_total,
    ROW_NUMBER()  OVER (ORDER BY monetary DESC) AS rn,
    COUNT(*)      OVER ()                        AS total_customers
  FROM `retail-pipeline-317c24.retail.customer_rfm`
)
SELECT
  ROUND(100 * rn / total_customers, 2)       AS customer_rank_pct,
  ROUND(100 * running_total / grand_total, 2) AS cumulative_revenue_pct
FROM ranked
ORDER BY rn;
