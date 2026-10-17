-- GOLD: business tables read by analysts, recomputed from silver.sales_lines.
-- Ports of ml_daily_revenue, ml_customer_rfm and rpt_customer_revenue_concentration.

-- Revenue per calendar day, with zero on days without sales (the shop is closed
-- on Saturdays): a forecasting model needs a series without gaps.
CREATE OR REFRESH MATERIALIZED VIEW retail.gold.daily_revenue (
  CONSTRAINT non_negative_revenue EXPECT (revenue >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'Revenue per day between the first and the last sale, zero-filled.'
AS WITH daily AS (
  SELECT invoice_date, sum(line_amount) AS revenue
  FROM retail.silver.sales_lines
  GROUP BY invoice_date
),
calendar AS (
  SELECT explode(sequence(min(invoice_date), max(invoice_date), INTERVAL 1 DAY)) AS date_day
  FROM daily
)
SELECT
  calendar.date_day,
  coalesce(daily.revenue, 0) AS revenue
FROM calendar
LEFT JOIN daily ON calendar.date_day = daily.invoice_date;

-- Recency / Frequency / Monetary per customer. Recency is counted from the day
-- after the last sale in the dataset.
CREATE OR REFRESH MATERIALIZED VIEW retail.gold.customer_rfm (
  CONSTRAINT positive_frequency EXPECT (frequency > 0) ON VIOLATION FAIL UPDATE,
  CONSTRAINT positive_monetary EXPECT (monetary > 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'RFM features per customer (recency in days, invoices, revenue in GBP).'
AS WITH customers AS (
  SELECT
    customer_id,
    max(invoice_date) AS last_invoice_date,
    count(DISTINCT invoice_id) AS frequency,
    sum(line_amount) AS monetary
  FROM retail.silver.sales_lines
  GROUP BY customer_id
),
bounds AS (
  SELECT date_add(max(invoice_date), 1) AS reference_date
  FROM retail.silver.sales_lines
)
SELECT
  customers.customer_id,
  datediff(bounds.reference_date, customers.last_invoice_date) AS recency_days,
  customers.frequency,
  customers.monetary
FROM customers
CROSS JOIN bounds;

-- Pareto curve: customers ranked by revenue, with cumulative shares.
-- Share of customers making 80 % of revenue =
--     avg(CASE WHEN is_in_top_80pct_revenue THEN 1 ELSE 0 END)
CREATE OR REFRESH MATERIALIZED VIEW retail.gold.customer_revenue_concentration (
  CONSTRAINT share_in_range EXPECT (cumulative_revenue_share > 0 AND cumulative_revenue_share <= 1.000001)
    ON VIOLATION FAIL UPDATE
)
COMMENT 'Customers ranked by revenue with cumulative revenue share (Pareto).'
AS WITH customer_revenue AS (
  SELECT customer_id, sum(line_amount) AS revenue
  FROM retail.silver.sales_lines
  GROUP BY customer_id
),
ranked AS (
  SELECT
    customer_id,
    revenue,
    row_number() OVER (ORDER BY revenue DESC, customer_id ASC) AS revenue_rank,
    count(*) OVER () AS customer_count,
    sum(revenue) OVER (
      ORDER BY revenue DESC, customer_id ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_revenue,
    sum(revenue) OVER () AS total_revenue
  FROM customer_revenue
)
SELECT
  customer_id,
  revenue,
  revenue_rank,
  cast(revenue_rank AS DOUBLE) / customer_count AS customer_share,
  cast(cumulative_revenue AS DOUBLE) / cast(total_revenue AS DOUBLE) AS cumulative_revenue_share,
  -- true for the customers needed to reach 80 % of revenue (rank order)
  cumulative_revenue - revenue < 0.8 * total_revenue AS is_in_top_80pct_revenue
FROM ranked;
