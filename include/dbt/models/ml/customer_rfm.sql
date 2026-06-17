-- customer_rfm.sql
-- Recency / Frequency / Monetary features per customer — feeds the KMeans segmentation.
-- Recency is measured against the day after the last transaction in the dataset.

WITH ref AS (
    SELECT DATE_ADD(MAX(DATE(datetime)), INTERVAL 1 DAY) AS ref_date
    FROM {{ ref('dim_datetime') }}
    WHERE datetime IS NOT NULL
),
inv AS (
    SELECT
        f.customer_id,
        f.invoice_id,
        MAX(DATE(d.datetime)) AS inv_date,
        SUM(f.total) AS inv_total
    FROM {{ ref('fct_invoices') }} f
    JOIN {{ ref('dim_datetime') }} d USING(datetime_id)
    GROUP BY f.customer_id, f.invoice_id
)
SELECT
    customer_id,
    DATE_DIFF((SELECT ref_date FROM ref), MAX(inv_date), DAY) AS recency,
    COUNT(DISTINCT invoice_id) AS frequency,
    ROUND(SUM(inv_total), 2) AS monetary
FROM inv
GROUP BY customer_id
HAVING monetary > 0
