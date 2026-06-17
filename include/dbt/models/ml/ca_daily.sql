-- ca_daily.sql
-- Daily revenue time series — feature table feeding the ARIMA_PLUS forecast model.

SELECT
    DATE(d.datetime) AS jour,
    SUM(f.total) AS ca
FROM {{ ref('fct_invoices') }} f
JOIN {{ ref('dim_datetime') }} d USING(datetime_id)
WHERE d.datetime IS NOT NULL
GROUP BY jour
