-- Daily revenue time series feeding the BigQuery ML ARIMA_PLUS model.
-- Days without sales (the shop does not trade on Saturdays, Christmas closure)
-- are kept with revenue = 0 instead of being missing, so the model sees the
-- real weekly pattern rather than interpolated values.
with daily as (

    select
        invoice_date,
        sum(line_amount) as revenue
    from {{ ref('fct_invoice_lines') }}
    group by invoice_date

),

bounds as (

    select
        min(invoice_date) as first_date,
        max(invoice_date) as last_date
    from daily

)

select
    dates.date_day,
    coalesce(daily.revenue, 0) as revenue
from {{ ref('dim_dates') }} as dates
cross join bounds
left join daily
    on dates.date_day = daily.invoice_date
where dates.date_day between bounds.first_date and bounds.last_date
