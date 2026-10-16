-- ARIMA_PLUS expects one row per day: the series must have no missing dates.
select
    count(*) as day_count,
    min(date_day) as first_day,
    max(date_day) as last_day
from {{ ref('ml_daily_revenue') }}
having count(*) != {{ dbt.datediff('min(date_day)', 'max(date_day)', 'day') }} + 1
