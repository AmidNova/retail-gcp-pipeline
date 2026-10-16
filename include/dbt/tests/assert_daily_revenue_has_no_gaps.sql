-- ARIMA_PLUS expects one row per day: the series must have no missing dates.
with series as (

    select
        count(*) as day_count,
        min(date_day) as first_day,
        max(date_day) as last_day
    from {{ ref('ml_daily_revenue') }}

)

select *
from series
where day_count != {{ dbt.datediff('first_day', 'last_day', 'day') }} + 1
