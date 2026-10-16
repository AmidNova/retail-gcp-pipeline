-- Calendar dimension generated from a date spine (no gaps, unlike dates found
-- in the facts). Bounds are project vars in dbt_project.yml.
{% set start_date = "cast('" ~ var('calendar_start_date') ~ "' as date)" %}
{% set end_date = "cast('" ~ var('calendar_end_date') ~ "' as date)" %}

with spine as (

    {{ dbt_utils.date_spine(datepart="day", start_date=start_date, end_date=end_date) }}

),

dates as (

    select cast(date_day as date) as date_day
    from spine

)

select
    date_day,
    extract(year from date_day) as calendar_year,
    extract(quarter from date_day) as calendar_quarter,
    extract(month from date_day) as calendar_month,
    extract(day from date_day) as day_of_month,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date) as month_start_date
from dates
