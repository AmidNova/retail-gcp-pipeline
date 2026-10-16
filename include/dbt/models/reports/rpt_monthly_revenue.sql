select
    dates.month_start_date,
    dates.calendar_year,
    dates.calendar_month,
    count(distinct lines.invoice_id) as invoice_count,
    sum(lines.line_amount) as revenue
from {{ ref('fct_invoice_lines') }} as lines
inner join {{ ref('dim_dates') }} as dates
    on lines.invoice_date = dates.date_day
group by
    dates.month_start_date,
    dates.calendar_year,
    dates.calendar_month
