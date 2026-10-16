-- Every report must add up to the fact table's revenue: catches join fan-outs
-- and silently dropped rows. Returns one row per report that does not match.
with fact as (

    select sum(line_amount) as revenue from {{ ref('fct_invoice_lines') }}

),

reports as (

    select
        'rpt_revenue_by_country' as report_name,
        sum(revenue) as revenue
    from {{ ref('rpt_revenue_by_country') }}
    union all
    select
        'rpt_monthly_revenue' as report_name,
        sum(revenue) as revenue
    from {{ ref('rpt_monthly_revenue') }}
    union all
    select
        'rpt_product_sales' as report_name,
        sum(revenue) as revenue
    from {{ ref('rpt_product_sales') }}
    union all
    select
        'rpt_customer_revenue_concentration' as report_name,
        sum(revenue) as revenue
    from {{ ref('rpt_customer_revenue_concentration') }}

)

select
    reports.report_name,
    reports.revenue as report_revenue,
    fact.revenue as fact_revenue
from reports
cross join fact
where abs(reports.revenue - fact.revenue) > 0.01
