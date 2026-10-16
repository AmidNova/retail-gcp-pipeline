-- Pareto view of revenue: customers ranked by revenue with cumulative shares.
-- Share of customers producing 80% of revenue =
--     avg(case when is_in_top_80pct_revenue then 1 else 0 end)
with customer_revenue as (

    select
        customer_id,
        sum(line_amount) as revenue
    from {{ ref('fct_invoice_lines') }}
    group by customer_id

),

ranked as (

    select
        customer_id,
        revenue,
        row_number() over (order by revenue desc, customer_id asc) as revenue_rank,
        count(*) over () as customer_count,
        sum(revenue) over (
            order by revenue desc, customer_id asc
            rows between unbounded preceding and current row
        ) as cumulative_revenue,
        sum(revenue) over () as total_revenue
    from customer_revenue

)

select
    customer_id,
    revenue,
    revenue_rank,
    cast(revenue_rank as {{ dbt.type_float() }}) / customer_count as customer_share,
    cast(cumulative_revenue as {{ dbt.type_float() }}) / total_revenue as cumulative_revenue_share,
    -- true for the customers needed to reach 80% of revenue (rank order)
    cumulative_revenue - revenue < 0.8 * total_revenue as is_in_top_80pct_revenue
from ranked
