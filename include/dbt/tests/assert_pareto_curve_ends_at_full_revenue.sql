-- The last customer of the Pareto curve must carry the cumulative share to 100%,
-- and the curve must never decrease.
with curve as (

    select
        revenue_rank,
        cumulative_revenue_share,
        lag(cumulative_revenue_share) over (order by revenue_rank) as previous_share,
        max(revenue_rank) over () as last_rank
    from {{ ref('rpt_customer_revenue_concentration') }}

)

select *
from curve
where
    (revenue_rank = last_rank and abs(cumulative_revenue_share - 1) > 1e-9)
    or cumulative_revenue_share < previous_share
