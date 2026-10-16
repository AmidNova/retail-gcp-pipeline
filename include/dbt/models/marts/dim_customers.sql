-- One row per customer. A handful of customers ordered from two countries:
-- the country of their most recent purchase wins.
with lines as (

    select * from {{ ref('int_invoice_lines__customer_sales') }}

),

countries as (

    select * from {{ ref('stg_reference__country_codes') }}

),

ranked_countries as (

    select
        customer_id,
        country_name,
        row_number() over (
            partition by customer_id
            order by invoiced_at desc, country_name asc
        ) as recency_rank
    from lines

),

activity as (

    select
        customer_id,
        min(invoiced_at) as first_purchase_at,
        max(invoiced_at) as last_purchase_at
    from lines
    group by customer_id

)

select
    activity.customer_id,
    ranked_countries.country_name,
    countries.country_iso_code,
    activity.first_purchase_at,
    activity.last_purchase_at
from activity
inner join ranked_countries
    on
        activity.customer_id = ranked_countries.customer_id
        and ranked_countries.recency_rank = 1
left join countries
    on ranked_countries.country_name = countries.country_name
