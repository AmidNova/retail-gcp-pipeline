select
    customers.country_name,
    customers.country_iso_code,
    count(distinct lines.invoice_id) as invoice_count,
    count(distinct lines.customer_id) as customer_count,
    sum(lines.line_amount) as revenue
from {{ ref('fct_invoice_lines') }} as lines
inner join {{ ref('dim_customers') }} as customers
    on lines.customer_id = customers.customer_id
group by
    customers.country_name,
    customers.country_iso_code
