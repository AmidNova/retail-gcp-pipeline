-- All products, not a top N: ranking and limits belong to the BI tool.
select
    products.stock_code,
    products.description,
    sum(lines.quantity) as quantity_sold,
    sum(lines.line_amount) as revenue
from {{ ref('fct_invoice_lines') }} as lines
inner join {{ ref('dim_products') }} as products
    on lines.product_key = products.product_key
group by
    products.stock_code,
    products.description
