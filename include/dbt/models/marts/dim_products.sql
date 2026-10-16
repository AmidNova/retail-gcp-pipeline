-- One row per (stock_code, description, unit_price): the same stock code is
-- sold under several labels and prices, so the product grain is the triple.
select distinct
    product_key,
    stock_code,
    description,
    unit_price
from {{ ref('int_invoice_lines__customer_sales') }}
