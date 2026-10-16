-- Grain: one row per in-scope invoice line (see int_invoice_lines__customer_sales).
-- The source contains exact duplicate lines; they are kept, as in the source,
-- so there is no unique key at this grain.
select
    invoice_id,
    invoice_date,
    invoiced_at,
    customer_id,
    product_key,
    quantity,
    unit_price,
    line_amount
from {{ ref('int_invoice_lines__customer_sales') }}
