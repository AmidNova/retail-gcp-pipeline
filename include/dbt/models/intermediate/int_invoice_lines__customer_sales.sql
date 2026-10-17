-- Scope of the analytical star schema: positive sales lines of identified customers.
-- Excluded on purpose (and counted by the reconciliation test):
--   * returns / cancellations  -> quantity <= 0
--   * free or adjustment lines -> unit_price <= 0
--   * guest checkouts          -> no customer_id, so no customer-level analysis
--   * non-merchandise codes    -> postage, manual adjustments, fees, vouchers...
with lines as (

    select * from {{ ref('stg_retail__invoice_lines') }}

),

non_merchandise as (

    select stock_code from {{ ref('stg_reference__non_merchandise_stock_codes') }}

)

select
    invoice_id,
    invoiced_at,
    cast(invoiced_at as date) as invoice_date,
    customer_id,
    country_name,
    {{ dbt_utils.generate_surrogate_key(['stock_code', 'description', 'unit_price']) }} as product_key,
    stock_code,
    description,
    quantity,
    unit_price,
    line_amount
from lines
where
    stock_code not in (select non_merchandise.stock_code from non_merchandise)
    and quantity > 0
    and unit_price > 0
    and customer_id is not null
