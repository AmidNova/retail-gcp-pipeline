-- One row per raw invoice line: renamed, typed, flagged. No rows are filtered here.
with source as (

    select * from {{ source('retail_raw', 'online_retail') }}

),

renamed as (

    select
        cast(Invoice as {{ dbt.type_string() }}) as invoice_id,
        cast(StockCode as {{ dbt.type_string() }}) as stock_code,
        trim(Description) as description,
        cast(Quantity as {{ dbt.type_bigint() }}) as quantity,
        cast(Price as {{ dbt.type_numeric() }}) as unit_price,
        {{ dbt.safe_cast("InvoiceDate", dbt.type_timestamp()) }} as invoiced_at,
        -- Customer_id may arrive as "13085.0": normalise to "13085"
        cast(
            cast(Customer_id as {{ dbt.type_bigint() }}) as {{ dbt.type_string() }}
        ) as customer_id,
        trim(Country) as country_name
    from source

)

select
    invoice_id,
    stock_code,
    description,
    quantity,
    unit_price,
    invoiced_at,
    customer_id,
    country_name,
    quantity * unit_price as line_amount,
    invoice_id like 'C%' as is_cancellation,
    customer_id is null as is_guest_checkout
from renamed
