-- Recency / Frequency / Monetary features per customer for the KMeans model.
-- Recency is measured from the day after the last sale in the dataset.
with invoices as (

    select
        customer_id,
        invoice_id,
        max(invoice_date) as invoice_date,
        sum(line_amount) as invoice_amount
    from {{ ref('fct_invoice_lines') }}
    group by customer_id, invoice_id

),

customers as (

    select
        customer_id,
        max(invoice_date) as last_invoice_date,
        count(distinct invoice_id) as frequency,
        sum(invoice_amount) as monetary
    from invoices
    group by customer_id

),

bounds as (

    select cast({{ dbt.dateadd('day', 1, 'max(invoice_date)') }} as date) as reference_date
    from invoices

)

select
    customers.customer_id,
    {{ dbt.datediff('last_invoice_date', 'reference_date', 'day') }} as recency_days,
    customers.frequency,
    customers.monetary
from customers
cross join bounds
