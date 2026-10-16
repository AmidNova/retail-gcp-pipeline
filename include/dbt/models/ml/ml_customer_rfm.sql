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

reference_date as (

    select cast({{ dbt.dateadd('day', 1, 'max(invoice_date)') }} as date) as reference_date
    from invoices

)

select
    invoices.customer_id,
    {{ dbt.datediff('max(invoices.invoice_date)', 'reference_date.reference_date', 'day') }} as recency_days,
    count(distinct invoices.invoice_id) as frequency,
    sum(invoices.invoice_amount) as monetary
from invoices
cross join reference_date
group by invoices.customer_id, reference_date.reference_date
