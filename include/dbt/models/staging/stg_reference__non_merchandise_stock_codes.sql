select
    stock_code,
    category as non_merchandise_category
from {{ ref('non_merchandise_stock_codes') }}
