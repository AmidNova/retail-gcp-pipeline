select
    nicename as country_name,
    iso as country_iso_code
from {{ ref('country_codes') }}
