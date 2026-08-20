select
    "customer_id" as customer_id,
    "customer_unique_id" as customer_unique_id,
    "customer_zip_code_prefix" as customer_zip_code_prefix,
    trim("customer_city") as customer_city,
    upper(trim("customer_state")) as customer_state

from {{ source('olist_raw', 'customers') }}