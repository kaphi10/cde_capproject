{{ config(materialized='view') }}


select
    customer_id,
    initcap(name) as customer_name,
    initcap(gender) as gender,
    date_of_birth,
    signup_date,
    lower(email) as email,
    address,
    load_timestamp
from {{ source('telecom_data', 'customers') }}
where customer_id is not null

