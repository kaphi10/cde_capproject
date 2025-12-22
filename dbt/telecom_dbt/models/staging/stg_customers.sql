{{ config(materialized='view') }}


select
    customer_id,
    name as customer_name,
    gender,
    date_of_birth,
    signup_date,
    lower(email) as email,
    address,
    load_timestamp
from {{ source('telecom_data', 'customers') }}
where customer_id is not null

