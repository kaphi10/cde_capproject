{{ config(materialized='table') }}

with ranked as (

    select
        customer_id,
        customer_name,
        gender,
        date_of_birth,
        signup_date,
        lower(trim(email))         as email,
        address,
        load_timestamp,
        row_number() over (
            partition by customer_id
            order by load_timestamp desc
        ) as rn
    from {{ ref('stg_customers') }}

)

select
    customer_id,
    customer_name,
    gender,
    date_of_birth,
    signup_date,
    email,
    address,
    load_timestamp as last_load_ts
from ranked
where rn = 1
