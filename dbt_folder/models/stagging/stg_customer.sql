{{ config(materialized='view') }}

select
  lower(trim(customer_id)) as customer_id,
  initcap(trim(name)) as customer_name,
  initcap(trim(Gender)) as gender,
  -- attempt to parse birth date; adjust format if needed
  try_cast(NULLIF(trim("DATE of biRTH"), '') as date) as date_of_birth,
  try_cast(NULLIF(trim(signup_date), '') as timestamp) as signup_date,
  lower(trim(email)) as email,
  trim(address) as address,
  current_timestamp as load_timestamp
from {{ source('telecom_raw', 'customers') }}
where customer_id is not null
