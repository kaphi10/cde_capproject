{{ config(materialized='table') }}

with src as (
  select * from {{ ref('stg_customers') }}
),

dedup as (
  select
    customer_id,
    max(customer_name) keep (dense_rank first order by load_timestamp desc) as customer_name,
    max(email) keep (dense_rank first order by load_timestamp desc) as email,
    max(gender) keep (dense_rank first order by load_timestamp desc) as gender,
    max(date_of_birth) keep (dense_rank first order by load_timestamp desc) as date_of_birth,
    max(signup_date) keep (dense_rank first order by load_timestamp desc) as signup_date,
    max(address) keep (dense_rank first order by load_timestamp desc) as address,
    max(load_timestamp) as last_load_ts
  from src
  group by customer_id
)

select * from dedup
