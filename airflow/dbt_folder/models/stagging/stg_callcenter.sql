{{ config(materialized='view') }}

-- STEP 1: READ RAW TABLE



select
    call_id ,
    lower(customerid) as customer_id,
    lower(agent_id) as agent_id,
    upper(complaint_category) as complaint_type,
    upper(resolution_status) as resolution_status,
    call_start_time,
    call_end_time,
    call_logs_generation_date as generation_date,
    load_timestamp
from {{ source('telecom_data', 'call_logs') }}
where call_id is not null