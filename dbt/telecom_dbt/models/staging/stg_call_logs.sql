{{ config(materialized='view') }}

select
  call_id                 as complaint_id,
  customer_id,
  agent_id,
  complaint_category,
  resolution_status,
  call_start_time         as complaint_date,
  call_end_time           as resolution_date,
  'call'                  as channel,
  load_timestamp
from {{ source('telecom_data', 'call_logs') }}
where call_id is not null
