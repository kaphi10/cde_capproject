{{ config(materialized='view') }}

select
  request_id              as complaint_id,
  customer_id,
  agent_id,
  complaint_category,
  resolution_status,
  request_date            as complaint_date,
  resolution_date,
  'webform'               as channel,
  load_timestamp
from {{ source('telecom_data', 'webforms') }}
where request_id is not null
