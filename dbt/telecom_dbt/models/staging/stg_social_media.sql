{{ config(materialized='view') }}

select
  complaint_id,
  customer_id,
  agent_id,
  complaint_category,
  resolution_status,
  request_date            as complaint_date,
  resolution_date,
  'social'                as channel,
  load_timestamp
from {{ source('telecom_data', 'social_media') }}
where complaint_id is not null
