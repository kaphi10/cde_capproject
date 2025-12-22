{{ config(materialized='view') }}

select
  agent_id,
  name as agent_name,
  experience as experience_type,
  state,
  load_timestamp
from {{ source('telecom_data', 'agents') }}
where agent_id is not null