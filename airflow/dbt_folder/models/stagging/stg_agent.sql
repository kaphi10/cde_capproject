{{ config(materialized='view') }}

select
  agent_id,
  initcap(trim("name")) as agent_name,
  initcap(trim("experience")) as experience_type,
  initcap(trim("state")) as state,
  load_timestamp
from {{ source('telecom_data', 'agents') }}
where agent_id is not null
