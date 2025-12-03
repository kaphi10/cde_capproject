{{ config(materialized='view') }}

select
  trim("iD") as agent_id,
  initcap(trim("NamE")) as agent_name,
  try_cast(trim("experience") as int) as experience_years,
  initcap(trim("state")) as state,
  current_timestamp as load_timestamp
from {{ source('telecom_raw', 'agents') }}
where trim("iD") is not null
