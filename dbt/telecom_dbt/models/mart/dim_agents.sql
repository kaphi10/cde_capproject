{{ config(materialized='table') }}

with ranked as (
  select *,
         row_number() over (
           partition by agent_id
           order by load_timestamp desc
         ) as rn
  from {{ ref('stg_agents') }}
)

select
  agent_id,
  agent_name,
  experience_type,
  state,
  load_timestamp as last_load_ts
from ranked
where rn = 1
