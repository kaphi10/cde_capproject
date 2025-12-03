{{ config(materialized='table') }}

with src as (
  select * from {{ ref('stg_agents') }}
),

dedup as (
  select
    agent_id,
    max(agent_name) as agent_name,
    max(experience_years) as experience_years,
    max(state) as state,
    max(load_timestamp) as last_load_ts
  from src
  group by agent_id
)

select * from dedup
