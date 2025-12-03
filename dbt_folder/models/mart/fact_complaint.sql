{{ config(materialized='incremental', unique_key='complaint_event_id') }}

with calls as (
  select
    'callcenter_' || call_id as complaint_event_id,
    customer_id,
    agent_id,
    complaint_type,
    resolution_status,
    call_start_time as event_time,
    'callcenter' as source,
    load_timestamp
  from {{ ref('stg_callcenter') }}
),

webforms as (
  select
    'webform_' || complaint_id as complaint_event_id,
    customer_id,
    agent_id,
    complaint_type,
    resolution_status,
    request_date as event_time,
    'webform' as source,
    load_timestamp
  from {{ ref('stg_webforms') }}
),

social as (
  select
    'social_' || complaint_id as complaint_event_id,
    customer_id,
    agent_id,
    complaint_type,
    resolution_status,
    request_date as event_time,
    'socialmedia' as source,
    load_timestamp
  from {{ ref('stg_socialmedia') }}
),

union_all as (
  select * from calls
  union all
  select * from webforms
  union all
  select * from social
),

enriched as (
  select
    u.*,
    coalesce(cust.customer_name, '') as customer_name,
    cust.email as customer_email,
    ag.agent_name as agent_name,
    ag.state as agent_state
  from union_all u
  left join {{ ref('dim_customers') }} cust on u.customer_id = cust.customer_id
  left join {{ ref('dim_agents') }} ag on u.agent_id = ag.agent_id
)

select * from enriched

{% if is_incremental() %}
where load_timestamp > (select coalesce(max(load_timestamp), '1900-01-01'::timestamp) from {{ this }})
{% endif %}
