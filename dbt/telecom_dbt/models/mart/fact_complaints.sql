{{ config(
    materialized='incremental',
    unique_key='complaint_id',
    incremental_strategy='merge'
) }}

with unioned as (

    select * from {{ ref('stg_call_logs') }}

    union all

    select * from {{ ref('stg_webforms') }}

    union all

    select * from {{ ref('stg_social_media') }}
)

select
  complaint_id,
  customer_id,
  agent_id,
  complaint_category,
  resolution_status,
  complaint_date,
  resolution_date,
  channel,
  load_timestamp
from unioned

{% if is_incremental() %}
where load_timestamp > (select max(load_timestamp) from {{ this }})
{% endif %}
