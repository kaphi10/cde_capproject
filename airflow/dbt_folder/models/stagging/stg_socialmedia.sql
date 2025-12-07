{{ config(materialized='view') }}

select
    complaint_id,
    lower(customer_id) as customer_id,
    lower(agent_id) as agent_id,
    upper(complaint_category) as complaint_type,
    upper(resolutionstatus) as resolution_status,
    request_date,
    resolution_date,
    media_channel,
    media_complaint_generation_date as generation_date,
    load_timestamp
from {{ source('telecom_data', 'socialmedia') }}
where complaint_id is not null
