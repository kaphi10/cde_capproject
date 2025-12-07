{{ config(materialized='view') }}

select
    request_id,
    lower(customer_id) as customer_id,
    lower(agent_id) as agent_id,
    upper(complaint_category) as complaint_type,
    upper(resolutionstatus) as resolution_status,
    request_date,
    resolution_date,
    webform_generation_date as generation_date,
    load_timestamp
from {{ source('telecom_data', 'webforms') }}
where request_id is not null