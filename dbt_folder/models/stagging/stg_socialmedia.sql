{{ config(materialized='view') }}

with raw as (
    select
        NULLIF(trim("complaint_id"), '') as post_id,
        NULLIF(trim(regexp_replace("customeR iD", '\s+', '')),'') as customer_id_raw,
        regexp_replace(trim("COMPLAINT_catego ry"), '\s+', ' ') as complaint_type_raw,
        NULLIF(trim("agent ID"), '') as agent_id_raw,
        NULLIF(trim("resolutionstatus"), '') as resolution_status_raw,
        try_cast(NULLIF(trim("request_date"), '') as timestamp) as request_date,
        try_cast(NULLIF(trim("resolution_date"), '') as timestamp) as resolution_date,
        NULLIF(trim("media_channel"), '') as media_channel,
        try_cast(NULLIF(trim("MediaComplaintGenerationDate"), '') as timestamp) as media_generation_date,
        current_timestamp as load_timestamp
    from {{ source('telecom_raw', 'socialmedia') }}
)

select
    post_id as complaint_id,
    lower(regexp_replace(customer_id_raw, '\s+', '')) as customer_id,
    upper(trim(complaint_type_raw)) as complaint_type,
    lower(regexp_replace(agent_id_raw, '\s+', '')) as agent_id,
    upper(trim(resolution_status_raw)) as resolution_status,
    request_date,
    resolution_date,
    media_channel,
    media_generation_date as source_generation_date,
    load_timestamp
from raw
where complaint_id is not null
