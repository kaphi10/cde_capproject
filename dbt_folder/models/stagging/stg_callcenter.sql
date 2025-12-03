{{ config(materialized='view') }}

-- STEP 1: READ RAW TABLE
with raw as (

    select
        -- resolve odd column names with quotes
        trim("call ID") as call_id,
        
        -- normalize customer_id
        lower(regexp_replace(trim("customeR iD"), '\s+', '')) as customer_id_raw,
        
        -- normalize complaint category (remove weird spacing)
        upper(trim(regexp_replace("COMPLAINT_catego ry", '\s+', ' '))) as complaint_category_raw,

        -- normalize agent
        lower(regexp_replace(trim("agent ID"), '\s+', '')) as agent_id_raw,

        -- normalize resolution status
        upper(trim("resolutionstatus")) as resolution_status_raw,

        -- timestamps (convert strings → timestamp)
        try_cast("call_start_time" as timestamp) as call_start_time,
        try_cast("call_end_time" as timestamp) as call_end_time,
        try_cast("callLogsGenerationDate" as timestamp) as generation_date,

        load_timestamp

    from {{ source('telecom_raw', 'call_logs') }}

)

-- STEP 2: CLEANED OUTPUT
select
    call_id,
    customer_id_raw as customer_id,
    agent_id_raw as agent_id,

    complaint_category_raw as complaint_category,
    resolution_status_raw as resolution_status,

    call_start_time,
    call_end_time,
    generation_date,
    load_timestamp

from raw
where call_id is not null
