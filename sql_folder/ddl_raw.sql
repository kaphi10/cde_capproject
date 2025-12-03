CREATE SCHEMA IF NOT EXISTS raw_schema;

CREATE TABLE IF NOT EXISTS raw_schema.call_logs (
    call_id VARCHAR,
    customer_id VARCHAR,
    complaint_category VARCHAR,
    agent_id VARCHAR,
    call_start_time TIMESTAMP,
    call_end_time TIMESTAMP,
    resolution_status VARCHAR,
    call_logs_generation_date TIMESTAMP,
    load_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_schema.webforms (
    request_id VARCHAR,
    customer_id VARCHAR,
    complaint_category VARCHAR,
    agent_id VARCHAR,
    resolution_status VARCHAR,
    request_date TIMESTAMP,
    resolution_date TIMESTAMP,
    webform_generation_date TIMESTAMP,
    load_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_schema.social_media (
    complaint_id VARCHAR,
    customer_id VARCHAR,
    complaint_category VARCHAR,
    agent_id VARCHAR,
    resolution_status VARCHAR,
    request_date TIMESTAMP,
    resolution_date TIMESTAMP,
    media_channel VARCHAR,
    media_complaint_generation_date TIMESTAMP,
    load_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_schema.agents (
    agent_id VARCHAR,
    name VARCHAR,
    experience INTEGER,
    state VARCHAR,
    load_timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_schema.customers (
    customer_id VARCHAR,
    name VARCHAR,
    gender VARCHAR,
    date_of_birth DATE,
    signup_date DATE,
    email VARCHAR,
    address VARCHAR,
    load_timestamp TIMESTAMP
);