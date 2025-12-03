-- Example COPYs — ensure the IAM role ARN below is allowed in Redshift
COPY raw_schema.customers
FROM 's3://core-telecoms-data-lake/raw/customers/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/redshift-dedicated-role' -- changed
FORMAT AS PARQUET;

COPY raw_schema.callcenter
FROM 's3://core-telecoms-data-lake/raw/callcenter/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/coretelecom-redshift-copy-role'
FORMAT AS PARQUET;

COPY raw_schema.socialmedia
FROM 's3://core-telecoms-data-lake/raw/socialmedia/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/coretelecom-redshift-copy-role'
FORMAT AS PARQUET;

COPY raw_schema.webforms
FROM 's3://core-telecoms-data-lake/raw/webforms/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/coretelecom-redshift-copy-role'
FORMAT AS PARQUET;

COPY raw_schema.agents
FROM 's3://core-telecoms-data-lake/raw/agents/'
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/coretelecom-redshift-copy-role'
FORMAT AS PARQUET;