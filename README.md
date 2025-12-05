# Telecom Data Pipeline

A containerized end-to-end data pipeline for extracting, loading, and transforming telecommunications data. Built with Apache Airflow, dbt, AWS services (S3, Redshift Serverless, Glue), and Google Sheets integration.

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Docker Setup](#docker-setup)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Pipeline Workflows](#pipeline-workflows)
- [Troubleshooting](#troubleshooting)

## 🎯 Project Overview

This project implements a three-stage data pipeline:

1. **Extract** - Collects data from multiple sources (S3, Google Sheets, PostgreSQL)
2. **Load** - Ingests raw data into AWS Redshift Serverless
3. **Transform** - Applies business logic using dbt to create staging and mart models

### Key Features

- **Multi-source extraction**: AWS S3, Google Sheets (agents), PostgreSQL (webforms), JSON/CSV parsing
- **Idempotent operations**: Prevents duplicate data loads
- **Serverless architecture**: AWS Redshift Serverless for scalable data warehouse
- **Modern transformation**: dbt for SQL-based data transformations
- **Orchestration**: Apache Airflow for workflow scheduling and monitoring
- **Containerized**: Docker & Docker Compose for consistent environments
- **Error handling**: Automatic retries and failure notifications

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Data Sources                               │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐   │
│  │ AWS S3       │  │ Google Sheets│  │ PostgreSQL (WebDB) │   │
│  │ (Customers,  │  │ (Agents)     │  │ (Webforms)         │   │
│  │ Call Logs,   │  └──────────────┘  └────────────────────┘   │
│  │ Social Media)│                                               │
│  └──────────────┘                                               │
└────────────────┬────────────────────────────────────────────────┘
                 │
        ┌────────▼────────┐
        │  EXTRACT (DAG)  │  [Airflow]
        │ dag_extract_raw │
        └────────┬────────┘
                 │
        ┌────────▼──────────────┐
        │ S3 Data Lake          │
        │ (kafayat-...-data-    │
        │  lake/raw/*)          │
        └────────┬──────────────┘
                 │
        ┌────────▼────────┐
        │  LOAD (DAG)     │  [Airflow]
        │ dag_load_raw_   │
        │ to_redshift     │
        └────────┬────────┘
                 │
        ┌────────▼──────────────┐
        │ Redshift Serverless   │
        │ (raw_schema.*)        │
        └────────┬──────────────┘
                 │
        ┌────────▼────────┐
        │ TRANSFORM (DAG) │  [Airflow + dbt]
        │ dag_dbt_         │
        │ transform        │
        └────────┬────────┘
                 │
        ┌────────▼──────────────┐
        │ Redshift Serverless   │
        │ (staging.*, marts.*)  │
        └───────────────────────┘
```

## 🔧 Prerequisites

### Local Development
- Python 3.12+
- Docker & Docker Compose
- AWS CLI configured
- Terraform (for infrastructure management)

### AWS Credentials
- AWS Account with permissions for: S3, Redshift, Glue, IAM
- AWS Access Key ID & Secret Access Key

### External Integrations
- Google Cloud service account JSON key (for Google Sheets)
- Source & destination AWS profiles configured

##  Project Structure

```
telecom_pipeline/
├── airflow/                          # Airflow DAGs and utilities
│   ├── dags/
│   │   ├── extraction_dag.py         # Extract raw data from sources
│   │   ├── load_raw_redshift.py      # Load data to Redshift
│   │   ├── dbt_transformation_dag.py # Run dbt transformations
│   │   ├── collect_data/
│   │   │   └── get_data.py           # Data extraction functions
│   │   └── utilities/
│   │       ├── helper.py             # AWS, DB connection helpers
│   │       ├── redshift_utils.py     # Redshift utilities
│   │       └── notify.py             # Failure/success callbacks
│   └── airflow.cfg                   # Airflow configuration
│
├── dbt_folder/                       # dbt project
│   ├── dbt_project.yaml              # dbt configuration
│   ├── profile.yml                   # Redshift connection profile
│   └── models/
│       ├── staging/                  # Staging layer (stg_*)
│       │   ├── stg_agent.sql
│       │   ├── stg_customer.sql
│       │   ├── stg_callcenter.sql
│       │   ├── stg_socialmedia.sql
│       │   └── stg_webform.sql
│       └── mart/                     # Mart layer (dim_*, fact_*)
│           ├── dim_agent.sql
│           ├── dim_customer.sql
│           └── fact_complaint.sql
│
├── extract_data/                     # Standalone extraction scripts
│   ├── extraction.py
│   ├── collect_data/
│   │   └── get_data.py
│   └── utilities/
│       └── helper.py
│
├── terraform_folder/                 # Infrastructure as Code
│   ├── backend.tf
│   ├── provider.tf
│   ├── redshift.tf                   # Redshift Serverless setup
│   ├── data_lake.tf                  # S3 setup
│   ├── glue_database.tf              # AWS Glue catalog
│   └── local.tf
│
├── sql_folder/                       # SQL utilities
│   ├── ddl_raw.sql                   # Create raw schema tables
│   └── copy_from_s3.sql              # COPY command templates
│
├── Dockerfile                        # Docker image for Airflow
├── docker-compose.yml                # Multi-container orchestration
├── .env                              # Environment variables
├── requirements.txt                  # Python dependencies
└── README.md                         # This file
```

## 🚀 Setup & Installation

### 1. Clone Repository

```bash
cd /home/kaphie/telecom_pipeline
```

### 2. Create & Activate Python Virtual Environment (Local Dev)

```bash
python3 -m venv myenv
source myenv/bin/activate
pip install -r requirements.txt
```

### 3. Set Up Environment Variables

Copy `.env` template and configure:


## 🐳 Docker Setup

### Containerized Architecture

The pipeline runs in containers to ensure consistency across development, testing, and production:

```
┌─────────────────────────────────────────┐
│      Docker Compose Network             │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │ Airflow Webserver               │  │
│  │ (Port 8080)                      │  │
│  └──────────────────────────────────┘  │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │ Airflow Scheduler                │  │
│  │ Detects & triggers DAGs          │  │
│  └──────────────────────────────────┘  │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │ Airflow Worker (Celery)          │  │
│  │ Executes tasks                   │  │
│  └──────────────────────────────────┘  │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │ PostgreSQL                       │  │
│  │ Airflow metadata database        │  │
│  └──────────────────────────────────┘  │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │ Redis                            │  │
│  │ Celery message broker            │  │
│  └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
         │
         ├──► AWS S3
         ├──► Redshift Serverless
         ├──► Google Sheets
         └──► PostgreSQL (WebForms DB)
```

### Build Docker Image

```bash
docker build -t telecom-airflow:latest .
```

### Start All Services

```bash
docker-compose up -d
```

### View Airflow Web UI

Open browser: `http://localhost:8080`

### Check Container Logs

```bash
docker-compose logs -f airflow-webserver
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker
```

### Stop Services

```bash
docker-compose down
```

### Rebuild & Restart (After Code Changes)

```bash
docker-compose down
docker build -t telecom-airflow:latest .
docker-compose up -d
```

## ⚙️ Configuration

### Airflow Configuration (airflow.cfg)

Key settings for Airflow inside Docker:

```ini
[core]
dags_folder = /opt/airflow/dags
plugins_folder = /opt/airflow/plugins
base_log_folder = /opt/airflow/logs

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres:5432/airflow

[celery]
broker_url = redis://redis:6379/0
result_backend = postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
```

### dbt Configuration (dbt_folder/profile.yml)

```yaml
coretelecom_profile:
  target: dev
  outputs:
    dev:
      type: redshift
      host: [RDST_HOST from .env]
      port: 5439
      user: [RDST_USER]
      password: [RDST_PASSWORD]
      dbname: telecomdb
      schema: staging
      threads: 4
      keepalives_idle: 0
```

### Terraform Configuration

Deploy infrastructure:

```bash
cd terraform_folder
terraform init
terraform plan
terraform apply
```

This creates:
- Redshift Serverless namespace & workgroup
- S3 data lake bucket
- AWS Glue database
- IAM roles and policies

## 📊 Running the Pipeline

### Option 1: Airflow UI (Docker)

1. Open http://localhost:8080
2. Unpause DAG: **dag_extract_raw**
3. Trigger manually or wait for schedule
4. Monitor execution in Airflow UI

### Option 2: CLI Commands

```bash
# Activate Airflow container
docker-compose exec airflow-webserver bash

# Trigger extraction DAG
airflow dags trigger dag_extract_raw

# Trigger load DAG
airflow dags trigger dag_load_raw_to_redshift

# Trigger transformation DAG
airflow dags trigger dag_dbt_transform

# View DAG status
airflow dags list
airflow tasks list dag_extract_raw
```

### Option 3: Local Development (Without Docker)

```bash
source myenv/bin/activate

# Start Airflow standalone
airflow standalone

# In another terminal, trigger DAGs
airflow dags trigger dag_extract_raw
```

## 🔄 Pipeline Workflows

### 1. Extract DAG (`dag_extract_raw`)

Extracts data from multiple sources and stores as Parquet in S3:

| Source | Location | Output |
|--------|----------|--------|
| Customers (CSV) | S3 `customers/` | `s3://.../raw/customers/` |
| Call Logs (CSV) | S3 `call logs/` | `s3://.../raw/callcenter/` |
| Social Media (JSON) | S3 `social_medias/` | `s3://.../raw/socialmedia/` |
| Agents (Google Sheets) | Google Sheets | `s3://.../raw/agents/` |
| Webforms | PostgreSQL | `s3://.../raw/webforms/` |

**Tasks:**
- `ensure_tmp_dir` - Create temporary directory
- `extract_customers` - Extract customer data
- `extract_callcenter` - Extract call logs
- `extract_socialmedia` - Extract social media
- `extract_agents` - Extract from Google Sheets
- `extract_webforms` - Extract from PostgreSQL
- `trigger_load_to_redshift_dag` - Trigger load DAG

### 2. Load DAG (`dag_load_raw_to_redshift`)

Loads Parquet files from S3 into Redshift raw schema:

| Table | Source |
|-------|--------|
| `raw_schema.customers` | `s3://.../raw/customers/` |
| `raw_schema.call_logs` | `s3://.../raw/callcenter/` |
| `raw_schema.social_media` | `s3://.../raw/socialmedia/` |
| `raw_schema.agents` | `s3://.../raw/agents/` |
| `raw_schema.webforms` | `s3://.../raw/webforms/` |

**Tasks:**
- `wait_for_customers_in_s3` - Poll for customer data
- `wait_for_callcenter_in_s3` - Poll for call logs
- `copy_raw_to_redshift` - Execute COPY commands
- `trigger_dbt_dag` - Trigger transformation DAG

**Features:**
- Idempotency: Skips tables with recent data
- Automatic retries on failure
- Cross-account S3 access via IAM credentials

### 3. Transform DAG (`dag_dbt_transform`)

Transforms raw data into staging and mart models:

**dbt Models:**

*Staging Layer:*
- `stg_agent.sql` - Clean agent data
- `stg_customer.sql` - Clean customer data
- `stg_callcenter.sql` - Clean call logs
- `stg_socialmedia.sql` - Clean social media
- `stg_webform.sql` - Clean webforms

*Mart Layer:*
- `dim_agents.sql` - Agent dimension table
- `dim_customer.sql` - Customer dimension table
- `fact_complaint.sql` - Complaint fact table

**Tasks:**
- `dbt_run_staging` - Run staging models
- `dbt_run_marts` - Run mart models
- `dbt_tests` - Run data quality tests
- `dbt_docs_generate` - Generate dbt documentation

## 🐛 Troubleshooting

### Issue: Redshift UnauthorizedException

**Error:**
```
SQL Error [XX000]: Not authorized to get credentials of role 
arn:aws:iam::240448085675:role/redshift-serverless-role
```

**Solution:**
1. Include amazon redshift to the resource and map the role to it

### Issue: S3 Access Denied

**Error:**
```
AccessDenied: User is not authorized to perform S3 actions
```

**Solution:**
1. Verify AWS credentials in `.env`
2. Check IAM user has S3 permissions
3. Verify bucket names match in `.env`
4. Ensure source/destination profiles are configured

### Issue: Redshift Connection Timeout

**Error:**
```
could not translate host name to address: Name or service not known
```

**Solution:**
1. Verify Redshift endpoint in `.env` (RDST_HOST)
2. Check security group allows connection from Airflow
3. Verify network connectivity to Redshift

### Issue: dbt Profile Not Found

**Error:**
```
dbt_folder is not a dbt project
```

**Solution:**
1. Ensure `dbt_project.yaml` exists in `dbt_folder/`
2. Verify `profile.yml` location matches dbt config
3. Check DBT_PROFILES_DIR environment variable points to correct location

### Docker Issues

**Container exits immediately:**
```bash
docker-compose logs airflow-webserver
```

**Permission denied errors:**
```bash
# Rebuild image
docker build --no-cache -t telecom-airflow:latest .
docker-compose up -d
```

**Network issues inside container:**
```bash
# Verify container can reach AWS/external services
docker-compose exec airflow-webserver ping <external-service>
```

