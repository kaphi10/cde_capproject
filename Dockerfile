FROM apache/airflow:2.10.2-python3.10

# Install OS libraries
USER root
RUN apt-get update \
    && apt-get install -y git build-essential default-libmysqlclient-dev \
    && apt-get clean

# Install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Install dbt-redshift
RUN pip install dbt-redshift

USER airflow

# Copy dbt project
COPY dbt /opt/airflow/dbt

# Set dbt profile path
ENV DBT_PROFILES_DIR="/opt/airflow/dbt"

