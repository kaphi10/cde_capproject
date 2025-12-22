
FROM apache/airflow:3.0.4-python3.12 

# Install only what you absolutely need
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

# Install only essential packages (Airflow already has many)
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    psycopg2-binary \
    boto3 \
    awswrangler==3.8.0 \
    pandas==2.2.1 \
    dbt-core==1.10.15 \
    dbt-redshift==1.9.5 \
    google-api-python-client \
    python-dotenv \
    gspread==6.2.1 \
    oauth2client==4.1.3\
    sqlalchemy==1.4.49

# Copy your code
COPY --chown=airflow:root dbt/telecom_dbt /opt/airflow/dbt/
COPY --chown=airflow:root airflow/dags /opt/airflow/dags/

EXPOSE 8080

CMD ["airflow", "webserver"]