# # dags/dag_dbt_transform.py

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from pendulum import datetime
from telecom.utilities.notify import notify_failure, notify_success

# -------------------------------------------------------------------
# ABSOLUTE PATHS (NO ~)
# -------------------------------------------------------------------
load_dotenv()
# dbt executable inside Airflow virtualenv
DBT_BIN = "/home/kaphie/telecom_pipeline/myenv/bin/dbt"

# dbt project root
DBT_PROJECT_DIR = "/home/kaphie/telecom_pipeline/dbt/telecom_dbt"

# location of profiles.yml
DBT_PROFILES_DIR = "/home/kaphie/.dbt"

# -------------------------------------------------------------------
# DEFAULT ARGS
# -------------------------------------------------------------------

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "on_success_callback": notify_success,
}

COMMON_ENV = {
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "RDST_HOST": os.getenv("RDST_HOST"),
    "RDST_USER": os.getenv("RDST_USER"),
    "RDST_PASSWORD": os.getenv("RDST_PASSWORD"),
    "RDST_DB": os.getenv("RDST_DB"),
}

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------

with DAG(
    dag_id="dag_dbt_transform",
    start_date=datetime(2025, 11, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "redshift", "transform"],
) as dag:

    # ---------------------------------------------------------------
    # 1. RUN STAGING MODELS
    # ---------------------------------------------------------------

    dbt_staging = BashOperator(
    task_id="dbt_run_staging",
    bash_command=f"""
        set -e
        cd {DBT_PROJECT_DIR}
        {DBT_BIN} deps --profiles-dir {DBT_PROFILES_DIR}
        {DBT_BIN} run --select staging+ --profiles-dir {DBT_PROFILES_DIR}
    """,
    env=COMMON_ENV,
)

    # ---------------------------------------------------------------
    # 2. RUN MARTS (DIMENSIONS + FACTS)
    # ---------------------------------------------------------------

    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"""
            set -e
            cd {DBT_PROJECT_DIR}
            {DBT_BIN} run --models marts+
        """,
       
        env=COMMON_ENV
        
    )

    # ---------------------------------------------------------------
    # 3. RUN TESTS
    # ---------------------------------------------------------------

    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=f"""
            set -e
            cd {DBT_PROJECT_DIR}
            {DBT_BIN} test
        """,
        env=COMMON_ENV
    )
    # ---------------------------------------------------------------
    # 4. GENERATE DOCS
    # ---------------------------------------------------------------

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
            set -e
            cd {DBT_PROJECT_DIR}
            {DBT_BIN} docs generate
        """,
        env=COMMON_ENV
    )

    # ---------------------------------------------------------------
    # TASK DEPENDENCIES
    # ---------------------------------------------------------------

    dbt_staging >> dbt_marts >> dbt_tests >> dbt_docs

