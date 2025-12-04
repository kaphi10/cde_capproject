# dags/helpers/redshift_utils.py
import os
import psycopg2
from typing import Dict

def get_redshift_conn_info() -> Dict[str, str]:
    """
    Prefer using Airflow connection 'redshift_default' or environment variables.
    If you want Airflow connections, replace usage with BaseHook.get_connection inside DAGs.
    """
    return {
        "host": os.environ.get("RDST_HOST"),
        "port": os.environ.get("RDST_PORT", "5439"),
        "dbname": os.environ.get("RDST_DB"),
        "user": os.environ.get("RDST_USER"),
        "password": os.environ.get("RDST_PASSWORD")
    }

def run_sql_script(sql: str):
    conn_info = get_redshift_conn_info()
    conn = psycopg2.connect(
        host=conn_info["host"],
        port=conn_info["port"],
        dbname=conn_info["dbname"],
        user=conn_info["user"],
        password=conn_info["password"]
    )
    cur = conn.cursor()
    cur.execute(sql)
    try:
        results = cur.fetchall()
    except Exception:
        results = None
    conn.commit()
    cur.close()
    conn.close()
    return results

def table_has_recent_load(table: str, cutoff_ts: str = "1 day"):
    """
    Simple check: look if table has rows loaded in the last 24 hours.
    Adjust cutoff logic to your preferred method.
    """
    # Uses literal interval - works in Redshift
    sql = f"select count(1) from {table} where load_timestamp >= sysdate - interval '{cutoff_ts}'"
    res = run_sql(sql)
    if res and res[0][0] > 0:
        return True
    return False
