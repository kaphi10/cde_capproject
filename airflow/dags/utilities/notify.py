# dags/helpers/notify.py
import logging

def notify_success(context):
    logging.info("DAG succeeded: %s", context.get("dag").dag_id)

def notify_failure(context):
    # You can extend to send email or slack here using Airflow connections/secrets
    logging.error("DAG failed: %s", context.get("dag").dag_id)
    # Example: send email via EmailOperator or use requests to Slack webhook (if available)
