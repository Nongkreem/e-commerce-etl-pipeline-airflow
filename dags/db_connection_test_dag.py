from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import sys
import os
import logging

log = logging.getLogger(__name__)

# เพิ่มโฟลเดอร์ src/ เข้าไปใน Airflow
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from db_connection_test import test_data_postgres_connection

# --- Email Alert Function ---
def send_failure_email_alert(context):
    """
    Sends an email alert when an Airflow task fails.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    log_url = context['task_instance'].log_url
    execution_date = context['execution_date']

    subject = f"Airflow Task FAILED: {dag_id}.{task_id}"
    html_content = f"""
        <html>
        <body>
            <h3>Pipeline Failed!</h3>
            <p>DAG: <b>{dag_id}</b></p>
            <p>Task: <b>{task_id}</b></p>
            <p>Status: <span style="color:red;">FAILED</span></p>
            <p>Execution Date: {execution_date}</p>
            <p>Please check the logs for more details:</p>
            <p><a href="{log_url}">View Task Logs</a></p>
        </body>
        </html>
    """
    recipient_email = os.environ.get('AIRFLOW_EMAIL_RECIPIENT', 'your_recipient_email@example.com') 
    send_email(to=recipient_email, subject=subject, html_content=html_content)
    log.info(f"Sent email alert for failed task {task_id} in DAG {dag_id} to {recipient_email}.")

# --- Define the DAG ---
with DAG(
    dag_id='db_connection_check',
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(hours=1), # Check every hour
    catchup=False,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': send_failure_email_alert,
    },
    tags=['monitoring', 'db-check'],
) as dag:
    check_db_connection = PythonOperator(
        task_id='check_data_postgres_connection',
        python_callable=test_data_postgres_connection,
    )

