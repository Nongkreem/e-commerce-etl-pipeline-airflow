from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email # Import send_email
from datetime import datetime, timedelta
import sys
import os
import logging

log = logging.getLogger(__name__)

# import scripts from src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Import ETL functions
from extract import extract_sales_data, extract_events_data
from transform import transform_sales_data, transform_events_data, transform_server_log_data
from load import load_data_to_postgres

# Define a directory for mock data
DATA_DIR = '/opt/airflow/data' # Path ใน Airflow Container

# --- Email Alert Function ---
def send_failure_email_alert(context):
    """
    ส่งเมลเเจ้งเตือนไปยัง DE เมื่อ Task ใน Airflow failed
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
    # Replace with the actual recipient email address
    recipient_email = os.environ.get('AIRFLOW_EMAIL_RECIPIENT', 'your_recipient_email@example.com')
    send_email(to=recipient_email, subject=subject, html_content=html_content)
    log.info(f"Sent email alert for failed task {task_id} in DAG {dag_id} to {recipient_email}.")

def send_dev_team_email_alert(context):
    """
    ส่งเมลแจ้งเตือนไปยัง Dev เมื่อพบความผิดปกติใน server log
    """
    dag_id = context['dag'].dag_id
    transform_task_id = 'transform_server_logs'
    log_url = context['ti'].xcom_pull(task_ids=transform_task_id, key='log_url')
    execution_date = context['execution_date']

    transformed_output = context['ti'].xcom_pull(
        task_ids=transform_task_id,
        key='return_value'
    )

    error_count = 0 
    if isinstance(transformed_output, (list, tuple)) and len(transformed_output) == 2:
        error_count = transformed_output[1] 
    
    if error_count == 0:
        log.info("No ERROR logs found in server_logs. No alert sent to Dev Team.")
        return # ไม่ส่งอีเมลถ้าไม่มี ERROR logs

    subject = f"Server Log Anomaly Detected: {dag_id} (Errors: {error_count})"
    html_content = f"""
        <html>
        <body>
            <h3>Server Log Anomaly Detected!</h3>
            <p>DAG: <b>{dag_id}</b></p>
            <p>Task: <b>{transform_task_id}</b></p>
            <p>Anomaly: Found <b>{error_count}</b> 'ERROR' level logs in server_logs data.</p>
            <p>Execution Date: {execution_date}</p>
            <p>Please investigate the server logs and data transformation process for potential issues.</p>
            <p><a href="{log_url}">View Task Logs for Server Logs Transformation</a></p>
        </body>
        </html>
    """
    dev_email_recipient = os.environ.get('AIRFLOW_DEV_EMAIL_RECIPIENT', 'dev.team@yourcompany.com')
    send_email(to=dev_email_recipient, subject=subject, html_content=html_content)
    log.info(f"Sent server log anomaly email alert to {dev_email_recipient}. Found {error_count} ERROR logs.")


# --- Define the DAG ---
with DAG(
    dag_id='e_commerce_etl_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(days=1), # Run daily
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
    tags=['e-commerce', 'etl', 'sales', 'events', 'logs'],
) as dag:
    # --- Sales Data Pipeline ---
    extract_sales = PythonOperator(
        task_id='extract_sales_data',
        python_callable=extract_sales_data,
        op_kwargs={'file_path': f'{DATA_DIR}/sale_data.csv'},
    )

    transform_sales = PythonOperator(
        task_id='transform_sales_data',
        python_callable=transform_sales_data,
        op_kwargs={'df': extract_sales.output},
    )

    load_sales = PythonOperator(
        task_id='load_sales_data',
        python_callable=load_data_to_postgres,
        op_kwargs={'df': transform_sales.output, 'table_name': 'sales_data', 'if_exists': 'replace'},
    )

    # --- Events Data Pipeline ---
    extract_events = PythonOperator(
        task_id='extract_events_data',
        python_callable=extract_events_data,
        op_kwargs={'file_path': f'{DATA_DIR}/events.json'},
    )

    transform_events = PythonOperator(
        task_id='transform_events_data',
        python_callable=transform_events_data,
        op_kwargs={'df': extract_events.output},
    )

    load_events = PythonOperator(
        task_id='load_events_data',
        python_callable=load_data_to_postgres,
        op_kwargs={'df': transform_events.output, 'table_name': 'events_data'},
    )
    
    # --- Server Log Data Pipeline ---
    transform_server_logs = PythonOperator(
        task_id='transform_server_logs',
        python_callable=lambda ti, file_path: transform_server_log_data(file_path),
        op_kwargs={'file_path': f'{DATA_DIR}/server.log'},
    )

    load_server_logs = PythonOperator(
        task_id='load_server_logs_data',
        python_callable=lambda ti: load_data_to_postgres(
            df=ti.xcom_pull(task_ids='transform_server_logs', key='return_value')[0],
            table_name='server_logs',
            if_exists='replace'
        ),
    )

    check_server_log_anomalies_and_alert_dev = PythonOperator(
        task_id='check_server_log_anomalies_and_alert_dev',
        python_callable=lambda **kwargs: send_dev_team_email_alert(context=kwargs), 
    )

    # --- Define Task Dependencies ---
    # Sales pipeline
    extract_sales >> transform_sales >> load_sales
    # Events pipeline
    extract_events >> transform_events >> load_events
    # Server Logs pipeline: Transform -> [Load, Alert]
    transform_server_logs >> [load_server_logs, check_server_log_anomalies_and_alert_dev]

