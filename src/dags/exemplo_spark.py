from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from scripts.criar_delta_table import criar_delta_table

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'exemplo_spark',
    default_args=default_args,
    description='Um exemplo de DAG usando apache spark',
    schedule_interval=None,
)

criar_delta_table = PythonOperator(
    task_id='criar_delta_table',
    python_callable=criar_delta_table,
    dag=dag,
)

# extract_task >> transform_task >> load_task
criar_delta_table