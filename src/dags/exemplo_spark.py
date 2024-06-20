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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'exemplo_spark',
    default_args=default_args,
    description='Um exemplo de DAG usando apache spark',
    schedule_interval=timedelta(days=1),
)

criar_delta_table = PythonOperator(
    task_id='criar_delta_table',
    python_callable=criar_delta_table,
    dag=dag,
)

# transform_task = PythonOperator(
#     task_id='transform_task',
#     python_callable=transform_data,
#     dag=dag,
# )

# load_task = PythonOperator(
#     task_id='load_task',
#     python_callable=load_data,
#     dag=dag,
# )

# extract_task >> transform_task >> load_task
criar_delta_table