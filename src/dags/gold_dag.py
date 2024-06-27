import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.Functions import create_spark_session, create_bucket_if_not_exists, connect_to_minio

from scripts.gold.dim_cliente import criar_dim_cliente
from scripts.gold.dim_corretor import criar_dim_corretor
from scripts.gold.dim_imovel import criar_dim_imovel
from scripts.gold.dim_seguradora import criar_dim_seguradora
from scripts.gold.fato_sinistro import criar_fato_sinistro

# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
}

# Initialize the DAG
dag = DAG(
    'silver_script',
    default_args=default_args,
    description='Code to move data from Silver layer to Gold layer',
    schedule_interval=timedelta(days=1),
)

silver_bucket = "silver"
gold_bucket = "gold"

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def gold_path_for_table(table):
    return f"s3a://{gold_bucket}/{table}"

def process_tables():
    spark = create_spark_session()
    minio = connect_to_minio()
    create_bucket_if_not_exists(minio, gold_bucket)

    try:
        print('------ iniciando dim_cliente ---------')
        criar_dim_cliente(spark)
        print('------ finalizando dim_cliente -------')
        
        print('------ iniciando dim_imovel ---------')
        criar_dim_imovel(spark)
        print('------ finalizando dim_imovel -------')
        
        print('------ iniciando dim_seguradora ---------')
        criar_dim_seguradora(spark)
        print('------ finalizando dim_seguradora -------')

        print('------ iniciando dim_corretor ---------')
        criar_dim_corretor(spark)
        print('------ finalizando dim_corretor -------')


        print('------ iniciando fato_sinistro ---------')
        criar_fato_sinistro(spark)
        print('------ finalizando fato_sinistro -------')

    finally:
        spark.stop()

# Define the tasks
process_tables_task = PythonOperator(
    task_id='process_tables',
    python_callable=process_tables,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
process_tables_task
