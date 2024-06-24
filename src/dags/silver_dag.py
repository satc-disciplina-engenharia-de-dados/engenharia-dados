import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.Functions import create_spark_session, create_bucket_if_not_exists, connect_to_minio
from scripts.silver.transformar_pessoa import transformar_pessoa
from scripts.silver.transformar_seguradora import transformar_seguradora

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
    description='Code to move data from Bronze layer to Silver layer',
    schedule_interval=timedelta(days=1),
)

silver_bucket = "silver"
bronze_bucket = "bronze"

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def bronze_path_for_table(table):
    return f"s3a://{bronze_bucket}/{table}"

def process_table(transformation_func, spark, bronze_path, silver_path):
    try:
        transformation_func(spark, bronze_path, silver_path)
    except Exception as e:
        print(f"Erro ao processar a tabela: {e}")
        raise

def process_tables():
    spark = create_spark_session()
    minio = connect_to_minio()
    create_bucket_if_not_exists(minio, silver_bucket)

    tasks = [
        (transformar_pessoa, "pessoa"),
        (transformar_seguradora, "seguradora"),
        # todo: adicionar outras tabelas aqui
    ]

    try:
        for transformation_func, table_name in tasks:
            bronze_path = bronze_path_for_table(table_name)
            silver_path = silver_path_for_table(table_name)
            process_table(transformation_func, spark, bronze_path, silver_path)
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
