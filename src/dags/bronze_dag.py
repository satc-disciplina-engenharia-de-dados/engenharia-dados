import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.Postgres import Postgres
from utils.Functions import list_data, create_spark_session, create_bucket_if_not_exists, connect_to_minio

# Load environment variables
load_dotenv()

# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'bronze_script',
    default_args=default_args,
    description='Code to move data from Postgres to Minio in the Bronze layer',
    schedule_interval=timedelta(days=1),
)

def connect_to_db() -> Postgres:
    '''Connect to the postgres db'''
    connection = Postgres(
        user= 'airflow', #os.getenv('DB_USER'),
        password= 'airflow', #os.getenv('DB_PASSWORD'),
        db= 'seguro', # os.getenv('DB_HOST'),
        host= 'postgres', #os.getenv('DB_HOST'),
        port= '5432' #os.getenv('DB_PORT')
    )
    return connection

def list_tables(**kwargs):
    '''List the tables in the database'''
    connection = connect_to_db()
    connection.connect()
    tables = connection.get_all_tables()
    kwargs['ti'].xcom_push(key='tables', value=tables)

def process_tables(**kwargs):
    '''Process each table and upload to Minio'''
    minio_client = connect_to_minio()
    spark = create_spark_session()
    connection = connect_to_db()
    connection.connect()

    bucket = "bronze"
    create_bucket_if_not_exists(minio_client, bucket)

    tables = kwargs['ti'].xcom_pull(key='tables', task_ids='list_tables')
    try:
        for table in tables:
            table_name = table[0]
            print(f"Processing table {table_name}")

            connection.set_table(table_name)
            data = connection.get_all_data()
            columns = connection.get_columns()

            if (len(data) > 0):
                df = spark.createDataFrame(data, columns)
                save_path = f"s3a://{bucket}/{table_name}/"
                (
                    df
                    .write
                    .format("delta")
                    .mode('overwrite')
                    .save(save_path)
                )

    finally:
        spark.stop()
        print('Finalizou processamento das tabelas')

# Define the tasks
list_tables_task = PythonOperator(
    task_id='list_tables',
    python_callable=list_tables,
    provide_context=True,
    dag=dag,
)

process_tables_task = PythonOperator(
    task_id='process_tables',
    python_callable=process_tables,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
list_tables_task >> process_tables_task
