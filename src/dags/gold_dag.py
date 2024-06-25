import os
import sys
from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.json as pajson
from deltalake import write_deltalake
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.Postgres import Postgres
from utils.Functions import list_data, clean_data, aggregate_data  # Importa função para limpeza e agregação de dados

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
    'Gold Script',
    default_args=default_args,
    description='Code to process and move data to Minio in the Gold layer',
    schedule_interval=timedelta(days=1),
)

def connect_to_db() -> Postgres:
    '''Connect to the postgres db'''
    connection = Postgres(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        db=os.getenv('DB_HOST'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    return connection

def list_tables(**kwargs):
    '''List the tables in the database'''
    connection = connect_to_db()
    tables = connection.get_all_tables()
    kwargs['ti'].xcom_push(key='tables', value=tables)

def connect_to_minio() -> Minio:
    '''Connect to the minio server'''
    minio_client = Minio(
        "localhost:9000",
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )
    return minio_client

def insert_data_on_minio(minio_client: Minio, bucket_name: str, object_name: str, file_path: str) -> None:
    '''Insert data into the minio server'''
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        minio_client.fput_object(bucket_name, object_name, file_path)
    except S3Error as e:
        raise e

def verify_folders_or_create(tables):
    '''Verify the folders or create them'''
    for folder in tables:
        if not os.path.exists(f'/tmp/{folder}'):
            os.makedirs(f'/tmp/{folder}')

def convert_json_to_delta(json_file_path, delta_file_path):
    '''Convert JSON file to Delta format'''
    table = pajson.read_json(json_file_path)
    write_deltalake(delta_file_path, table)

def clean_and_process_tables(**kwargs):
    '''Process each table, clean, aggregate, and upload to Minio in the Gold layer'''
    tables = kwargs['ti'].xcom_pull(key='tables', task_ids='list_tables')
    connection = connect_to_db()
    minio_client = connect_to_minio()
    now = datetime.now().strftime('%Y-%m-%d')
    verify_folders_or_create(tables)
    for table in tables:
        data = connection.get_all_data(table)
        df = pd.DataFrame(data)
        cleaned_df = clean_data(df)  # Aplica limpeza na camada "gold"
        aggregated_df = aggregate_data(cleaned_df)  # Aplica agregação na camada "gold"
        csv_file_path = f'/tmp/{table}/{table}_{now}_gold.csv'
        aggregated_df.to_csv(csv_file_path, index=False)
        df = pd.read_csv(csv_file_path, chunksize=2000)
        count = 0
        for chunk in df:
            json_data = {
                'data': chunk.to_json(orient='columns'),
                'date': now
            }
            json_file_path = f'/tmp/{table}/{table}_{now}_{count}.json'
            with open(json_file_path, 'w', encoding='utf-8') as file:
                file.write(json.dumps(json_data))
            delta_file_path = f'/tmp/{table}/{table}_{now}_{count}'
            convert_json_to_delta(json_file_path, delta_file_path)
            count += 1
        data_files = list_data(f'/tmp/{table}')
        for data_file in data_files:
            if not data_file.endswith('.csv') and not data_file.endswith('.json'):
                insert_data_on_minio(minio_client, 'gold', os.path.basename(data_file), data_file)  # Envia para o Minio na camada "gold"

# Define the tasks
list_tables_task = PythonOperator(
    task_id='list_tables',
    python_callable=list_tables,
    provide_context=True,
    dag=dag,
)

clean_and_process_tables_task = PythonOperator(
    task_id='clean_and_process_tables',
    python_callable=clean_and_process_tables,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
list_tables_task >> clean_and_process_tables_task
