import os
import sys
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.Postgres import Postgres
from utils.Functions import list_data, clean_data  # Importa função para limpeza de dados

load_dotenv()

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

def list_tables(conn: Postgres) -> list:
    '''List the tables in the database'''
    return conn.get_all_tables()

def connect_to_minio() -> Minio:
    '''Connect to the minio server'''
    minioClient = Minio(
        "localhost:9000",
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )
    return minioClient

def insert_data_on_minio(minio_client: Minio, bucket_name: str, object_name: str, file_path: str) -> None:
    '''Insert data into the minio server'''
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        minio_client.fput_object(bucket_name, object_name, file_path)
    except S3Error as e:
        raise e

def main():
    '''Main function'''
    connection = connect_to_db()  # Conecta ao banco de dados
    tables = list_tables(connection)  # Lista todas as tabelas do banco de dados
    minio_client = connect_to_minio()  # Conecta ao minio
    now = datetime.now().strftime('%Y-%m-%d')  # Pega a data atual para utilizar como metadado e sufixo do arquivo
    for table in tables:  # Para cada tabela no banco de dados
        data = connection.get_all_data(table)  # Pega todos os dados da tabela
        df = pd.DataFrame(data)  # Cria um dataframe com os dados
        df_cleaned = clean_data(df)  # Limpa e transforma os dados na camada "silver"
        df_cleaned.to_csv(f'./data/{table}/{table}_{now}_silver.csv', index=False)  # Salva os dados limpos
        insert_data_on_minio(minio_client, 'silver', f'{table}_{now}_silver.csv', f'./data/{table}/{table}_{now}_silver.csv')  # Insere os dados limpos no Minio

if __name__ == '__main__':
    main()
