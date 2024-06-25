import os
import sys
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.Postgres import Postgres
from utils.Functions import list_data, clean_data, aggregate_data  # Importa função para limpeza e agregação de dados

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
    minio_client = connect_to_minio()  # Conecta ao Minio
    now = datetime.now().strftime('%Y-%m-%d')  # Obtém a data atual
    for table in tables:  # Para cada tabela no banco de dados
        data = connection.get_all_data(table)  # Obtém todos os dados da tabela
        df = pd.DataFrame(data)  # Cria um DataFrame com os dados
        cleaned_df = clean_data(df)  # Aplica limpeza na camada "gold"
        aggregated_df = aggregate_data(cleaned_df)  # Aplica agregação na camada "gold"
        csv_file_path = f'./data/{table}/{table}_{now}_gold.csv'  # Caminho para salvar o CSV
        aggregated_df.to_csv(csv_file_path, index=False)  # Salva os dados limpos e agregados em CSV
        df = pd.read_csv(csv_file_path, chunksize=2000)  # Lê o CSV em chunks
        count = 0  # Contador para o sufixo do arquivo JSON
        for chunk in df:  # Para cada chunk do DataFrame
            json_data = {  # Cria um dicionário com os dados e a data
                'data': chunk.to_json(orient='columns'),
                'date': now
            }
            json_file_path = f'./data/{table}/{table}_{now}_{count}.json'  # Caminho para salvar o JSON
            with open(json_file_path, 'w', encoding='utf-8') as file:  # Abre o arquivo JSON
                file.write(json.dumps(json_data))  # Escreve os dados no arquivo JSON
            delta_file_path = f'./data/{table}/{table}_{now}_{count}'  # Caminho para salvar o Delta Lake
            # Converte o JSON para Delta Lake
            table_json = pd.read_json(json_file_path)
            table_pq = pa.Table.from_pandas(table_json)
            pa.parquet.write_table(table_pq, delta_file_path)

            count += 1  # Incrementa o contador

        data_files = list_data(f'./data/{table}')  # Lista os arquivos no diretório da tabela
        for data_file in data_files:  # Para cada arquivo de dados
            if not data_file.endswith('.csv') and not data_file.endswith('.json'):
                insert_data_on_minio(minio_client, 'gold', os.path.basename(data_file), data_file)  # Insere no Minio na camada "gold"

if __name__ == '__main__':
    main()

