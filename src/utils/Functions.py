import os
from pathlib import Path
from glob import iglob
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error

def list_data(path):
    '''List all unprocessed data files in the path

    Parameters
    ----------
    path : str
        Path to the unprocessed data
    Returns
    -------
    list
        List of unprocessed data files
    '''
    data_path = Path(path)
    files = [x for x in iglob(str(data_path / "*"))] if data_path.exists() else []
    if len(files) == 0:
        raise FileNotFoundError(f"No files found in {path}")
    files.sort()
    data = [fpath for fpath in files]
    data.sort()
    return data

def create_spark_session(): 
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
    S3_ENDPOINT = os.environ.get("S3_ENDPOINT_URL")
    
    return ( 
        SparkSession
        .builder
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-core_2.12:2.4.0")
        .config("driver", "org.postgresql.Driver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://" + S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .getOrCreate() 
    )

def create_bucket_if_not_exists(minio_client: Minio, bucket_name: str) -> None:
    '''Insert data into the minio server'''
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
    except S3Error as e:
        raise e

def connect_to_minio() -> Minio:
    '''Connect to the minio server'''
    minio_client = Minio(
        os.environ.get("S3_ENDPOINT_URL"),
        access_key=os.environ.get("S3_ACCESS_KEY"),
        secret_key=os.environ.get("S3_SECRET_KEY"),
        secure=False
    )
    return minio_client
