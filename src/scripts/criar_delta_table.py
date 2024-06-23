from delta import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# todo: adicionar variáveis de ambiente no container
S3_ACCESS_KEY = "minioadmin" #os.environ.get("S3_ACCESS_KEY")
S3_BUCKET = "teste" # os.environ.get("S3_BUCKET")
S3_SECRET_KEY = "minioadmin" #os.environ.get("S3_SECRET_KEY")
S3_ENDPOINT = "http://minio:9000" #os.environ.get("S3_ENDPOINT")

## Tutorial datawaybr
def criar_delta_table():
    print("criar_delta_table")
    # Create SparkSession 
    spark = ( 
        SparkSession
        .builder
        .master("local[*]")
        # .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .getOrCreate() 
    )

    try:
        data = [
            ("ID001", "CLIENTE_X","SP","ATIVO",   250000.00),
            ("ID002", "CLIENTE_Y","SC","INATIVO", 400000.00),
            ("ID003", "CLIENTE_Z","DF","ATIVO",   1000000.00)
        ]

        schema = (
            StructType([
                StructField("ID_CLIENTE",     StringType(),True),
                StructField("NOME_CLIENTE",   StringType(),True),
                StructField("UF",             StringType(),True),
                StructField("STATUS",         StringType(),True),
                StructField("LIMITE_CREDITO", FloatType(), True)
            ])
        )

        df = spark.createDataFrame(data=data, schema=schema)

        # Salvar os dados no MinIO
        print("Salvando os dados no MinIO")
        save_path = f"s3a://{S3_BUCKET}/"
        (
            df
            .write
            .format("delta")
            .mode('overwrite')
            .save(save_path)
        )

        (spark.read.format("delta").load(save_path).show())
    finally:
        spark.stop()
        print('fechou spark')


