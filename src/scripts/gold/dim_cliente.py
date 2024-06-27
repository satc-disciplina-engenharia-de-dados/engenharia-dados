from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, quarter
from pyspark.sql.types import DateType

silver_bucket = "silver"
gold_bucket = "gold"

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def gold_path_for_table(table):
    return f"s3a://{gold_bucket}/{table}"

def criar_dim_cliente(spark: SparkSession):
    silver_cliente = spark.read.format("delta").load(silver_path_for_table("cliente"))

    (
        silver_cliente
        .write
        .format("delta")
        .mode('overwrite')
        .save(gold_path_for_table("dim_cliente"))
    )

    