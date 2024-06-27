from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, quarter
from pyspark.sql.types import DateType

silver_bucket = "silver"
gold_bucket = "gold"

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def gold_path_for_table(table):
    return f"s3a://{gold_bucket}/{table}"

def criar_dim_seguradora(spark: SparkSession):
    silver_seguradora = spark.read.format("delta").load(silver_path_for_table("seguradora"))

    silver_seguradora.show(10)

    (
        silver_seguradora
        .write
        .format("delta")
        .mode('overwrite')
        .save(gold_path_for_table("dim_seguradora"))
    )

    