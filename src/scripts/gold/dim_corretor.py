from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, quarter
from pyspark.sql.types import DateType

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def gold_path_for_table(table):
    return f"s3a://{gold_bucket}/{table}"

def criar_dim_corretor(spark: SparkSession):
    silver_corretor = spark.read.format("delta").load(silver_path_for_table("corretores"))

    (
        silver_corretor
        .write
        .format("delta")
        .mode('overwrite')
        .save(gold_path_for_table("dim_corretor"))
    )

    