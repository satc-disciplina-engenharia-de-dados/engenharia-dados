from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, quarter, monotonically_increasing_id,row_number
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

silver_bucket = "silver"
gold_bucket = "gold"

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def gold_path_for_table(table):
    return f"s3a://{gold_bucket}/{table}"

def criar_dim_tempo(spark: SparkSession):
    silver_sinistros = spark.read.format("delta").load(silver_path_for_table("sinistro"))
    df_datas = silver_sinistros.select('data_ocorrencia', 'ano', 'mes', 'dia', 'trimestre').distinct()

    df_dim_tempo = df_datas \
        .withColumn("ano", silver_sinistros.ano) \
        .withColumn("mes", silver_sinistros.mes) \
        .withColumn("dia", silver_sinistros.dia) \
        .withColumn("trimestre", silver_sinistros.trimestre) \
        .withColumn("sk_tempo", monotonically_increasing_id())

    df_dim_tempo = df_dim_tempo.withColumn("sk_tempo", row_number().over(Window.orderBy(monotonically_increasing_id())))    
    df_dim_tempo.show(10)

    (
        df_dim_tempo
        .write
        .format("delta")
        .mode('overwrite')
        .save(gold_path_for_table("dim_tempo"))
    )

    