from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, quarter
from pyspark.sql.types import DateType

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def gold_path_for_table(table):
    return f"s3a://{gold_bucket}/{table}"

def criar_dim_tempo(spark: SparkSession):
    silver_sinistros = spark.read.format("delta").load(silver_path_for_table("sinistros"))
    df_datas = silver_sinistros.select('data_ocorrencia').distinct()

    df_dim_tempo = df_datas \
        .withColumn("ano", df_sinistros.ano) \
        .withColumn("mes", df_sinistros.mes) \
        .withColumn("dia", df_sinistro.dia) \
        .withColumn("trimestre", df_sinistro.trimestre)

    # Exibir o DataFrame final para verificação
    df_dim_tempo.show(10)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(gold_path_for_table("dim_tempo"))
    )

    