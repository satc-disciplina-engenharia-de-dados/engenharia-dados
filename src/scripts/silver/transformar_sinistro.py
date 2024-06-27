from pyspark.sql.functions import col, concat_ws, when, current_date, year, regexp_replace, dayofmonth, month, quarter, round
from pyspark.sql import SparkSession

def transformar_sinistro(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela sinistro")
    df_bronze = spark.read.format("delta").load(bronze_path).alias('sinistro')
    bronze_imovel = spark.read.format("delta").load(f"s3a://bronze/imovel").alias('imovel')

    df_bronze = df_bronze.join(bronze_imovel, col('sinistro.id_imovel') == col('imovel.id_imovel'), how='left')
    df_bronze = df_bronze.withColumn('sinistro.valor_prejuizo', when(col('sinistro.valor_prejuizo') < 0, None).otherwise(col('sinistro.valor_prejuizo')))
    df_silver = df_bronze.withColumn('sinistro.valor_prejuizo', round('sinistro.valor_prejuizo', 2))

    df_silver = df_bronze.select(
        'id_sinistro', 
        'sinistro.id_imovel', 
        'imovel.id_cliente', 
        'sinistro.data_ocorrencia', 
        'sinistro.descricao', 
        'sinistro.valor_prejuizo',
        dayofmonth(col('sinistro.data_ocorrencia')).alias('dia'),
        month(col('sinistro.data_ocorrencia')).alias('mes'),
        year(col('sinistro.data_ocorrencia')).alias('ano'),
        quarter(col('sinistro.data_ocorrencia')).alias('trimestre')
    )

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela sinistro finalizada")
