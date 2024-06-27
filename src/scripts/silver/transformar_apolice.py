from pyspark.sql.functions import col, concat_ws, when, current_date, year, regexp_replace, round, dayofmonth, month, quarter
from pyspark.sql import SparkSession

def transformar_apolice(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela apolice")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Validar datas e valor de cobertura
    df_bronze = df_bronze.withColumn('data_fim', when(col('data_fim') < col('data_inicio'), None).otherwise(col('data_fim')))
    df_bronze = df_bronze.withColumn('valor_cobertura', when(col('valor_cobertura') < 0, None).otherwise(col('valor_cobertura')))

    df_silver = df_bronze.withColumn('valor_cobertura', round('valor_cobertura', 2))
    df_silver = df_bronze.select('id_apolice', 'id_imovel', 'id_corretor_pessoa', 'id_corretor_seguradora', 'data_inicio', 'data_fim', 'valor_cobertura')

    df_silver = df_bronze.select(
        'id_apolice', 
        'id_imovel', 
        'id_corretor_pessoa',
        'id_corretor_seguradora',
        'data_inicio',
        'data_fim',
        'valor_cobertura',
        dayofmonth(col('data_inicio')).alias('dia_inicio'),
        month(col('data_inicio')).alias('mes_inicio'),
        year(col('data_inicio')).alias('ano_inicio'),
        quarter(col('data_inicio')).alias('trimestre_inicio'),
        dayofmonth(col('data_fim')).alias('dia_fim'),
        month(col('data_fim')).alias('mes_fim'),
        year(col('data_fim')).alias('ano_fim'),
        quarter(col('data_fim')).alias('trimestre_fim')
    )

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela apolice finalizada")
