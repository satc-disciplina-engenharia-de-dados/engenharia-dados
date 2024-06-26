from pyspark.sql.functions import col, concat_ws,  when,current_date, year, regexp_replace
from pyspark.sql import SparkSession

def transformar_sinistro(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela sinistro")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Validar valor de prejuízo
    df_bronze = df_bronze.withColumn('valor_prejuizo', when(col('valor_prejuizo') < 0, None).otherwise(col('valor_prejuizo')))

    # Selecionar colunas relevantes
    df_silver = df_bronze.select('id_sinistro', 'id_imovel', 'data_ocorrencia', 'descricao', 'valor_prejuizo')

    df_silver.show(5)

    # Salvar como tabela Silver
    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela sinistro finalizada")
