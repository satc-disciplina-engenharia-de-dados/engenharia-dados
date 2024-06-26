from pyspark.sql.functions import col, concat_ws, when, current_date, year, regexp_replace
from pyspark.sql import SparkSession

def transformar_seguradora(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela seguradora")
    # Ler os dados da camada Bronze
    df_bronze = spark.read.format("delta").load(bronze_path)

    df_bronze = df_bronze.withColumn('cnpj', regexp_replace('cnpj', '[^0-9]', ''))
    df_bronze = df_bronze.withColumn('endereco_completo', concat_ws(', ', 'rua', 'numero', 'bairro', 'cidade', 'estado', 'cep'))
    df_silver = df_bronze.select('id_seguradora', 'nome', 'cnpj', 'razao_social', 'telefone', 'email', 'endereco_completo')

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela seguradora finalizada")