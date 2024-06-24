from pyspark.sql.functions import col, concat_ws, current_date, year, regexp_replace
from pyspark.sql import SparkSession

def transformar_pessoa(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela pessoa")
    df_bronze = spark.read.format("delta").load(bronze_path)

    df_bronze = df_bronze.withColumn('cpf', regexp_replace('cpf', '[^0-9]', '')) # Remover formatação de CPF
    df_bronze = df_bronze.withColumn('endereco_completo', concat_ws(', ', 'rua', 'numero', 'bairro', 'cidade', 'estado', 'cep')) # Endereço completo
    df_bronze = df_bronze.withColumn('idade', (year(current_date()) - year(col('data_nasc')))) # Calcular idade

    df_silver = df_bronze.select('id_pessoa', 'nome', 'cpf', 'telefone', 'email', 'data_nasc', 'endereco_completo', 'idade')

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela pessoa finalizada")