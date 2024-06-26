from pyspark.sql.functions import col, concat_ws,  when,current_date, year, regexp_replace
from pyspark.sql import SparkSession

def transformar_cliente(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela cliente")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Limpeza de CPF e criação de endereço completo
    df_bronze = df_bronze.withColumn('cpf', regexp_replace('cpf', '[^0-9]', ''))
    df_bronze = df_bronze.withColumn('endereco_completo', concat_ws(', ', 'rua', 'numero', 'bairro', 'cidade', 'estado', 'cep'))

    # Selecionar colunas relevantes
    df_silver = df_bronze.select('id_cliente', 'id_pessoa', 'nome', 'cpf', 'telefone', 'email', 'data_nasc', 'endereco_completo')

    df_silver.show(5)

    # Salvar como tabela Silver
    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela cliente finalizada")
