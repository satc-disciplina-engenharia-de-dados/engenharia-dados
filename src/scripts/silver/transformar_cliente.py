from pyspark.sql.functions import col, concat_ws, when, current_date, year, regexp_replace, round
from pyspark.sql import SparkSession

def transformar_cliente(spark: SparkSession, bronze_path, silver_path, path_bronze_pessoa):
    print("Iniciando transformacao da tabela cliente")
    df_bronze_cliente = spark.read.format("delta").load(bronze_path)
    df_bronze_pessoa = spark.read.format("delta").load(path_bronze_pessoa)

    # Renomeia colunas para evitar conflitos no join
    df_bronze_cliente = (
        df_bronze_cliente.join(df_bronze_pessoa, df_bronze_cliente.id_pessoa == df_bronze_pessoa.id_pessoa, how='left')
        .select('id_cliente', 'nome', 'cpf', 'email', 'telefone')
    )
    df_bronze_cliente = df_bronze_cliente.withColumnRenamed('nome', 'nome_pessoa_cliente') \
                                        .withColumnRenamed('telefone', 'telefone_pessoa_cliente')\
                                        .withColumnRenamed('cpf', 'cpf_pessoa_cliente')\
                                        .withColumnRenamed('email', 'email_pessoa_cliente')

    df_silver = df_bronze_cliente

    df_silver.show(5)


    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela cliente finalizada")
