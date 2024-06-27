from pyspark.sql.functions import col, concat_ws, when, current_date, year, regexp_replace, round
from pyspark.sql import SparkSession

def transformar_imovel(spark: SparkSession, bronze_path, silver_path, path_bronze_seguradora, path_bronze_cliente, path_bronze_pessoa):
    print("Iniciando transformacao da tabela imovel")
    df_bronze_imovel = spark.read.format("delta").load(bronze_path)
    df_bronze_seguradora = spark.read.format("delta").load(path_bronze_seguradora)
    df_bronze_cliente = spark.read.format("delta").load(path_bronze_cliente)
    df_bronze_pessoa = spark.read.format("delta").load(path_bronze_pessoa)

    # (Cliente) Renomeia colunas para evitar conflitos no join
    df_bronze_cliente = (
        df_bronze_cliente.join(df_bronze_pessoa, df_bronze_cliente.id_pessoa == df_bronze_pessoa.id_pessoa, how='left')
        .select('id_cliente', 'nome', 'cpf', 'telefone', 'email')
    )
    df_bronze_cliente = df_bronze_cliente.withColumnRenamed('nome', 'nome_cliente') \
                                        .withColumnRenamed('telefone', 'telefone_cliente') \
                                        .withColumnRenamed('email', 'email_cliente')

    df_bronze_seguradora = df_bronze_seguradora.select('id_seguradora', 'nome', 'cnpj', 'razao_social', 'telefone', 'email')
    df_bronze_seguradora = df_bronze_seguradora.withColumnRenamed('nome', 'nome_seguradora') \
                                        .withColumnRenamed('telefone', 'telefone_seguradora') \
                                        .withColumnRenamed('email', 'email_seguradora')

    # Join das tabelas
    df_bronze_imovel = df_bronze_imovel.withColumnRenamed('id_seguradora', 'imovel_id_seguradora')
    df_bronze_imovel = df_bronze_imovel.withColumnRenamed('id_cliente', 'imovel_id_cliente')

    df_silver = df_bronze_imovel.join(df_bronze_cliente, df_bronze_imovel.imovel_id_cliente == df_bronze_cliente.id_cliente, how='left')
    df_silver = df_silver.join(df_bronze_seguradora, df_silver.imovel_id_seguradora == df_bronze_seguradora.id_seguradora, how='left')


    df_silver = df_silver.withColumn('endereco_completo', concat_ws(', ', 'rua', 'numero', 'bairro', 'cidade', 'estado', 'cep'))
    df_silver = df_silver.withColumn('area', round('area', 2))
    df_silver = df_silver.withColumn('valor', round('valor', 2))


    df_silver = df_silver.select(
        'id_imovel', 'tipo', 'area', 'valor', 'descricao', 'endereco_completo',
        'imovel_id_cliente',  'nome_cliente', 'cpf', 'telefone_cliente', 'email_cliente', 
        'imovel_id_seguradora', 'nome_seguradora', 'cnpj', 'razao_social', 'telefone_seguradora', 'email_seguradora'
    )

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela imovel finalizada")