from pyspark.sql.functions import col, concat_ws,  when,current_date, year, regexp_replace
from pyspark.sql import SparkSession

def transformar_corretor(spark: SparkSession, bronze_path, silver_path, bronze_path_pessoa):
    print("Iniciando transformacao da tabela corretor")
    df_bronze = spark.read.format("delta").load(bronze_path)
    df_bronze = df_bronze.withColumnRenamed("id_pessoa", "corretor_id_pessoa")
    bronze_pessoa = spark.read.format("delta").load(bronze_path_pessoa)

    df_bronze = (
        df_bronze.join(bronze_pessoa, df_bronze.corretor_id_pessoa == bronze_pessoa.id_pessoa, how='left')
        # .select('id_corretor', 'nome', 'cpf', 'telefone', 'email', 'rua', 'numero', 'bairro', 'cidade', 'estado', 'cep', 'data_nasc')
    )

    df_bronze = df_bronze.withColumn('cpf', regexp_replace('cpf', '[^0-9]', ''))
    df_bronze = df_bronze.withColumn('endereco_completo', concat_ws(', ', 'rua', 'numero', 'bairro', 'cidade', 'estado', 'cep'))
    df_bronze = df_bronze.withColumn('idade', (year(current_date()) - year(col('data_nasc'))))

    df_silver = df_bronze.select('id_seguradora', 'id_pessoa', 'nome', 'cpf', 'telefone', 'email', 'data_nasc', 'idade', 'endereco_completo')

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela corretor finalizada")
