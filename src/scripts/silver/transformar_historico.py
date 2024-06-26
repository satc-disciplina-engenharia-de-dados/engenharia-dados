from pyspark.sql.functions import regexp_replace

def transformar_historico(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela historico")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Limpar descrições
    df_bronze = df_bronze.withColumn('descricao', regexp_replace('descricao', '[^a-zA-Z0-9\s]', ''))

    df_silver = df_bronze.select('id_historico', 'id_imovel', 'data_atualizacao', 'descricao')

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela historico finalizada")
