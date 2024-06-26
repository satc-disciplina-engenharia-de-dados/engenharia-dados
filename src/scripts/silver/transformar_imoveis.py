from pyspark.sql.functions import when

def transformar_imovel(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela imovel")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Remover formatação e criar endereço completo
    df_bronze = df_bronze.withColumn('endereco_completo', concat_ws(', ', 'rua', 'numero', 'bairro', 'cidade', 'estado', 'cep'))

    # Validar área e valor
    df_bronze = df_bronze.withColumn('area', when(col('area') < 0, None).otherwise(col('area')))
    df_bronze = df_bronze.withColumn('valor', when(col('valor') < 0, None).otherwise(col('valor')))

    df_silver = df_bronze.select('id_imovel', 'tipo', 'area', 'valor', 'descricao', 'id_seguradora', 'id_cliente', 'endereco_completo')

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela imovel finalizada")
