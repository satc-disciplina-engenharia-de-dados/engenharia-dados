def transformar_mobilia(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela mobilia")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Validar valor
    df_bronze = df_bronze.withColumn('valor', when(col('valor') < 0, None).otherwise(col('valor')))

    df_silver = df_bronze.select('id_mobilia', 'id_imovel', 'nome', 'valor')

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela mobilia finalizada")
