def transformar_vistoria(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela vistoria")
    df_bronze = spark.read.format("delta").load(bronze_path)

    df_silver = df_bronze.select('id_vistoria', 'id_imovel', 'data_vistoria', 'descricao')

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela vistoria finalizada")
