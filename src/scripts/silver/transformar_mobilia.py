def transformar_mobilia(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela mobilia")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Selecionar colunas relevantes e validar valores
    df_silver = df_bronze.select('id_mobilia', 'id_imovel', 'nome', 'valor')

    df_silver.show(5)

    # Salvar como tabela Silver
    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela mobilia finalizada")
