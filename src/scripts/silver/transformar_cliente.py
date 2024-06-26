from pyspark.sql.functions import col, concat_ws, current_date, year, regexp_replace
from pyspark.sql import SparkSession

def transformar_cliente(spark: SparkSession, bronze_path, silver_path):
    print("Iniciando transformacao da tabela cliente")
    df_bronze = spark.read.format("delta").load(bronze_path)

    df_silver = df_bronze.select('id_cliente', 'id_pessoa')

    df_silver.show(5)

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    print("Transformacao da tabela cliente finalizada")