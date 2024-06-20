import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from delta import *

## Tutorial datawaybr
def criar_delta_table():
    # Create SparkSession 
    spark = ( 
        SparkSession
        .builder
        .master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate() 
    )
    data = [
        ("ID001", "CLIENTE_X","SP","ATIVO",   250000.00),
        ("ID002", "CLIENTE_Y","SC","INATIVO", 400000.00),
        ("ID003", "CLIENTE_Z","DF","ATIVO",   1000000.00)
    ]

    schema = (
        StructType([
            StructField("ID_CLIENTE",     StringType(),True),
            StructField("NOME_CLIENTE",   StringType(),True),
            StructField("UF",             StringType(),True),
            StructField("STATUS",         StringType(),True),
            StructField("LIMITE_CREDITO", FloatType(), True)
        ])
    )

    df = spark.createDataFrame(data=data,schema=schema)

    df.show(truncate=False)

    ( 
        df
        .write
        .format("delta")
        .mode('overwrite')
        .save("./RAW/CLIENTES")
    )

    spark.sql("SHOW TABLES IN delta.`./RAW/CLIENTES`").show()


