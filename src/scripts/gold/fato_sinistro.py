from pyspark.sql.functions import col, concat_ws,  when,current_date, year, regexp_replace
from pyspark.sql import SparkSession

silver_bucket = "silver"
gold_bucket = "gold"

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def gold_path_for_table(table):
    return f"s3a://{gold_bucket}/{table}"

def criar_fato_sinistro(spark: SparkSession):
    sinistros_silver_path = silver_path_for_table("sinistro")
    dim_tempo_path = gold_path_for_table("dim_tempo")
    dim_cliente_path = gold_path_for_table("dim_cliente")
    dim_corretor_path = gold_path_for_table("dim_corretor")
    dim_imovel_path = gold_path_for_table("dim_imovel")
    dim_seguradora_path = gold_path_for_table("dim_seguradora")
    

    dim_tempo_path = gold_path_for_table("dim_tempo")
    dim_cliente_path = gold_path_for_table("dim_cliente")
    dim_corretor_path = gold_path_for_table("dim_corretor")
    dim_imovel_path = gold_path_for_table("dim_imovel")
    dim_seguradora_path = gold_path_for_table("dim_seguradora")

    dim_tempo = spark.read.format("delta").load(dim_tempo_path)
    dim_cliente = spark.read.format("delta").load(dim_cliente_path)
    dim_corretor = spark.read.format("delta").load(dim_corretor_path)
    dim_imovel = spark.read.format("delta").load(dim_imovel_path)
    dim_seguradora = spark.read.format("delta").load(dim_seguradora_path)

    silver_sinistros = spark.read.format("delta").load(sinistros_silver_path)

    fato_sinistro = silver_sinistros \
        .join(dim_tempo, on=['data_ocorrencia'], how='inner') \
        .join(dim_cliente, on=['id_cliente'], how='inner') \
        .join(dim_imovel, on=['id_imovel'], how='inner') \
        .select(
            'id_sinistro',
            'id_cliente',
            'id_imovel',
            'sk_tempo',
            'valor_prejuizo',
        )

    fato_sinistro.show(5)

    (
        fato_sinistro
        .write
        .format("delta")
        .mode('overwrite')
        .save(gold_path_for_table("fato_sinistro"))
    )

