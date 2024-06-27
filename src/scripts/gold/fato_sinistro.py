from pyspark.sql.functions import col, concat_ws,  when,current_date, year, regexp_replace
from pyspark.sql import SparkSession

def silver_path_for_table(table): 
    return f"s3a://{silver_bucket}/{table}"

def gold_path_for_table(table):
    return f"s3a://{gold_bucket}/{table}"

def criar_fato_sinistro(spark: SparkSession):
    sinistros_silver_path = silver_path_for_table("sinistros")

    dim_tempo_path = gold_path_for_table("dim_tempo")
    dim_cliente_path = gold_path_for_table("dim_cliente")
    dim_corretor_path = gold_path_for_table("dim_corretor")
    dim_imovel_path = gold_path_for_table("dim_imovel")
    dim_seguradora_path = gold_path_for_table("dim_seguradora")

    dim_tempo = spark.read.format("delta").load(dim_tempo_path)
    dim_cliente = spark.read.format("delta").load(dim_cliente_path)
    dim_corretor = spark.read.format("delta").load(dim_corretor_path)
    dim_imovel = spark.read.format("delta").load(dim_imovel_path)
    dim_seuguradora = spark.read.format("delta").load(dim_seuguradora_path)

    silver_sinistros = spark.read.format("delta").load(sinistros_silver_path)

    fato_sinistro = silver_sinistros \
        .join(dim_tempo, on=['data_ocorrencia'], how='inner') \
        .join(dim_cliente, on=['id_cliente'], how='inner') \
        .join(dim_corretor, on=['id_corretor_pessoa', 'id_corretor_seguradora'], how='inner') \
        .join(dim_imovel, on=['id_imovel'], how='inner') \
        .join(dim_seguradora, on=['id_seguradora'], how='inner') \
        .select(
            'id_sinistro',
            dim_tempo['ano'].alias('ano_ocorrencia'),
            dim_tempo['mes'].alias('mes_ocorrencia'),
            dim_tempo['dia'].alias('dia_ocorrencia'),
            dim_tempo['trimestre'].alias('trimestre_ocorrencia'),
            'descricao',
            'valor_prejuizo',
            'id_cliente',
            'id_corretor_pessoa',
            'id_corretor_seguradora',
            'id_imovel',
            'id_seguradora'
        )

    # Juntar fato sinistro com dimensão tempo para obter as chaves de tempo
    df_fato_sinistro = df_fato_sinistro \
        .join(df_dim_tempo, on=['data_ocorrencia'], how='inner') \
        .select(
            'id_sinistro',
            'id_imovel',
            'data_ocorrencia',
            'descricao',
            'valor_prejuizo',
            'ano',
            'mes',
            'dia',
            'trimestre'
        )