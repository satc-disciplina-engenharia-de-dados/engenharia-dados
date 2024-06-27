from pyspark.sql.functions import col, concat_ws, regexp_replace
from pyspark.sql import SparkSession

def transformar_corretor(spark: SparkSession, bronze_path, silver_path, path_bronze_pessoa):
    print("Iniciando transformacao da tabela corretor")
    df_bronze_corretor = spark.read.format("delta").load(bronze_path)
    df_bronze_pessoa = spark.read.format("delta").load(path_bronze_pessoa)
     
    var = df_bronze_corretor.withColumnRenamed('id_pessoa', 'corretor_id_pessoa')

    print(var)
    # Renomeia colunas para evitar conflitos no join
    df_bronze_corretor = (
        df_bronze_corretor.join(df_bronze_pessoa, var.corretor_id_pessoa == df_bronze_pessoa.id_pessoa, how='left')
        .select('id_cliente', 'nome', 'cpf', 'email', 'telefone')
    )
    df_bronze_corretor = df_bronze_corretor.withColumnRenamed('nome', 'nome_pessoa_corretor') \
                                           .withColumnRenamed('telefone', 'telefone_pessoa_corretor') \
                                           .withColumnRenamed('email', 'email_pessoa_corretor') 
                                                                                    

    df_silver = df_bronze_corretor

    df_silver.show(5)
 

    (
        df_silver
        .write
        .format("delta")
        .mode('overwrite')
        .save(silver_path)
    )
    
    print("Transformacao da tabela corretor finalizada")
