from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Streaming - postgres") \
    .getOrCreate()
    
json_schema = "nome STRING, postagem STRING, data INT"
df = spark.readStream.json("/home/guilherme/testestream", schema=json_schema)

dir_state_app = "/home/guilherme/temp"

def atualizaPostgres(dataframe, batchid):
    dataframe.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/posts") \
        .option("dbtable", "posts") \
        .option("user", "postgres") \
        .option("password", "Saronroses1") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
    .save()

Stcal = df.writeStream.foreachBatch(atualizaPostgres) \
            .outputMode("append") \
            .trigger(processingTime="5 second") \
            .option("checkpointlocation", dir_state_app) \
        .start()

Stcal.awaitTermination()

# teste:
# 1 rodar a aplicacao: spark-submit --jars /home/guilherme/Downloads/postgresql-42.7.3.jar  streaming_postgres.py
# 2 adicionar mais registros na pasta testestream
# 3 verificar a nova quantidade de linhas da tabela sink: select count(*) from posts;