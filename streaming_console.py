from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Streaming - console").getOrCreate()

json_schema = "nome STRING, postagem STRING, data INT"

df = spark.readStream.json("/home/guilherme/testestream", schema=json_schema)

dir_state_app = "/home/guilherme/temp"

stcal = df.writeStream.format("console").outputMode("append").trigger(processingTime="5 second").option("checkpointlocation", dir_state_app).start()

stcal.awaitTermination()
