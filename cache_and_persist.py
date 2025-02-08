from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark import StorageLevel

spark = SparkSession.builder \
            .appName("Particionamento e Bucketing") \
            .config("spark.sql.warehouse.dir", "/home/guilherme/pyspark_projects/spark-warehouse") \
            .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS desp")
spark.sql("show databases").show()
spark.sql("use desp").show()

arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("/home/guilherme/download/despachantes.csv", header=False, schema=arqschema)

despachantes.write.saveAsTable("despachantes_cache")

df = spark.sql("select * from despachantes_cache")

stgLevel = df.storageLevel
print(stgLevel) #Serialized 1x Replicated

df.cache()

stgLevel = df.storageLevel
print(stgLevel) #Disk Memory Deserialized 1x Replicated

df.unpersist()

stgLevel = df.storageLevel
print(stgLevel) #Serialized 1x Replicated

df.persist(StorageLevel.DISK_ONLY)

stgLevel = df.storageLevel
print(stgLevel) #Disk Serialized 1x Replicated