from pyspark.sql import SparkSession
from pyspark.sql import functions as Func

spark = SparkSession.builder \
            .appName("Particionamento e Bucketing") \
            .config("spark.sql.warehouse.dir", "/home/guilherme/pyspark_projects/spark-warehouse") \
            .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE DATABASE IF NOT EXISTS desp")
spark.sql("show databases").show()
spark.sql("use desp").show()

churn = spark.read.csv("/home/guilherme/download/Churn.csv", header=True, inferSchema=True, sep=";")

#Particao
churn.write.partitionBy("Geography").saveAsTable("churn_geo")
spark.sql("select * from churn_geo").show(5)

#Bucket
churn.write.bucketBy(3, "Geography").saveAsTable("churn_geo_bucket")
spark.sql("select * from churn_geo_bucket").show(5)

spark.stop()