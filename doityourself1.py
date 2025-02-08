from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as Func

if __name__ == "__main__":
    spark = SparkSession.builder.appName("doityourself1").getOrCreate()
    
    # Obtendo os dados
    clientes = spark.read.parquet("/home/guilherme/download/Atividades/Clientes.parquet")
    vendas = spark.read.parquet("/home/guilherme/download/Atividades/Vendas.parquet")
    vendas = vendas.withColumn("Total", Func.col("Total").cast(DecimalType(10, 2)))

    # Atividade 1 - Selecionar colunas Cliente, Estado e Status:
    clientes.select("Cliente", "Estado", "Status").show() 

    # Atividade 2 - Filtrar clientes com status Platimum e Gold
    clientes.filter((Func.col("Status") == "Platinum") | (Func.col("Status") == "Gold")).show() 

    # Atividade 3 - Totalizar vendas por status
    vendas_clientes = clientes.join(vendas, vendas.ClienteID == clientes.ClienteID)
    vendas_clientes.groupBy("Status").agg(Func.sum("Total").alias("TotalVendas")).show()

    spark.stop

# spark-submit doityoutself1.py