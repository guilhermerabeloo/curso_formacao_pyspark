from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as Func
import sys


if __name__ == "__main__":
    spark = SparkSession.builder.appName("execwithparams").getOrCreate()
    
    # Obtendo os dados
    dadosDoParametro = spark.read.parquet(sys.argv[1])

    dadosDoParametro.show() 

    spark.stop

# spark-submit execwithparams.py /home/guilherme/download/Atividades/Clientes.parquet