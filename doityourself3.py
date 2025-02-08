from pyspark.sql import SparkSession
import sys, getopt

if __name__ == "__main__":
    spark = SparkSession.builder.appName("doityourself3").getOrCreate()
    opts, args = getopt.getopt(sys.argv[1:], "a:t:")

    arquivo, tabela = "", ""
    for opt, arg in opts:
        if opt == "-a":
            arquivo = arg
        elif opt == "-t":
            tabela = arg
            
    df = spark.read.load(arquivo)
    df.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/tabelas").option("dbtable", tabela).option("user", "postgres").option("password", "XXXXXXXXXX").option("driver", "org.postgresql.Driver").save()
    
    spark.stop()   
    
# spark-submit --jars /home/guilherme/Downloads/postgresql-42.7.3.jar doityourself3.py -a /home/guilherme/download/Atividades/Vendas.parquet -t vendas