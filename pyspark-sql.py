from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.types import *

# 1. Criando tabela gerenciada
# 1.1 Criando banco de dados que vai receber as tabelas
spark.sql("show databases").show() 
spark.sql("create database desp")
spark.sql("use desp").show()

# 1.2 Criando dataframe com dados que serao usados nas tabelas
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
despachantes = spark.read.csv("/home/guilherme/download/despachantes.csv", header=False, schema=arqschema)

# 1.3 Criando tabela gerenciada e conferindo os dados
despachantes.write.saveAsTable("despachantes")
spark.sql("select * from despachantes").show()
spark.sql("show tables").show()

# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# 2. Tabela externa / Nao gerenciada
# 2.1 Criando novo dataframe e salvando fisicamente em forma de arquivo
despachantes = spark.sql("select * from despachantes")
despachantes.write.format("parquet").save("/home/guilherme/desparquet")

# 2.2 Usando arquivo e metadados criados e salvando em nova tabela
despachantes.write.option("path","../home/guilherme/desparquet").saveAsTable("despachantes_ng")
spark.sql("select * from despachantes_ng").show()
spark.sql("show tables").show()

# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# 3. Verificando a criacao das tabelas para identificar a tabela externa
spark.sql("show create table despachantes").show(truncate=False)
spark.sql("show create table despachantes_ng").show(truncate=False)

# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

# 4. Criando views
# 4.1 View de sessao
despachantes.createOrReplaceTempView("Despachantes_view1")
spark.sql("select * from Despachantes_view1").show()

# 4.2 View global
despachantes.createOrReplaceGlobalTempView("Despachantes_view2")
spark.sql("select * from global_temp.Despachantes_view2").show()
















# exec(open("doityourself2.py").read())