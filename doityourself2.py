from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.types import *


# Atividade 1 - Criar uma database no DW do Spark e criar tabelas externas
spark.sql("show databases").show() 
spark.sql("create database vendasvarejo")
spark.sql("use vendasvarejo").show()

clientes = spark.read.parquet("/home/guilherme/download/Atividades/Clientes.parquet")
produtos = spark.read.parquet("/home/guilherme/download/Atividades/Produtos.parquet")
vendedores = spark.read.parquet("/home/guilherme/download/Atividades/Vendedores.parquet")
itensvendas = spark.read.parquet("/home/guilherme/download/Atividades/ItensVendas.parquet")
vendas = spark.read.parquet("/home/guilherme/download/Atividades/Vendas.parquet")

clientes.write.option("path","../home/guilherme/download/Atividades/Clientes.parquet").saveAsTable("clientes")
produtos.write.option("path","../home/guilherme/download/Atividades/Produtos.parquet").saveAsTable("produtos")
vendedores.write.option("path","../home/guilherme/download/Atividades/Vendedores.parquet").saveAsTable("vendedores")
itensvendas.write.option("path","../home/guilherme/download/Atividades/ItensVendas.parquet").saveAsTable("itensvendas")
vendas.write.option("path","../home/guilherme/download/Atividades/Vendas.parquet").saveAsTable("vendas")

spark.sql("show tables").show()
spark.sql("select * from clientes").show()



# Atividade 2 - Criar relatorio com dados da venda e itens vendidos
spark.sql("use vendasvarejo").show()

spark.sql("""
          SELECT
            CLI.Cliente as NomeCliente,
            VDA.Data as DataVenda,
            PRO.Produto as Produto,
            VEN.Vendedor as Vendedor,
            ITM.TotalComDesconto ValorTotalItem
          FROM VENDAS AS VDA
          INNER JOIN CLIENTES AS CLI ON CLI.ClienteID = VDA.ClienteID
          INNER JOIN VENDEDORES AS VEN ON VEN.VendedorID = VDA.VendedorID
          INNER JOIN ITENSVENDAS AS ITM ON ITM.VendasID = VDA.VendasID
          INNER JOIN PRODUTOS AS PRO ON PRO.ProdutoID = ITM.ProdutoID
""").show()








# exec(open("doityourself2.py").read())