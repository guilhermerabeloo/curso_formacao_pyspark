from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("Machine Learning - Pipeline") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

carros = spark.read.csv(
    "/home/guilherme/download/Carros.csv",
    inferSchema=True,
    sep=";",
    header=True
).select("Consumo", "Cilindros", "Cilindradas", "HP")

# Criando vetor com os dados relevantes para usar no algoritmo de Machine Learning
veccaracteristicas = VectorAssembler(
    inputCols=["Consumo", "Cilindros", "Cilindradas"],
    outputCol="Caracteristicas"
)

# Passando a coluna vetorizada e a coluna que deve ser prevista
reglin = LinearRegression(
    featuresCol="Caracteristicas",
    labelCol="HP"
)

# Criando um pipeline para definir os estagios de execucao
pipeline = Pipeline(stages=[veccaracteristicas, reglin])

# treinando o modelo
pipeline_model = pipeline.fit(carros)

# gerando as previsoes com o modelo
previsoes = pipeline_model.transform(carros)

# exibindo as previsoes geradas
previsoes.select("Consumo", "Cilindros", "Cilindradas", "HP", "prediction").show(10)

spark.stop()
