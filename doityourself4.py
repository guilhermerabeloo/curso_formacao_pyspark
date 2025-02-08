from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder \
    .appName("Do it yourself - Machine Learning") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

# Importando os dados
iris = spark.read.csv("/home/guilherme/download/iris.csv", inferSchema=True, sep=",", header=True)

# Preparando os dados
indexer = StringIndexer(inputCol="class", outputCol="label")

vec_caracteristicas = VectorAssembler(inputCols=[("sepallength"),("sepalwidth"),("petallength"),("petalwidth")], outputCol="caracteristicas")

iris_treino, iris_teste = iris.randomSplit([0.7, 0.3])

# Treinando o modelo
nb = NaiveBayes(featuresCol="caracteristicas", labelCol="label", modelType="multinomial")

pipeline = Pipeline(stages=[indexer, vec_caracteristicas, nb])

model = pipeline.fit(iris_treino)

# Realizando previsoes
previsao = model.transform(iris_teste)

acuracidade = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol="label").evaluate(previsao)

# Mostrando as previsoes e acuracidade
previsao.show()
print("Acuracidade ", acuracidade)







spark.stop()
    