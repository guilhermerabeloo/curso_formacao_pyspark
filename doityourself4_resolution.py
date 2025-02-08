from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import NaiveBayes

spark = SparkSession.builder \
    .appName("Do it yourself (Resolution)- Machine Learning") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("ERROR")

# Importando os dados
iris = spark.read.csv("/home/guilherme/download/iris.csv", inferSchema=True, sep=",", header=True)

# Preparando os dados
formula = RFormula(formula="class ~ .", featuresCol="features", labelCol="label", handleInvalid="skip")

iris_transform = formula.fit(iris).transform(iris).select("features", "label")

iris_treino, iris_teste = iris_transform.randomSplit([0.7, 0.3])

# Treinando o modelo
nb = NaiveBayes(featuresCol="features", labelCol="label",)

model = nb.fit(iris_treino)

# Realizando previsoes
previsao = model.transform(iris_teste)

acuracidade = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="label", metricName="accuracy").evaluate(previsao)

# Mostrando as previsoes e acuracidade
previsao.show()
print("Acuracidade ", acuracidade)


spark.stop()
    