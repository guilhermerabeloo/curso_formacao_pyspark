from pyspark.sql import SparkSession
from pyspark.ml.feature import RFormula
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("machinelearning_classification") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # importando dados
    churn = spark.read.csv("/home/guilherme/download/Churn.csv", inferSchema=True, header=True, sep=";")
    
    # tratando dados
    formula = RFormula(formula="Exited ~ .", featuresCol="features", labelCol="label", handleInvalid="skip")
    churn_transormado = formula.fit(churn).transform(churn).select("features", "label")
    
    # separando dados de treino e teste
    churn_treino, churn_teste = churn_transormado.randomSplit([0.7, 0.3])
    
    # realizando treinamento e previsao com arvore de classificacao
    decisionTree = DecisionTreeClassifier(labelCol="label", featuresCol="features")
    modelo = decisionTree.fit(churn_treino)
    previsao = modelo.transform(churn_teste)
    previsao.show()
    
    # avaliando resultado do modelo
    avaliar = BinaryClassificationEvaluator(rawPredictionCol="prediction", labelCol="label", metricName="areaUnderROC")
    resultado = avaliar.evaluate(previsao)
    print(f'Resultado do modelo: {resultado}')
    
    spark.stop()