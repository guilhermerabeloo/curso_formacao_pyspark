from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__": 
    spark = SparkSession.builder \
                .appName("machinelearning_regression") \
                .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    # Importando dados
    carros_temp = spark.read.csv("/home/guilherme/download/Carros.csv", inferSchema=True, header=True, sep=";")  
    
    # Refinando os dados importados
    carros = carros_temp.select("Consumo", "Cilindros", "Cilindradas", "HP")
    
    # Criando um vetor com os dados que serao usados
    caracteristicasvec = VectorAssembler(inputCols=[("Consumo"), ("Cilindros"), ("Cilindradas")], outputCol="caracteristicas")
    carros = caracteristicasvec.transform(carros)
    
    # separando dados de treino e de teste
    carrostreino, carrosteste = carros.randomSplit([0.7, 0.3])
    
    # realizando treinamento e previsao com regressao linear
    regressaoLinear = LinearRegression(featuresCol="caracteristicas", labelCol="HP")
    modelo = regressaoLinear.fit(carrostreino)
    previsao = modelo.transform(carrosteste)
    previsao.show(5)
    
    # avaliando o modelo de Linear Regression
    avaliar = RegressionEvaluator(predictionCol="prediction", labelCol="HP", metricName="rmse")
    rmse = avaliar.evaluate(previsao)
    print(f'Linear Regression: {rmse}')

    # realizando treinamento e previsao com Random Forest Regressor
    rfreg = RandomForestRegressor(featuresCol="caracteristicas", labelCol="HP")
    modelo2 = rfreg.fit(carrostreino)
    previsao2 = modelo2.transform(carrosteste)
    previsao2.show(5)
    
    # avaliando o modelo Random Forest Regressor
    rmse2 = avaliar.evaluate(previsao2)
    print(f'Random Forest Regression: {rmse2}')
    
    spark.stop()