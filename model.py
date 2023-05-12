from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_auc_score
from sklearn.metrics import accuracy_score
import pandas as pd
import numpy as np

spark = SparkSession.builder.appName("ChurnPrediction").getOrCreate()
df = spark.read.csv("/FileStore/tables/Bank_Customer_Churn_Prediction.csv", header=True, inferSchema=True)
df.show(10)

# Data cleaning
df = df.dropna()
df = df.dropDuplicates()
df = df.withColumn("age", col("age").cast("integer"))

# Feature engineering
df = df.withColumn("avg_balance", (col("balance") / col("products_number")))
df.show(10)

# Exploratory Data Analysis
df.describe().show()

df.groupBy("churn").count().show()
df.select("avg_balance").show()

# Prepare data for model building
assembler = VectorAssembler(inputCols=["avg_balance", "age"], outputCol="features")
data = assembler.transform(df)

# Split data into training and test sets
train, test = data.randomSplit([0.7, 0.3])

# Build the model
lr = LogisticRegression(labelCol="churn")

# Set up the parameter grid for cross-validation
paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .addGrid(lr.elasticNetParam, [0.5, 0.8]) \
    .build()

# Set up the evaluator
evaluator = BinaryClassificationEvaluator(labelCol="churn")

# Set up the cross-validator
cv = CrossValidator(estimator=lr, estimatorParamMaps=paramGrid, evaluator=evaluator)

# Train the model
model = cv.fit(train)

# Make predictions on the test set
predictions = model.transform(test)

# Evaluate the model
print("Accuracy: ", evaluator.evaluate(predictions))

test1['churn'].to_numpy()
array = test1['churn'].to_numpy()
array2 = predictions1['prediction'].to_numpy()
print('accuracy is:', accuracy_score(array, array2))
print('auroc score is:', roc_auc_score(array, array2))


