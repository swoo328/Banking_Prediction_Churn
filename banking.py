from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Creating the Spark Session
spark = SparkSession.builder.appName("BankingChurn").getOrCreate()
# Reading the file
df = spark.read.csv("/FileStore/tables/Bank_Customer_Churn_Prediction.csv", header=True, inferSchema=True)
# Show top 10 rows
df.show(10)

# check for duplicate data
# removes the rows with null data
df = df.dropna()
df = df.dropDuplicates()

# show churn count
df.groupBy("churn").count().show()

# Create a view or table
temp_table_name = "churn_banking"
df.createOrReplaceTempView(temp_table_name)

%sql
select * from churn_banking

# age interval compare with churn
%sql 
SELECT
  CASE WHEN age>=0 AND age<=10 THEN '0-10'
       WHEN age>=11 AND age<=20 THEN '11-20'
       WHEN age>=21 and age<=30 THEN '21-30'
       WHEN age>=31 and age<=40 THEN '31-40'
       WHEN age>=41 and age<=50 THEN '41-50'
       WHEN age>=51 and age<=60 THEN '51-60'
       WHEN age>=61 and age<=70 THEN '61-70'
       WHEN age>=71 and age<=80 THEN '71-80'
       WHEN age>=81 and age<=90 THEN '81-90'
       WHEN age>=81 and age<=90 THEN '91-100'
       ELSE 'Other' 
       END AS age_range,
       SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) AS Churn1,
       SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) AS Churn0
FROM churn_banking GROUP BY age_range
ORDER BY age_range asc

# balance interval compare with churn
%sql 
SELECT
  CASE WHEN balance>=0 AND balance<=20000 THEN 'A 0-20000'
       WHEN balance>=20001 AND balance<=40000 THEN 'B 20001-40000'
       WHEN balance>=40001 and balance<=60000 THEN 'C 40001-60000'
       WHEN balance>=60001 and balance<=80000 THEN 'D 60001-80000'
       WHEN balance>=80001 and balance<=100000  THEN 'E 80001-100000'
       WHEN balance>=100001 and balance<=120000  THEN 'F 100001-120000'
       WHEN balance>=120001 and balance<=140000 THEN 'G 120001-140000'
       WHEN balance>=140001 and balance<=160000 THEN 'H 140001-160000'
       WHEN balance>=160001 and balance<=180000 THEN 'I 160001-180000'
       WHEN balance>=180001 and balance<=200000 THEN 'J 180001-200000'
       ELSE 'Other' 
       END AS bank_balance,
   SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) AS Churn1,
   SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) AS Churn0
FROM churn_banking GROUP BY bank_balance
ORDER BY bank_balance asc

# estimated salary interval compare with churn
%sql 
SELECT
  CASE WHEN estimated_salary>=0 AND estimated_salary<=20000 THEN 'A 0-20000'
       WHEN estimated_salary>=20001 AND estimated_salary<=40000 THEN 'B 20001-40000'
       WHEN estimated_salary>=40001 and estimated_salary<=60000 THEN 'C 40001-60000'
       WHEN estimated_salary>=60001 and estimated_salary<=80000 THEN 'D 60001-80000'
       WHEN estimated_salary>=80001 and estimated_salary<=100000  THEN 'E 80001-100000'
       WHEN estimated_salary>=100001 and estimated_salary<=120000  THEN 'F 100001-120000'
       WHEN estimated_salary>=120001 and estimated_salary<=140000 THEN 'G 120001-140000'
       WHEN estimated_salary>=140001 and estimated_salary<=160000 THEN 'H 140001-160000'
       WHEN estimated_salary>=160001 and estimated_salary<=180000 THEN 'I 160001-180000'
       WHEN estimated_salary>=180001 and estimated_salary<=200000 THEN 'J 180001-200000'
       ELSE 'Other' 
       END AS salary,
   SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) AS Churn1,
   SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) AS Churn0
FROM churn_banking GROUP BY salary
ORDER BY salary asc

# gender compare with churn 
%sql 
SELECT
   gender,
   SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) AS Churn1,
   SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) AS Churn0
FROM churn_banking 

# active members compare with churn
%sql 
SELECT
   active_member,
   SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) AS Churn1,
   SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) AS Churn0
FROM churn_banking

# Countries compare with churn
%sql 
SELECT
   country,
   SUM(CASE WHEN churn = 1 THEN 1 ELSE 0 END) AS Churn1,
   SUM(CASE WHEN churn = 0 THEN 1 ELSE 0 END) AS Churn0
FROM churn_banking
GROUP BY country
GROUP BY active_member
GROUP BY gender

