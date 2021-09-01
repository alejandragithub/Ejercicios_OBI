// Databricks notebook source
// MAGIC %python
// MAGIC import pandas as pd

// COMMAND ----------

// MAGIC %python
// MAGIC df_spark = spark.read.format('csv').options(sep= ',').options(header= 'true').load('/mnt/raw-data/DIP/GDA/PRUEBA_NATI/all_data.csv').toPandas()

// COMMAND ----------

// MAGIC %python
// MAGIC #Ejercicio 1
// MAGIC #Punto 1
// MAGIC len(df_spark.index)

// COMMAND ----------

// MAGIC %python
// MAGIC #Ejercicio 1
// MAGIC #Punto 2
// MAGIC print(df_spark['categoria'].value_counts())

// COMMAND ----------

// MAGIC %python
// MAGIC #Ejercicio 1
// MAGIC #Punto 3
// MAGIC print(df_spark['cadenaComercial'].value_counts(ascending=False)) 

// COMMAND ----------

// MAGIC %python
// MAGIC #Ejercicio 1
// MAGIC #Punto 4
// MAGIC print(df_spark.groupby(['estado'])[['producto']].max())

// COMMAND ----------

// MAGIC %python
// MAGIC #Ejercicio 1
// MAGIC #Punto 5
// MAGIC dfMax=df_spark.groupby(['cadenaComercial'])['producto'].count().nlargest(1)
// MAGIC print(dfMax)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC #Ejercicio 2
// MAGIC #Punto 1-2
// MAGIC 
// MAGIC import csv
// MAGIC 
// MAGIC with open('/dbfs/mnt/raw-data/DIP/GDA/PRUEBA_NATI/demo.csv', mode='r', encoding='latin-1') as infile:
// MAGIC     reader = csv.reader(infile)
// MAGIC     with open("/dbfs/mnt/raw-data/DIP/GDA/PRUEBA_NATI/demo2.csv", mode='w') as outfile:
// MAGIC         writer = csv.writer(outfile)
// MAGIC         mydict = dict((rows[6],rows[6]) for rows in reader)
// MAGIC         writer.writerow(mydict)

// COMMAND ----------

val dfAeropuerto1= spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("dbfs:/mnt/raw-data/DIP/GDA/PRUEBA_NATI/demo.csv")


// COMMAND ----------

dfAeropuerto1.createOrReplaceTempView("aeropuerto1")

// COMMAND ----------

val dfAeropuerto2= spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("dbfs:/mnt/raw-data/DIP/GDA/PRUEBA_NATI/demo2.csv")
display(dfAeropuerto2)

// COMMAND ----------

dfAeropuerto2.createOrReplaceTempView("aeropuerto2")

// COMMAND ----------

/*Ejercicio 2
#Punto 3*/

sqlContext.sql(
  "select airline_id, name, alias, iata, icao, callsign, ae1.country, active, ae2.country from aeropuerto1 ae1 inner join aeropuerto2 ae2 where ae1.country = ae2.country").show()


