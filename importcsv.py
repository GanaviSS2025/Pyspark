# Databricks notebook source
# Load the CSV using Spark
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/FileStore/shared_uploads/ganavisuresh8197@gmail.com/data.csv")

# Show the data
display(df)


# COMMAND ----------

df.printSchema()
