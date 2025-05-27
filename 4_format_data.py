# Databricks notebook source
# Set file paths
csv_path = "dbfs:/FileStore/shared_uploads/ganavisuresh8197@gmail.com/data.csv"
txt_path = "dbfs:/FileStore/shared_uploads/ganavisuresh8197@gmail.com/data.txt"
json_path = "dbfs:/FileStore/shared_uploads/ganavisuresh8197@gmail.com/data.json"

# -----------------------------------------
# 1. Read CSV file and save as table
csv_df = spark.read.option("header", True).csv(csv_path)
csv_df.display()
csv_df.write.mode("overwrite").saveAsTable("csv_data_table")

# -----------------------------------------
# 2. Read TXT file (as plain text) and save as table
txt_df = spark.read.text(txt_path)
txt_df.display()
txt_df.write.mode("overwrite").saveAsTable("txt_data_table")

# -----------------------------------------
# 3. Read JSON file and save as table
json_df = spark.read.option("multiline", True).json(json_path)
json_df.display()
json_df.write.mode("overwrite").saveAsTable("json_data_table")


# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/shared_uploads/ganavisuresh8197@gmail.com/"))


# COMMAND ----------

# Step 1: Create sample DataFrames (like your tables)
data = [("Alice", 25), ("Bob", 30), ("Charlie", 28)]
columns = ["name", "age"]

df = spark.createDataFrame(data, columns)

# Save the DataFrame as a table first
df.write.mode("overwrite").saveAsTable("people_table")

# Define output paths on DBFS
csv_output = "dbfs:/FileStore/sample_data/people.csv"
parquet_output = "dbfs:/FileStore/sample_data/people.parquet"
txt_output = "dbfs:/FileStore/sample_data/people.txt"
json_output = "dbfs:/FileStore/sample_data/people.json"

# Step 2: Write DataFrame to different formats

# CSV
df.write.mode("overwrite").option("header", True).csv(csv_output)

# Parquet
df.write.mode("overwrite").parquet(parquet_output)

# TXT (write as text: one column with name and age concatenated)
from pyspark.sql.functions import concat_ws
txt_df = df.select(concat_ws(", ", "name", "age").alias("value"))
txt_df.write.mode("overwrite").text(txt_output)

# JSON
df.write.mode("overwrite").json(json_output)

# Step 3: Read back the files into DataFrames

# Read CSV
csv_df = spark.read.option("header", True).csv(csv_output)
csv_df.show()

# Read Parquet
parquet_df = spark.read.parquet(parquet_output)
parquet_df.show()

# Read TXT
txt_df2 = spark.read.text(txt_output)
txt_df2.show()

# Read JSON
json_df = spark.read.json(json_output)
json_df.show()
