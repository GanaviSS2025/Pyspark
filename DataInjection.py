# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS demo_schema;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS demo_schema.people (
# MAGIC   name STRING,
# MAGIC   age INT,
# MAGIC   city STRING
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO demo_schema.people VALUES
# MAGIC ('Alice', 30, 'New York'),
# MAGIC ('Bob', 25, 'Los Angeles'),
# MAGIC ('Charlie', 35, 'Chicago');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_schema.people;
# MAGIC

# COMMAND ----------

df = spark.table("demo_schema.people")
df.show()
