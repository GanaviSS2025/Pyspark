# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE demo_schema.people_delta
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM demo_schema.people;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_schema.people_delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL demo_schema.people_delta;
# MAGIC