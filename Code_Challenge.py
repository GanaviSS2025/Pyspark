# Databricks notebook source
crude_df = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/FileStore/shared_uploads/ganavisuresh8197@gmail.com/Crude_Oil_data.csv")
gasoline_df = spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/FileStore/shared_uploads/ganavisuresh8197@gmail.com/RBOB_Gasoline_data.csv")


# COMMAND ----------

from pyspark.sql.functions import lit

crude_df = crude_df.withColumn("Ticker", lit("Crude_Oil"))
gasoline_df = gasoline_df.withColumn("Ticker", lit("Gasoline"))



# COMMAND ----------

df_all = crude_df.unionByName(gasoline_df)


# COMMAND ----------

from pyspark.sql.functions import to_timestamp, year, month, dayofmonth

df_all = df_all.withColumn("Date", to_timestamp("Date", "yyyy-MM-dd")) \
               .withColumn("Year", year("Date")) \
               .withColumn("Month", month("Date")) \
               .withColumn("Day", dayofmonth("Date"))


# COMMAND ----------

columns_to_fill = ["Open", "Close", "Volume"]

for col_name in columns_to_fill:
    median_val = df_all.approxQuantile(col_name, [0.5], 0.01)[0]
    df_all = df_all.fillna({col_name: median_val})


# COMMAND ----------

from pyspark.sql.functions import col, round

df_all = df_all.withColumn("Price_Range", round(col("High") - col("Low"), 2)) \
               .withColumn("Daily_Return", round((col("Close") - col("Open")) / col("Open"), 4))


# COMMAND ----------

vol_75 = df_all.approxQuantile("Volume", [0.75], 0.01)[0]
df_all = df_all.filter(col("Volume") > vol_75)


# COMMAND ----------

from pyspark.sql.functions import dayofweek

df_all = df_all.withColumn("dayofweek", dayofweek("Date"))
df_all = df_all.filter((col("dayofweek") >= 2) & (col("dayofweek") <= 6)).drop("dayofweek")

# COMMAND ----------

from pyspark.sql.functions import avg, round

# Ensure Date is timestamp type
df_all = df_all.withColumn("Date", to_timestamp("Date"))

# Pivot table with double values for Close price (rounded to 2 decimals)
pivot_df = df_all.groupBy("Date") \
                 .pivot("Ticker") \
                 .agg(round(avg("Close").cast("double"), 2))

# Verify schema
pivot_df.printSchema()

# Show data
pivot_df.orderBy("Date").show(truncate=False)



# COMMAND ----------

df_all.write.mode("overwrite").format("delta").saveAsTable("fuel_futures_transformed")
pivot_df.write.mode("overwrite").format("delta").saveAsTable("fuel_futures_pivot")



# COMMAND ----------

# Show top 10 rows ordered by Date from "fuel_futures_transformed"
spark.sql("SELECT * FROM fuel_futures_transformed ORDER BY Date LIMIT 10").show(truncate=False)

# Show top 10 rows ordered by Date from "fuel_futures_pivot"
spark.sql("SELECT * FROM fuel_futures_pivot ORDER BY Date LIMIT 10").show(truncate=False)
