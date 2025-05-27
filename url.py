# Databricks notebook source
import requests
import json
import re
import pandas as pd
from pyspark.sql.functions import col
 
# Step 2: Define API URL and fetch data
url = "https://api.energy-charts.info/total_power"
response = requests.get(url)
 
if response.status_code != 200:
    raise Exception(f"API request failed with status code: {response.status_code}")
 
json_data = response.json()
 
# Step 3: Define function to clean column names
def clean_column_name(name):
    # Replace any character not a-z, A-Z, 0-9 or _ with _
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)
 
# Step 4: Extract data from JSON
unix_seconds = json_data["unix_seconds"]
production_types = json_data["production_types"]
 
# Convert unix timestamps to pandas datetime
timestamps = pd.to_datetime(unix_seconds, unit='s')
 
# Build data dictionary starting with timestamp column
data_dict = {"timestamp": timestamps} 
# Add production type data with cleaned column names
for prod in production_types:
    original_name = prod["name"]
    clean_name = clean_column_name(original_name)
    values = prod["data"]
    data_dict[clean_name] = values
 
# Check lengths match
lengths = [len(v) for v in data_dict.values()]
assert all(length == lengths[0] for length in lengths), "Column length mismatch!"
 
# Step 5: Create pandas DataFrame then convert to Spark DataFrame
pdf = pd.DataFrame(data_dict)
df = spark.createDataFrame(pdf)
 
# Cast timestamp column to Spark timestamp type
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))
 
# Step 6: Preview the data
display(df)
 
# Step 7: Save as Delta table in Databricks
table_name = "energy_total_power_clean"
df.write.format("delta").mode("overwrite").saveAsTable(table_name)
 
print(f"Data successfully saved to Delta table `{table_name}`.")