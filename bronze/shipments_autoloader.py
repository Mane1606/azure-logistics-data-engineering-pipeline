# Databricks notebook source
dbutils.widgets.text("src","")


# COMMAND ----------

src_value = dbutils.widgets.get("src")
src_value

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

input_path = f"/Volumes/logistics_catalog/bronze/raw_volume/{src_value}"
checkpoint = f"/Volumes/logistics_catalog/bronze/raw_volume/_checkpoints/{src_value}"
schema_path = f"/Volumes/logistics_catalog/bronze/raw_volume/_schemas/{src_value}"
table_name = f"logistics_catalog.bronze.{src_value}_bronze"

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(input_path)
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))  
)

(
    df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)   
    .toTable(table_name)
)


