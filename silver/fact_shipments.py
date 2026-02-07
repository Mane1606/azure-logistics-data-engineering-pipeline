# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

bronze_df = spark.read.table('logistics_catalog.bronze.shipments_bronze')

display(bronze_df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

bronze_table = "logistics_catalog.bronze.shipments_bronze"
silver_table = "logistics_catalog.silver.shipments_silver"
dup_audit_table = "logistics_catalog.silver.shipments_duplicates_audit"


# 1. Read Bronze

bronze_df = spark.read.table(bronze_table)


# 2. Cleaning


clean_df = (
    bronze_df
    .withColumn("shipment_id", upper(trim(col("shipment_id")))) \
    .withColumn("customer_id", upper(trim(col("customer_id")))) \
    .withColumn("product_id", upper(trim(col("product_id")))) \
    .withColumn("carrier_id", upper(trim(col("carrier_id")))) \
    .withColumn("origin_port", trim(col("origin_port"))) \
    .withColumn("destination_port", trim(col("destination_port"))) \
    .withColumn("shipment_mode", upper(trim(col("shipment_mode")))) \
    .withColumn("shipment_status", upper(trim(col("shipment_status"))))
    .withColumn(
    "shipment_date",
    coalesce(
        expr("try_to_timestamp(trim(shipment_date), 'yyyy-MM-dd')"),
        expr("try_to_timestamp(trim(shipment_date), 'dd/MM/yyyy')"),
        expr("try_to_timestamp(trim(shipment_date), 'd-MMM-yyyy')"),
        expr("try_to_timestamp(trim(shipment_date), 'MM-dd-yyyy')"),
        expr("try_to_timestamp(trim(shipment_date), 'dd-MM-yyyy')"),
        expr("try_to_timestamp(trim(shipment_date), 'yyyy/MM/dd')"),
        expr("try_to_timestamp(trim(shipment_date), 'yyyyMMdd')")
    )
)

    .withColumn('freight_cost_usd', col('freight_cost_usd').cast('double'))
    .withColumn('ingest_time', current_timestamp())

    .drop('_rescued_data')
    .withColumn(
    "freight_cost_usd",
    when(col("freight_cost_usd") <= 0, None)
    .otherwise(col("freight_cost_usd"))
)
    .withColumn(
    "shipment_mode",
    when(col("shipment_mode").isin("SEA","AIR"), col("shipment_mode"))
    .otherwise("UNKNOWN")
)
    .withColumn(
    "shipment_status",
    when(col("shipment_status").isin("DELIVERED","IN_TRANSIT"), col("shipment_status"))
    .otherwise("UNKNOWN")
)
    .fillna({
        "customer_id": "UNKNOWN",
        "product_id": "UNKNOWN",
        "carrier_id": "UNKNOWN",
        "freight_cost_usd": 0
    }) 
    #.filter(col("shipment_date").isNotNull())
 
    )





# Detect duplicates


window_spec = Window.partitionBy("shipment_id").orderBy(col("ingest_time").desc())

dedupe_df = clean_df.withColumn(
    "rn",
    row_number().over(window_spec)
)

valid_df = dedupe_df.filter("rn = 1").drop("rn")
duplicate_df = dedupe_df.filter("rn > 1").drop("rn")



#  Create duplicates audit table if not exists



if not spark.catalog.tableExists(dup_audit_table):
    print("Creating Duplicates Audit Table for the first time")
    # Create empty table with schema same as duplicate_df
    empty_df = duplicate_df.limit(0)
    empty_df.write.format("delta").saveAsTable(dup_audit_table)

# Save duplicates
dup_count = duplicate_df.count()

if dup_count > 0:
    duplicate_df.write.mode("append").saveAsTable(dup_audit_table)
    print(f"Duplicates logged: {dup_count}")


# Use deduplicated dataframe for silver
clean_df = valid_df




# Create Silver table


if not spark.catalog.tableExists(silver_table):
    print("First run - Creating Silver table")
    clean_df.write.format("delta").saveAsTable(silver_table)

else:
    print("Table exists - Running INSERT ONLY MERGE (FACT pattern)")

    silver_delta = DeltaTable.forName(spark, silver_table)

    (
        silver_delta.alias("t")
        .merge(
            clean_df.alias("s"),
            "t.shipment_id = s.shipment_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()   
        .execute()
    )


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logistics_catalog.silver.shipments_silver
# MAGIC
# MAGIC
