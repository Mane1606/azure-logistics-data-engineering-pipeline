# Databricks notebook source
from pyspark.sql.functions import *




fact_df = spark.read.table("logistics_catalog.silver.shipments_silver")
customer_df = (
    spark.read.table("logistics_catalog.silver.dim_customer")
    .select(
        "customer_id",
        "customer_name",
        col("country").alias("customer_country"),
        "city"
    )
)
product_df = spark.read.table("logistics_catalog.silver.dim_product")
carrier_df = (
    spark.read.table("logistics_catalog.silver.dim_carrier")
    .select(
        "carrier_id",
        "carrier_name",
        "carrier_type",
        col("country").alias("carrier_country")
    )
)


display(customer_df)

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit, trim

base_df  = (
    fact_df.alias("f")
    # Join customer table
    .join(customer_df.alias("c"), "customer_id", "left")
    # Join product table
    .join(product_df.alias("p"), "product_id", "left")
    # Join carrier table
    .join(carrier_df.alias("r"), "carrier_id", "left")
    .select(
        col("f.*"),
        # Customer columns
        coalesce(trim(col("c.customer_name")), lit("Unknown")).alias("customer_name"),
        coalesce(col("c.customer_country"), lit("Unknown")).alias("customer_country"),
        coalesce(trim(col("c.city")), lit("Unknown")).alias("city"),
        # Product columns
        coalesce(trim(col("p.product_name")), lit("Unknown")).alias("product_name"),
        coalesce(trim(col("p.category")), lit("Unknown")).alias("category"),
        col("p.weight_kg").cast("double").alias("weight_kg"),
        # Carrier columns
        coalesce(trim(col("r.carrier_name")), lit("Unknown")).alias("carrier_name"),
        coalesce(trim(col("r.carrier_type")), lit("Unknown")).alias("carrier_type"),
        coalesce(trim(col("r.carrier_country")), lit("Unknown")).alias("carrier_country")
    )
)


display(base_df)

base_df.write.mode("overwrite").format("delta").saveAsTable(
    "logistics_catalog.gold.base"
)

# COMMAND ----------

from pyspark.sql import functions as F
gold_daily = (
    base_df
    .groupBy("shipment_date")
    .agg(
        count("*").alias("total_shipments"),
        F.round(F.sum("freight_cost_usd"), 2).alias("total_cost"),
         F.round(F.avg("freight_cost_usd"),2).alias("avg_cost"),
        sum(when(col("shipment_status")=="DELIVERED",1).otherwise(0)).alias("delivered_cnt"),
        sum(when(col("shipment_status")=="IN_TRANSIT",1).otherwise(0)).alias("in_transit_cnt")
    )
)


display(gold_daily)

gold_daily.write.mode("overwrite").format("delta").saveAsTable(
    "logistics_catalog.gold.gold_daily_shipments"
)

# COMMAND ----------

gold_customer = (
    base_df
    .groupBy(
        "customer_id",
        "customer_name",
        "customer_country",
        "city"
    )
    .agg(
        count("*").alias("total_shipments"),
        F.round(sum("freight_cost_usd"),2).alias("total_cost")
    )
)
display(gold_customer)

gold_customer.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(
    "logistics_catalog.gold.gold_customer_summary"
)

# COMMAND ----------

gold_product = (
    base_df
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        count("*").alias("total_shipments"),
        F.round(sum("freight_cost_usd"),2).alias("total_cost")
    )
)

gold_product.write.mode("overwrite").format("delta").saveAsTable(
    "logistics_catalog.gold.gold_product_summary"
)


display(gold_product)

# COMMAND ----------

gold_carrier = (
    base_df
    .groupBy(
        "carrier_id",
        "carrier_name",
        "carrier_country",
        "carrier_type"
    )
    .agg(
        count("*").alias("total_shipments"),
        F.round(sum("freight_cost_usd"),2).alias("total_cost")
    )
)

display(gold_carrier)

gold_carrier.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable(
    "logistics_catalog.gold.gold_carrier_summary"
)
