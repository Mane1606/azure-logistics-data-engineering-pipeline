# Databricks notebook source
df = spark.read.table("logistics_catalog.bronze.customers_bronze")
display(df)

# COMMAND ----------


# SILVER DIMENSIONS TABLE CREATION


from pyspark.sql.functions import *
from pyspark.sql.window import Window   
from delta.tables import DeltaTable



# Merge Function 


def type1_merge(src_df, target_table, key_col):
    
    if not spark.catalog.tableExists(target_table):
        print(f"Creating table first time → {target_table}")
        src_df.write.format("delta").saveAsTable(target_table)

    else:
        print(f"Merging into → {target_table}")
        delta_tbl = DeltaTable.forName(spark, target_table)

        (
            delta_tbl.alias("t")
            .merge(
                src_df.alias("s"),
                f"t.{key_col} = s.{key_col}"
            )
            .whenMatchedUpdateAll()       
            .whenNotMatchedInsertAll()
            .execute()
        )


# CUSTOMER TABLE


def load_customer():

    bronze_table = "logistics_catalog.bronze.customers_bronze"
    silver_table = "logistics_catalog.silver.dim_customer"

    df = spark.read.table(bronze_table)

    
    window_spec = Window.partitionBy("customer_id") \
        .orderBy(col("_metadata.file_modification_time").desc())

    clean_df = (
        df
        .withColumn("customer_id", upper(trim(col("customer_id")))) \
        .withColumn("customer_name", initcap(trim(col("customer_name")))) \
        .withColumn("country", upper(trim(col("country")))) \
        .withColumn("city", initcap(trim(col("city")))) 
        .withColumn("rn", row_number().over(window_spec))
        .filter("rn = 1")
        .drop("rn")
        .fillna({
        "customer_name": "UNKNOWN",
        "country": "UNKNOWN",
        "city": "UNKNOWN"
        })
        .dropDuplicates(["customer_id"])
    )

    

    type1_merge(clean_df, silver_table, "customer_id")



# PRODUCT TABLE


def load_product():

    bronze_table = "logistics_catalog.bronze.products_bronze"
    silver_table = "logistics_catalog.silver.dim_product"

    df = spark.read.table(bronze_table)

    window_spec = Window.partitionBy("product_id") \
        .orderBy(col("_metadata.file_modification_time").desc())

    clean_df = (
        df
        .withColumn("product_id", upper(trim(col("product_id")))) \
        .withColumn("product_name", initcap(trim(col("product_name")))) \
        .withColumn("category", upper(trim(col("category"))))
        .withColumn(
        "weight_kg",
        when(col("weight_kg").cast("double") <= 0, None)
        .otherwise(col("weight_kg").cast("double"))
        )
        .fillna({
        "category": "UNKNOWN",
        "weight_kg": 0
        })
        .withColumn("rn", row_number().over(window_spec))
        .filter("rn = 1")
        .drop("rn")
        .dropDuplicates(["product_id"])
        
    )

    type1_merge(clean_df, silver_table, "product_id")



# CARRIER TABLE


def load_carrier():

    bronze_table = "logistics_catalog.bronze.carrier_bronze"
    silver_table = "logistics_catalog.silver.dim_carrier"

    df = spark.read.table(bronze_table)

    window_spec = Window.partitionBy("carrier_id") \
        .orderBy(col("_metadata.file_modification_time").desc())

    clean_df = (
        df
        .withColumn("carrier_id", upper(trim(col("carrier_id")))) \
        .withColumn("carrier_name", initcap(trim(col("carrier_name")))) \
        .withColumn("carrier_type", upper(trim(col("carrier_type")))) \
        .withColumn("country", upper(trim(col("country"))))
        .fillna({
        "carrier_name": "UNKNOWN",
        "carrier_type": "UNKNOWN",
        "country": "UNKNOWN"
         })
        .withColumn("rn", row_number().over(window_spec))
        .filter("rn = 1")
        .drop("rn")
        .dropDuplicates(["carrier_id"])
       

    )

    type1_merge(clean_df, silver_table, "carrier_id")



# FUNCTION EXECUTION


print("Starting Dimension Loads...")

load_customer()
load_product()
load_carrier()

print("All dimensions loaded successfully ✅")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from logistics_catalog.silver.dim_customer
