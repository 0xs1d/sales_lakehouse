# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime


def _get_parameter(name, default=""):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)
    except Exception:
        return default


environment = _get_parameter("environment", "dev")
storage_account_name = _get_parameter("storageAccountName")
catalog_name = _get_parameter("catalog", "")
schema_name = _get_parameter("schema", "")

if catalog_name and schema_name:
    spark.sql(f"USE CATALOG `{catalog_name}`")
    spark.sql(f"USE SCHEMA `{schema_name}`")


def _write_delta(dataframe, table_name, layer_name):
    writer = (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if storage_account_name:
        writer = writer.option(
            "path",
            f"abfss://{layer_name}@{storage_account_name}.dfs.core.windows.net/"
            f"{environment}/{table_name}",
        )
    writer.saveAsTable(table_name)

print("=" * 60)
print("ENTERPRISE SALES LAKEHOUSE")
print("NOTEBOOK 02 - BRONZE SALES INGESTION")
print("=" * 60)
print("Layer  : BRONZE")
print("Status : INITIALIZED")

# COMMAND ----------

customers_source = spark.table("enterprise_customers_source")
products_source = spark.table("enterprise_products_source")
stores_source = spark.table("enterprise_stores_source")
orders_source = spark.table("enterprise_orders_source")
order_items_source = spark.table("enterprise_order_items_source")

print("Source Delta tables loaded successfully")
print(f"Customers   : {customers_source.count()}")
print(f"Products    : {products_source.count()}")
print(f"Stores      : {stores_source.count()}")
print(f"Orders      : {orders_source.count()}")
print(f"Order Items : {order_items_source.count()}")

# COMMAND ----------

def add_bronze_metadata(df, source_name):
    return (
        df
        .withColumn("source_system", F.lit(source_name))
        .withColumn("bronze_ingestion_timestamp", F.current_timestamp())
        .withColumn(
            "bronze_batch_id",
            F.concat(
                F.lit("BATCH_"),
                F.date_format(F.current_timestamp(), "yyyyMMddHHmmss")
            )
        )
    )

bronze_customers = add_bronze_metadata(customers_source, "ENTERPRISE_CUSTOMER_SOURCE")
bronze_products = add_bronze_metadata(products_source, "ENTERPRISE_PRODUCT_SOURCE")
bronze_stores = add_bronze_metadata(stores_source, "ENTERPRISE_STORE_SOURCE")
bronze_orders = add_bronze_metadata(orders_source, "ENTERPRISE_ORDER_SOURCE")
bronze_order_items = add_bronze_metadata(order_items_source, "ENTERPRISE_ORDER_ITEM_SOURCE")

print("Bronze metadata added successfully")

display(
    bronze_orders
    .select(
        "order_id",
        "customer_id",
        "store_id",
        "order_status",
        "source_system",
        "bronze_ingestion_timestamp",
        "bronze_batch_id"
    )
    .limit(10)
)

# COMMAND ----------

bronze_tables = {
    "bronze_customers": bronze_customers,
    "bronze_products": bronze_products,
    "bronze_stores": bronze_stores,
    "bronze_orders": bronze_orders,
    "bronze_order_items": bronze_order_items
}

for table_name, df in bronze_tables.items():
    _write_delta(df, table_name, "bronze")

    print(f"{table_name:<25} -> {spark.table(table_name).count()} records")

# COMMAND ----------

reconciliation_data = [
    ("Customers", customers_source.count(), spark.table("bronze_customers").count()),
    ("Products", products_source.count(), spark.table("bronze_products").count()),
    ("Stores", stores_source.count(), spark.table("bronze_stores").count()),
    ("Orders", orders_source.count(), spark.table("bronze_orders").count()),
    ("Order Items", order_items_source.count(), spark.table("bronze_order_items").count())
]

reconciliation_df = spark.createDataFrame(
    reconciliation_data,
    ["dataset", "source_count", "bronze_count"]
)

reconciliation_df = (
    reconciliation_df
    .withColumn(
        "difference",
        F.col("source_count") - F.col("bronze_count")
    )
    .withColumn(
        "status",
        F.when(F.col("source_count") == F.col("bronze_count"), "PASS")
         .otherwise("FAIL")
    )
)

display(reconciliation_df)

# COMMAND ----------

quality_results = [
    (
        "bronze_customers",
        spark.table("bronze_customers").count(),
        spark.table("bronze_customers")
             .filter(F.col("customer_id").isNull()).count()
    ),
    (
        "bronze_products",
        spark.table("bronze_products").count(),
        spark.table("bronze_products")
             .filter(F.col("product_id").isNull()).count()
    ),
    (
        "bronze_stores",
        spark.table("bronze_stores").count(),
        spark.table("bronze_stores")
             .filter(F.col("store_id").isNull()).count()
    ),
    (
        "bronze_orders",
        spark.table("bronze_orders").count(),
        spark.table("bronze_orders")
             .filter(F.col("order_id").isNull()).count()
    ),
    (
        "bronze_order_items",
        spark.table("bronze_order_items").count(),
        spark.table("bronze_order_items")
             .filter(F.col("order_item_id").isNull()).count()
    )
]

bronze_quality_df = spark.createDataFrame(
    quality_results,
    ["table_name", "record_count", "null_primary_keys"]
).withColumn(
    "quality_status",
    F.when(F.col("null_primary_keys") == 0, "PASS")
     .otherwise("FAIL")
)

display(bronze_quality_df)

# COMMAND ----------

total_bronze_records = sum(
    spark.table(table_name).count()
    for table_name in bronze_tables.keys()
)

failed_reconciliations = reconciliation_df.filter(
    F.col("status") == "FAIL"
).count()

failed_quality_checks = bronze_quality_df.filter(
    F.col("quality_status") == "FAIL"
).count()

print("=" * 65)
print("BRONZE SALES INGESTION COMPLETED")
print("=" * 65)
print(f"Bronze Tables          : {len(bronze_tables)}")
print(f"Total Bronze Records   : {total_bronze_records}")
print(f"Reconciliation Failures: {failed_reconciliations}")
print(f"Data Quality Failures  : {failed_quality_checks}")
print(
    "Reconciliation        :",
    "PASSED" if failed_reconciliations == 0 else "FAILED"
)
print(
    "Data Quality          :",
    "PASSED" if failed_quality_checks == 0 else "FAILED"
)
print("Storage Format         : DELTA")
print("Bronze Layer Status    : SUCCESS")
print("=" * 65)
