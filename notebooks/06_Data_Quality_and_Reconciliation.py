# Databricks notebook source
from pyspark.sql import functions as F


def _get_parameter(name, default=""):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)
    except Exception:
        return default


catalog_name = _get_parameter("catalog", "")
schema_name = _get_parameter("schema", "")
if catalog_name and schema_name:
    spark.sql(f"USE CATALOG `{catalog_name}`")
    spark.sql(f"USE SCHEMA `{schema_name}`")

print("=" * 65)
print("ENTERPRISE SALES LAKEHOUSE")
print("NOTEBOOK 06 - DATA QUALITY & RECONCILIATION")
print("=" * 65)
print("Framework : Enterprise Data Quality")
print("Status    : INITIALIZED")

# COMMAND ----------

bronze_orders = spark.table("bronze_orders")
bronze_items = spark.table("bronze_order_items")

silver_orders = spark.table("silver_orders")
silver_items = spark.table("silver_order_items")
silver_sales = spark.table("silver_trusted_sales")

gold_fact = spark.table("gold_fact_sales")

print("Lakehouse tables loaded successfully")
print(f"Bronze Orders       : {bronze_orders.count()}")
print(f"Silver Orders       : {silver_orders.count()}")
print(f"Silver Order Items  : {silver_items.count()}")
print(f"Trusted Sales       : {silver_sales.count()}")
print(f"Gold Fact Sales     : {gold_fact.count()}")

# COMMAND ----------

quality_checks = [
    ("Silver Orders - Null Order ID",
     silver_orders.filter(F.col("order_id").isNull()).count()),

    ("Silver Orders - Duplicate Order ID",
     silver_orders.groupBy("order_id").count()
                  .filter(F.col("count") > 1).count()),

    ("Silver Items - Null Item ID",
     silver_items.filter(F.col("order_item_id").isNull()).count()),

    ("Silver Items - Invalid Quantity",
     silver_items.filter(F.col("quantity") <= 0).count()),

    ("Silver Items - Negative Price",
     silver_items.filter(F.col("unit_price") < 0).count()),

    ("Trusted Sales - Null Customer",
     silver_sales.filter(F.col("customer_id").isNull()).count()),

    ("Trusted Sales - Null Product",
     silver_sales.filter(F.col("product_id").isNull()).count()),

    ("Trusted Sales - Negative Net Amount",
     silver_sales.filter(F.col("net_amount") < 0).count()),

    ("Gold Fact - Null Order ID",
     gold_fact.filter(F.col("order_id").isNull()).count()),

    ("Gold Fact - Negative Net Amount",
     gold_fact.filter(F.col("net_amount") < 0).count())
]

quality_df = (
    spark.createDataFrame(
        quality_checks,
        ["quality_check", "failed_records"]
    )
    .withColumn(
        "status",
        F.when(F.col("failed_records") == 0, "PASS")
         .otherwise("FAIL")
    )
)

display(quality_df)

# COMMAND ----------

bronze_order_count = bronze_orders.count()
silver_order_count = silver_orders.count()

incremental_orders = silver_order_count - bronze_order_count

silver_item_count = silver_items.count()
trusted_sales_count = silver_sales.count()

reconciliation_data = [
    (
        "Bronze → Silver Orders",
        bronze_order_count,
        silver_order_count,
        incremental_orders,
        "PASS" if silver_order_count >= bronze_order_count else "FAIL"
    ),
    (
        "Silver Items → Trusted Sales",
        silver_item_count,
        trusted_sales_count,
        silver_item_count - trusted_sales_count,
        "PASS" if silver_item_count == trusted_sales_count else "FAIL"
    )
]

reconciliation_df = spark.createDataFrame(
    reconciliation_data,
    [
        "reconciliation",
        "source_records",
        "target_records",
        "difference",
        "status"
    ]
)

display(reconciliation_df)

# COMMAND ----------

orphan_customer_orders = (
    silver_orders.alias("o")
    .join(
        spark.table("silver_customers").alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left_anti"
    )
    .count()
)

orphan_store_orders = (
    silver_orders.alias("o")
    .join(
        spark.table("silver_stores").alias("s"),
        F.col("o.store_id") == F.col("s.store_id"),
        "left_anti"
    )
    .count()
)

orphan_product_items = (
    silver_items.alias("i")
    .join(
        spark.table("silver_products").alias("p"),
        F.col("i.product_id") == F.col("p.product_id"),
        "left_anti"
    )
    .count()
)

print(f"Orphan Customer References : {orphan_customer_orders}")
print(f"Orphan Store References    : {orphan_store_orders}")
print(f"Orphan Product References  : {orphan_product_items}")

# COMMAND ----------

quality_failures = quality_df.filter(
    F.col("status") == "FAIL"
).count()

reconciliation_failures = reconciliation_df.filter(
    F.col("status") == "FAIL"
).count()

referential_failures = (
    orphan_customer_orders +
    orphan_store_orders +
    orphan_product_items
)

overall_status = (
    "SUCCESS"
    if quality_failures == 0
    and reconciliation_failures == 0
    and referential_failures == 0
    else "FAILED"
)

print("=" * 65)
print("ENTERPRISE DATA QUALITY VALIDATION COMPLETED")
print("=" * 65)
print(f"Quality Checks              : {quality_df.count()}")
print(f"Quality Failures            : {quality_failures}")
print(f"Reconciliation Checks       : {reconciliation_df.count()}")
print(f"Reconciliation Failures     : {reconciliation_failures}")
print(f"Referential Integrity Issues: {referential_failures}")
print("Data Quality                :",
      "PASSED" if quality_failures == 0 else "FAILED")
print("Reconciliation              :",
      "PASSED" if reconciliation_failures == 0 else "FAILED")
print("Referential Integrity       :",
      "PASSED" if referential_failures == 0 else "FAILED")
print(f"Overall Validation Status   : {overall_status}")
print("=" * 65)
