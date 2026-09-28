# Databricks notebook source
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType


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


print("=" * 65)
print("ENTERPRISE SALES LAKEHOUSE")
print("NOTEBOOK 05 - INCREMENTAL SALES PROCESSING")
print("=" * 65)
print("Pattern : Incremental Load + Delta MERGE")
print("Status  : INITIALIZED")

# COMMAND ----------

current_orders = spark.table("silver_orders")
before_count = current_orders.count()

print(f"Current Silver Orders : {before_count}")
print(f"Distinct Order IDs    : {current_orders.select('order_id').distinct().count()}")

# COMMAND ----------

incremental_order_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("order_status", StringType(), False),
        StructField("payment_method", StringType(), False),
    ]
)

if spark.catalog.tableExists("enterprise_orders_incremental_source"):
    incoming_source = spark.table("enterprise_orders_incremental_source").select(
        "order_id",
        "customer_id",
        "store_id",
        "order_timestamp",
        "order_status",
        "payment_method",
    )
else:
    incremental_data = [
        (
            "ORD000501",
            "CUST0001",
            "STORE001",
            datetime(2025, 12, 31, 10, 30),
            "Completed",
            "UPI",
        ),
        (
            "ORD000502",
            "CUST0025",
            "STORE004",
            datetime(2025, 12, 31, 11, 15),
            "Completed",
            "Credit Card",
        ),
        (
            "ORD000503",
            "CUST0050",
            "STORE006",
            datetime(2025, 12, 31, 12, 45),
            "Pending",
            "Debit Card",
        ),
        (
            "ORD000010",
            "CUST0010",
            "STORE002",
            datetime(2025, 1, 15, 14, 30),
            "Completed",
            "UPI",
        ),
    ]
    incoming_source = spark.createDataFrame(incremental_data, incremental_order_schema)
    _write_delta(incoming_source, "enterprise_orders_incremental_source", "source")

incremental_orders = (
    incoming_source.dropDuplicates(["order_id"])
    .withColumn("order_date", F.to_date("order_timestamp"))
    .withColumn("order_year", F.year("order_timestamp"))
    .withColumn("order_month", F.month("order_timestamp"))
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

new_orders = incremental_orders.join(
    current_orders.select("order_id"), "order_id", "leftanti"
)
new_order_count = new_orders.count()
updated_order_count = incremental_orders.count() - new_order_count

display(incremental_orders)

# COMMAND ----------

target = DeltaTable.forName(spark, "silver_orders")

(
    target.alias("target")
    .merge(
        incremental_orders.alias("source"),
        "target.order_id = source.order_id",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Silver order Delta MERGE completed successfully")

# COMMAND ----------

item_schema = StructType(
    [
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_pct", DoubleType(), False),
        StructField("gross_amount", DoubleType(), False),
        StructField("discount_amount", DoubleType(), False),
        StructField("net_amount", DoubleType(), False),
    ]
)

if spark.catalog.tableExists("enterprise_order_items_incremental_source"):
    incoming_items = spark.table("enterprise_order_items_incremental_source").select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "gross_amount",
        "discount_amount",
        "net_amount",
    )
else:
    product = spark.table("silver_products").select("product_id", "unit_price").first()
    generated_items = []
    for row in new_orders.select("order_id").collect():
        quantity = 1
        unit_price = float(product["unit_price"])
        gross_amount = round(quantity * unit_price, 2)
        generated_items.append(
            (
                f"INC_{row['order_id']}",
                row["order_id"],
                product["product_id"],
                quantity,
                unit_price,
                0.0,
                gross_amount,
                0.0,
                gross_amount,
            )
        )
    incoming_items = spark.createDataFrame(generated_items, item_schema)
    _write_delta(incoming_items, "enterprise_order_items_incremental_source", "source")

current_items = spark.table("silver_order_items")
if incoming_items.count() > 0:
    item_target = DeltaTable.forName(spark, "silver_order_items")
    merge_items = (
        incoming_items.withColumn(
            "calculated_gross_amount",
            F.round(F.col("quantity") * F.col("unit_price"), 2),
        )
        .withColumn(
            "calculated_net_amount",
            F.round(
                F.col("quantity")
                * F.col("unit_price")
                * (1 - F.col("discount_pct") / 100),
                2,
            ),
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )
    (
        item_target.alias("target")
        .merge(
            merge_items.alias("source"),
            "target.order_item_id = source.order_item_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

silver_orders_after = spark.table("silver_orders")
silver_items_after = spark.table("silver_order_items")

trusted_sales = (
    silver_items_after.alias("oi")
    .join(
        silver_orders_after.alias("o"),
        F.col("oi.order_id") == F.col("o.order_id"),
        "inner",
    )
    .join(
        spark.table("silver_customers").alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "inner",
    )
    .join(
        spark.table("silver_products").alias("p"),
        F.col("oi.product_id") == F.col("p.product_id"),
        "inner",
    )
    .join(
        spark.table("silver_stores").alias("s"),
        F.col("o.store_id") == F.col("s.store_id"),
        "inner",
    )
    .select(
        F.col("oi.order_item_id"),
        F.col("o.order_id"),
        F.col("o.order_date"),
        F.col("o.order_year"),
        F.col("o.order_month"),
        F.col("o.order_status"),
        F.col("o.payment_method"),
        F.col("c.customer_id"),
        F.col("c.customer_name"),
        F.col("c.customer_segment"),
        F.col("p.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("s.store_id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("oi.discount_pct"),
        F.col("oi.calculated_gross_amount").alias("gross_amount"),
        F.col("oi.calculated_net_amount").alias("net_amount"),
    )
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

_write_delta(trusted_sales, "silver_trusted_sales", "silver")

# COMMAND ----------

after_orders = spark.table("silver_orders")
after_count = after_orders.count()
distinct_count = after_orders.select("order_id").distinct().count()
expected_count = before_count + new_order_count

validation = spark.createDataFrame(
    [
        ("Before MERGE", before_count),
        ("Incoming Batch", incremental_orders.count()),
        ("New Records", new_order_count),
        ("Updated Records", updated_order_count),
        ("Expected Final Count", expected_count),
        ("After MERGE", after_count),
        ("Distinct Order IDs", distinct_count),
        ("Trusted Sales Records", trusted_sales.count()),
    ],
    ["metric", "value"],
)

display(validation)

# COMMAND ----------

count_before_rerun = spark.table("silver_orders").count()
rerun_target = DeltaTable.forName(spark, "silver_orders")
(
    rerun_target.alias("target")
    .merge(
        incremental_orders.alias("source"),
        "target.order_id = source.order_id",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
count_after_rerun = spark.table("silver_orders").count()

duplicate_ids = (
    spark.table("silver_orders")
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

merge_status = (
    "SUCCESS"
    if after_count == expected_count
    and duplicate_ids == 0
    and count_before_rerun == count_after_rerun
    else "FAILED"
)

print("IDEMPOTENCY VALIDATION")
print("-" * 40)
print(f"Before Re-run : {count_before_rerun}")
print(f"After Re-run  : {count_after_rerun}")
print(f"Duplicates    : {count_after_rerun - count_before_rerun}")
print("Status        :", "PASS" if count_before_rerun == count_after_rerun else "FAIL")

print("=" * 65)
print("INCREMENTAL SALES PROCESSING COMPLETED")
print("=" * 65)
print(f"Initial Records      : {before_count}")
print(f"Incoming Records     : {incremental_orders.count()}")
print(f"New Records          : {new_order_count}")
print(f"Updated Records      : {updated_order_count}")
print(f"Final Records        : {after_count}")
print(f"Trusted Sales        : {trusted_sales.count()}")
print(f"Duplicate Order IDs  : {duplicate_ids}")
print("Processing Pattern   : DELTA MERGE")
print("Idempotency          :", "PASSED" if count_before_rerun == count_after_rerun else "FAILED")
print(f"Incremental Status   : {merge_status}")
print("=" * 65)
