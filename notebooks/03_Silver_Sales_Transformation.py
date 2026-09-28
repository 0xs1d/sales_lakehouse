# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
print("NOTEBOOK 03 - SILVER SALES TRANSFORMATION")
print("=" * 65)
print("Layer  : SILVER")
print("Engine : PySpark")
print("Status : INITIALIZED")

# COMMAND ----------

bronze_customers = spark.table("bronze_customers")
bronze_products = spark.table("bronze_products")
bronze_stores = spark.table("bronze_stores")
bronze_orders = spark.table("bronze_orders")
bronze_order_items = spark.table("bronze_order_items")

print("Bronze tables loaded successfully")
print(f"Customers   : {bronze_customers.count()}")
print(f"Products    : {bronze_products.count()}")
print(f"Stores      : {bronze_stores.count()}")
print(f"Orders      : {bronze_orders.count()}")
print(f"Order Items : {bronze_order_items.count()}")

# COMMAND ----------

silver_customers = (
    bronze_customers
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("customer_segment", F.trim(F.col("customer_segment")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("region", F.upper(F.trim(F.col("region"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

print(f"Silver Customers : {silver_customers.count()}")

display(
    silver_customers.select(
        "customer_id",
        "customer_name",
        "customer_segment",
        "city",
        "state",
        "region",
        "email"
    ).limit(10)
)

# COMMAND ----------

silver_products = (
    bronze_products
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("unit_price") >= 0)
    .dropDuplicates(["product_id"])
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn(
        "gross_margin",
        F.round(F.col("unit_price") - F.col("unit_cost"), 2)
    )
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

silver_stores = (
    bronze_stores
    .filter(F.col("store_id").isNotNull())
    .dropDuplicates(["store_id"])
    .withColumn("store_name", F.trim(F.col("store_name")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
    .withColumn("region", F.upper(F.trim(F.col("region"))))
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

print(f"Silver Products : {silver_products.count()}")
print(f"Silver Stores   : {silver_stores.count()}")

# COMMAND ----------

silver_orders = (
    bronze_orders
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("store_id").isNotNull())
    .dropDuplicates(["order_id"])
    .withColumn("order_date", F.to_date(F.col("order_timestamp")))
    .withColumn("order_year", F.year(F.col("order_timestamp")))
    .withColumn("order_month", F.month(F.col("order_timestamp")))
    .withColumn(
        "order_status",
        F.initcap(F.trim(F.col("order_status")))
    )
    .withColumn(
        "payment_method",
        F.initcap(F.trim(F.col("payment_method")))
    )
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

print(f"Silver Orders : {silver_orders.count()}")

display(
    silver_orders.select(
        "order_id",
        "customer_id",
        "store_id",
        "order_date",
        "order_year",
        "order_month",
        "order_status",
        "payment_method"
    ).limit(10)
)

# COMMAND ----------

silver_order_items = (
    bronze_order_items
    .filter(F.col("order_item_id").isNotNull())
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") >= 0)
    .dropDuplicates(["order_item_id"])
    .withColumn(
        "calculated_gross_amount",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
    .withColumn(
        "calculated_net_amount",
        F.round(
            F.col("quantity") * F.col("unit_price") *
            (1 - F.col("discount_pct") / 100),
            2
        )
    )
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)

print(f"Silver Order Items : {silver_order_items.count()}")

display(
    silver_order_items.select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "calculated_gross_amount",
        "calculated_net_amount"
    ).limit(10)
)

# COMMAND ----------

trusted_sales = (
    silver_order_items.alias("oi")

    .join(
        silver_orders.alias("o"),
        F.col("oi.order_id") == F.col("o.order_id"),
        "inner"
    )

    .join(
        silver_customers.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "inner"
    )

    .join(
        silver_products.alias("p"),
        F.col("oi.product_id") == F.col("p.product_id"),
        "inner"
    )

    .join(
        silver_stores.alias("s"),
        F.col("o.store_id") == F.col("s.store_id"),
        "inner"
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
        F.col("oi.calculated_net_amount").alias("net_amount")
    )

    .withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )
)

print(f"Trusted Sales Records : {trusted_sales.count()}")

display(
    trusted_sales.select(
        "order_id",
        "order_date",
        "customer_name",
        "product_name",
        "store_name",
        "region",
        "quantity",
        "gross_amount",
        "net_amount"
    ).limit(10)
)

# COMMAND ----------

silver_tables = {
    "silver_customers": silver_customers,
    "silver_products": silver_products,
    "silver_stores": silver_stores,
    "silver_orders": silver_orders,
    "silver_order_items": silver_order_items,
    "silver_trusted_sales": trusted_sales
}

for table_name, df in silver_tables.items():
    _write_delta(df, table_name, "silver")

    print(
        f"{table_name:<30} -> "
        f"{spark.table(table_name).count()} records"
    )

# COMMAND ----------

quality_checks = [
    (
        "Customer Primary Key",
        silver_customers.filter(
            F.col("customer_id").isNull()
        ).count()
    ),
    (
        "Product Primary Key",
        silver_products.filter(
            F.col("product_id").isNull()
        ).count()
    ),
    (
        "Order Primary Key",
        silver_orders.filter(
            F.col("order_id").isNull()
        ).count()
    ),
    (
        "Order Item Primary Key",
        silver_order_items.filter(
            F.col("order_item_id").isNull()
        ).count()
    ),
    (
        "Invalid Quantity",
        silver_order_items.filter(
            F.col("quantity") <= 0
        ).count()
    ),
    (
        "Invalid Unit Price",
        silver_order_items.filter(
            F.col("unit_price") < 0
        ).count()
    ),
    (
        "Trusted Sales Null Order",
        trusted_sales.filter(
            F.col("order_id").isNull()
        ).count()
    )
]

silver_quality_df = spark.createDataFrame(
    quality_checks,
    ["quality_check", "failed_records"]
).withColumn(
    "status",
    F.when(F.col("failed_records") == 0, "PASS")
     .otherwise("FAIL")
)

display(silver_quality_df)

# COMMAND ----------

failed_checks = silver_quality_df.filter(
    F.col("status") == "FAIL"
).count()

print("=" * 65)
print("SILVER SALES TRANSFORMATION COMPLETED")
print("=" * 65)
print(f"Silver Tables        : {len(silver_tables)}")
print(f"Customers            : {silver_customers.count()}")
print(f"Products             : {silver_products.count()}")
print(f"Stores               : {silver_stores.count()}")
print(f"Orders               : {silver_orders.count()}")
print(f"Order Items          : {silver_order_items.count()}")
print(f"Trusted Sales        : {trusted_sales.count()}")
print(f"Data Quality Failures: {failed_checks}")
print(
    "Data Quality        :",
    "PASSED" if failed_checks == 0 else "FAILED"
)
print("Storage Format       : DELTA")
print("Silver Layer Status  : SUCCESS")
print("=" * 65)
