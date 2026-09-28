# Databricks notebook source
from pyspark.sql import functions as F


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
print("NOTEBOOK 04 - GOLD SALES ANALYTICS")
print("=" * 65)
print("Layer  : GOLD")
print("Status : INITIALIZED")

# COMMAND ----------

sales = spark.table("silver_trusted_sales")
customers = spark.table("silver_customers")
products = spark.table("silver_products")
stores = spark.table("silver_stores")

print("Silver datasets loaded successfully")
print(f"Trusted Sales : {sales.count()}")
print(f"Customers     : {customers.count()}")
print(f"Products      : {products.count()}")
print(f"Stores        : {stores.count()}")

# COMMAND ----------

gold_fact_sales = (
    sales
    .filter(F.col("order_status") == "Completed")
    .select(
        "order_item_id",
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "gross_amount",
        "net_amount",
        F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key")
    )
    .withColumn("gold_processed_timestamp", F.current_timestamp())
)

print(f"Gold Fact Sales Records : {gold_fact_sales.count()}")

display(gold_fact_sales.limit(10))

# COMMAND ----------

gold_dim_customer = customers.select(
    "customer_id", "customer_name", "customer_segment",
    "city", "state", "region"
).dropDuplicates(["customer_id"])

gold_dim_product = products.select(
    "product_id", "product_name", "category",
    "unit_price", "unit_cost", "gross_margin"
).dropDuplicates(["product_id"])

gold_dim_store = stores.select(
    "store_id", "store_name", "city", "state", "region"
).dropDuplicates(["store_id"])

print(f"dim_customer : {gold_dim_customer.count()}")
print(f"dim_product  : {gold_dim_product.count()}")
print(f"dim_store    : {gold_dim_store.count()}")

# COMMAND ----------

gold_kpis = (
    gold_fact_sales
    .agg(
        F.round(F.sum("net_amount"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("units_sold"),
        F.round(
            F.sum("net_amount") / F.countDistinct("order_id"), 2
        ).alias("average_order_value")
    )
)

display(gold_kpis)

# COMMAND ----------

gold_product_performance = (
    gold_fact_sales.alias("f")
    .join(
        gold_dim_product.alias("p"),
        F.col("f.product_id") == F.col("p.product_id"),
        "inner"
    )
    .groupBy(
        F.col("p.product_id"),
        F.col("p.product_name"),
        F.col("p.category")
    )
    .agg(
        F.sum("f.quantity").alias("units_sold"),
        F.round(F.sum("f.net_amount"), 2).alias("revenue"),
        F.countDistinct("f.order_id").alias("orders")
    )
    .orderBy(F.desc("revenue"))
)

display(gold_product_performance)

# COMMAND ----------

gold_regional_performance = (
    gold_fact_sales.alias("f")
    .join(
        gold_dim_store.alias("s"),
        F.col("f.store_id") == F.col("s.store_id"),
        "inner"
    )
    .groupBy("s.region")
    .agg(
        F.round(F.sum("f.net_amount"), 2).alias("revenue"),
        F.countDistinct("f.order_id").alias("orders"),
        F.sum("f.quantity").alias("units_sold")
    )
    .orderBy(F.desc("revenue"))
)

display(gold_regional_performance)

# COMMAND ----------

gold_monthly_sales = (
    gold_fact_sales
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .groupBy("year", "month")
    .agg(
        F.round(F.sum("net_amount"), 2).alias("revenue"),
        F.countDistinct("order_id").alias("orders")
    )
    .orderBy("year", "month")
)

display(gold_monthly_sales)

# COMMAND ----------

date_bounds = gold_fact_sales.select(
    F.min("order_date").alias("min_date"),
    F.max("order_date").alias("max_date"),
).collect()[0]

gold_dim_date = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{date_bounds.min_date}'), "
        f"to_date('{date_bounds.max_date}'), interval 1 day)) AS date"
    )
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day_of_month", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
)

display(gold_dim_date.limit(10))

# COMMAND ----------

gold_tables = {
    "gold_fact_sales": gold_fact_sales,
    "gold_dim_customer": gold_dim_customer,
    "gold_dim_product": gold_dim_product,
    "gold_dim_store": gold_dim_store,
    "gold_dim_date": gold_dim_date,
    "gold_product_performance": gold_product_performance,
    "gold_regional_performance": gold_regional_performance,
    "gold_monthly_sales": gold_monthly_sales
}

for table_name, df in gold_tables.items():
    _write_delta(df, table_name, "gold")

    print(f"{table_name:<30} -> {spark.table(table_name).count()} records")

# COMMAND ----------

null_fact_keys = gold_fact_sales.filter(
    F.col("order_id").isNull() |
    F.col("customer_id").isNull() |
    F.col("product_id").isNull() |
    F.col("store_id").isNull()
).count()

invalid_amounts = gold_fact_sales.filter(
    F.col("net_amount") < 0
).count()

print("=" * 65)
print("GOLD SALES ANALYTICS COMPLETED")
print("=" * 65)
print(f"Gold Tables          : {len(gold_tables)}")
print(f"Fact Sales Records   : {gold_fact_sales.count()}")
print(f"Customer Dimensions  : {gold_dim_customer.count()}")
print(f"Product Dimensions   : {gold_dim_product.count()}")
print(f"Store Dimensions     : {gold_dim_store.count()}")
print(f"Null Fact Keys       : {null_fact_keys}")
print(f"Invalid Sales Amounts: {invalid_amounts}")
print("Star Schema          : CREATED")
print("Business KPIs        : CREATED")
print("Storage Format       : DELTA")
print(
    "Gold Layer Status    :",
    "SUCCESS" if null_fact_keys == 0 and invalid_amounts == 0 else "FAILED"
)
print("=" * 65)
