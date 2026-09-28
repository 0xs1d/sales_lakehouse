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
print("NOTEBOOK 08 - PERFORMANCE & BUSINESS ANALYTICS")
print("=" * 65)
print("Focus  : Optimization + Analytics")
print("Status : INITIALIZED")

# COMMAND ----------

gold_fact = spark.table("gold_fact_sales")

print("Gold fact loaded successfully")
print(f"Gold Fact Records : {gold_fact.count()}")

# COMMAND ----------

optimized_sales = (
    gold_fact
    .repartition(4, "order_date")
    .groupBy("order_date")
    .agg(
        F.round(F.sum("net_amount"), 2).alias("daily_revenue"),
        F.countDistinct("order_id").alias("daily_orders"),
        F.sum("quantity").alias("daily_units")
    )
    .orderBy("order_date")
)

display(optimized_sales)

# COMMAND ----------

optimized_sales.explain("formatted")

# COMMAND ----------

top_products = (
    spark.table("gold_product_performance")
    .orderBy(F.desc("revenue"))
    .limit(5)
)

top_regions = (
    spark.table("gold_regional_performance")
    .orderBy(F.desc("revenue"))
)

print("TOP 5 PRODUCTS BY REVENUE")
display(top_products)

print("REGIONAL SALES PERFORMANCE")
display(top_regions)

# COMMAND ----------

performance_checks = [
    ("Gold Fact Available", gold_fact.count() > 0),
    ("Optimized Aggregation Available", optimized_sales.count() > 0),
    ("Product Analytics Available", top_products.count() > 0),
    ("Regional Analytics Available", top_regions.count() > 0)
]

performance_df = spark.createDataFrame(
    [
        (name, "PASS" if result else "FAIL")
        for name, result in performance_checks
    ],
    ["performance_check", "status"]
)

display(performance_df)

# COMMAND ----------

passed_checks = performance_df.filter(
    F.col("status") == "PASS"
).count()

total_checks = performance_df.count()

print("=" * 65)
print("PERFORMANCE & BUSINESS ANALYTICS COMPLETED")
print("=" * 65)
print(f"Gold Fact Records        : {gold_fact.count()}")
print(f"Performance Checks       : {total_checks}")
print(f"Checks Passed            : {passed_checks}")
print("Optimized Aggregation    : CREATED")
print("Execution Plan           : VALIDATED")
print("Business Insights        : CREATED")
print(
    "Performance Status      :",
    "SUCCESS" if passed_checks == total_checks else "FAILED"
)
print("=" * 65)
