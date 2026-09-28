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

print("=" * 70)
print("ENTERPRISE SALES LAKEHOUSE")
print("NOTEBOOK 09 - FINAL PROJECT VALIDATION")
print("=" * 70)
print("Validation Scope : End-to-End Lakehouse")
print("Status           : INITIALIZED")

# COMMAND ----------

required_tables = [
    "enterprise_customers_source",
    "enterprise_products_source",
    "enterprise_stores_source",
    "enterprise_orders_source",
    "enterprise_order_items_source",

    "bronze_customers",
    "bronze_products",
    "bronze_stores",
    "bronze_orders",
    "bronze_order_items",

    "silver_customers",
    "silver_products",
    "silver_stores",
    "silver_orders",
    "silver_order_items",
    "silver_trusted_sales",

    "gold_fact_sales",
    "gold_dim_customer",
    "gold_dim_product",
    "gold_dim_store",
    "gold_dim_date",
    "gold_product_performance",
    "gold_regional_performance",
    "gold_monthly_sales",

    "audit_layer_metrics",
    "audit_pipeline_runs",
    "audit_monitoring_health"
]

print(f"Required Tables : {len(required_tables)}")

# COMMAND ----------

table_results = []

for table_name in required_tables:
    exists = spark.catalog.tableExists(table_name)

    row_count = (
        spark.table(table_name).count()
        if exists else 0
    )

    table_results.append(
        (
            table_name,
            exists,
            row_count,
            "PASS" if exists and row_count > 0 else "FAIL"
        )
    )

table_validation_df = spark.createDataFrame(
    table_results,
    [
        "table_name",
        "table_exists",
        "record_count",
        "status"
    ]
)

display(table_validation_df)

# COMMAND ----------

source_orders = spark.table(
    "enterprise_orders_source"
).count()

bronze_orders = spark.table(
    "bronze_orders"
).count()

silver_orders = spark.table(
    "silver_orders"
).count()

customers = spark.table(
    "silver_customers"
).count()

products = spark.table(
    "silver_products"
).count()

stores = spark.table(
    "silver_stores"
).count()

count_checks = [
    ("Source Orders Present", source_orders > 0),
    ("Bronze Orders Reconciled", bronze_orders == source_orders),
    ("Silver Orders Not Lost", silver_orders >= source_orders),
    ("Customers Present", customers > 0),
    ("Products Present", products > 0),
    ("Stores Present", stores > 0)
]

count_validation_df = spark.createDataFrame(
    [
        (name, "PASS" if result else "FAIL")
        for name, result in count_checks
    ],
    ["validation_check", "status"]
)

display(count_validation_df)

# COMMAND ----------

silver_orders_df = spark.table("silver_orders")
silver_sales_df = spark.table("silver_trusted_sales")
gold_fact_df = spark.table("gold_fact_sales")

duplicate_orders = (
    silver_orders_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_order_ids = (
    silver_orders_df
    .filter(F.col("order_id").isNull())
    .count()
)

negative_sales = (
    silver_sales_df
    .filter(F.col("net_amount") < 0)
    .count()
)

null_fact_keys = (
    gold_fact_df
    .filter(
        F.col("order_id").isNull() |
        F.col("customer_id").isNull() |
        F.col("product_id").isNull() |
        F.col("store_id").isNull()
    )
    .count()
)

integrity_checks = [
    ("Duplicate Orders", duplicate_orders == 0),
    ("Null Order IDs", null_order_ids == 0),
    ("Negative Silver Sales", negative_sales == 0),
    ("Null Gold Fact Keys", null_fact_keys == 0)
]

integrity_df = spark.createDataFrame(
    [
        (name, "PASS" if result else "FAIL")
        for name, result in integrity_checks
    ],
    ["integrity_check", "status"]
)

display(integrity_df)

# COMMAND ----------

audit_runs = spark.table("audit_pipeline_runs")

pipeline_failures = (
    audit_runs
    .filter(F.col("status") != "SUCCESS")
    .count()
)

monitoring_health = spark.table(
    "audit_monitoring_health"
)

active_alerts = (
    monitoring_health
    .filter(F.col("status") == "ALERT")
    .count()
)

print(f"Pipeline Failures : {pipeline_failures}")
print(f"Active Alerts     : {active_alerts}")
print(
    "Monitoring Status:",
    "PASS"
    if pipeline_failures == 0 and active_alerts == 0
    else "FAIL"
)

# COMMAND ----------

table_pass = (
    table_validation_df
    .filter(F.col("status") == "PASS")
    .count() == len(required_tables)
)

count_pass = (
    count_validation_df
    .filter(F.col("status") == "PASS")
    .count() == count_validation_df.count()
)

integrity_pass = (
    integrity_df
    .filter(F.col("status") == "PASS")
    .count() == integrity_df.count()
)

monitoring_pass = (
    pipeline_failures == 0
    and active_alerts == 0
)

final_checks = [
    ("Required Tables", table_pass),
    ("Expected Record Counts", count_pass),
    ("Data Integrity", integrity_pass),
    ("Incremental Processing", silver_orders >= source_orders),
    ("Gold Analytics", gold_fact_df.count() > 0),
    ("Audit Logging", audit_runs.count() > 0),
    ("Monitoring Health", monitoring_pass)
]

final_validation_df = spark.createDataFrame(
    [
        (
            check_name,
            "PASS" if result else "FAIL"
        )
        for check_name, result in final_checks
    ],
    [
        "validation_area",
        "status"
    ]
)

display(final_validation_df)

# COMMAND ----------

passed_checks = (
    final_validation_df
    .filter(F.col("status") == "PASS")
    .count()
)

total_checks = final_validation_df.count()

project_score = round(
    (passed_checks / total_checks) * 100,
    2
)

print(f"Validation Areas Passed : {passed_checks}/{total_checks}")
print(f"Project Validation Score: {project_score}%")

# COMMAND ----------

project_status = (
    "VALIDATED"
    if project_score == 100.0
    else "REVIEW_REQUIRED"
)

print("=" * 70)
print("ENTERPRISE SALES LAKEHOUSE - FINAL PROJECT SUMMARY")
print("=" * 70)

print(f"Source Orders              : {source_orders}")
print(f"Bronze Orders              : {bronze_orders}")
print(f"Silver Orders              : {silver_orders}")
print(f"Incremental Orders Added   : {silver_orders - bronze_orders}")
print(f"Customers                  : {customers}")
print(f"Products                   : {products}")
print(f"Stores                     : {stores}")
print(f"Gold Fact Records          : {gold_fact_df.count()}")

print("-" * 70)

print(f"Required Tables            : {len(required_tables)}")
print(f"Pipeline Failures          : {pipeline_failures}")
print(f"Active Monitoring Alerts   : {active_alerts}")
print(f"Validation Areas Passed    : {passed_checks}/{total_checks}")
print(f"Project Validation Score   : {project_score}%")
print(f"Project Status             : {project_status}")

print("=" * 70)

if project_status != "VALIDATED":
    raise RuntimeError("Project validation failed; inspect the validation tables above.")
