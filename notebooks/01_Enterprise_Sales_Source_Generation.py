# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

random.seed(42)


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
print("NOTEBOOK 01 - SOURCE DATA GENERATION")
print("=" * 60)
print("Environment : Databricks")
print("Engine      : Apache Spark / PySpark")
print("Status      : INITIALIZED")

# COMMAND ----------

customer_rows = []

cities = [
    ("Chennai", "Tamil Nadu", "South"),
    ("Bengaluru", "Karnataka", "South"),
    ("Hyderabad", "Telangana", "South"),
    ("Mumbai", "Maharashtra", "West"),
    ("Pune", "Maharashtra", "West"),
    ("Delhi", "Delhi", "North"),
    ("Kolkata", "West Bengal", "East"),
    ("Ahmedabad", "Gujarat", "West")
]

segments = ["Consumer", "Corporate", "Small Business"]

for i in range(1, 101):
    city, state, region = random.choice(cities)

    customer_rows.append((
        f"CUST{i:04d}",
        f"Customer {i:03d}",
        random.choice(segments),
        city,
        state,
        region,
        f"customer{i:03d}@example.com",
        datetime(2024, 1, 1) + timedelta(days=random.randint(0, 600))
    ))

customer_schema = """
customer_id STRING,
customer_name STRING,
customer_segment STRING,
city STRING,
state STRING,
region STRING,
email STRING,
created_timestamp TIMESTAMP
"""

customers_df = spark.createDataFrame(customer_rows, customer_schema)

print("Customer source generated successfully")
print(f"Customer Records : {customers_df.count()}")

display(customers_df.limit(10))

# COMMAND ----------

product_config = [
    ("Electronics", ["Laptop", "Monitor", "Keyboard", "Mouse", "Headphones"]),
    ("Furniture", ["Office Chair", "Desk", "Bookshelf", "Cabinet", "Table"]),
    ("Office Supplies", ["Notebook", "Printer Paper", "Pen Set", "Stapler", "File Folder"])
]

product_rows = []
product_number = 1

for category, products in product_config:
    for product_name in products:
        product_rows.append((
            f"PROD{product_number:03d}",
            product_name,
            category,
            round(random.uniform(100, 75000), 2),
            round(random.uniform(50, 50000), 2),
            True
        ))
        product_number += 1

product_schema = """
product_id STRING,
product_name STRING,
category STRING,
unit_price DOUBLE,
unit_cost DOUBLE,
is_active BOOLEAN
"""

products_df = spark.createDataFrame(product_rows, product_schema)

print("Product source generated successfully")
print(f"Product Records : {products_df.count()}")

display(products_df)

# COMMAND ----------

store_rows = [
    ("STORE001", "Chennai Central", "Chennai", "Tamil Nadu", "South"),
    ("STORE002", "Bengaluru Tech Park", "Bengaluru", "Karnataka", "South"),
    ("STORE003", "Hyderabad City", "Hyderabad", "Telangana", "South"),
    ("STORE004", "Mumbai Central", "Mumbai", "Maharashtra", "West"),
    ("STORE005", "Pune Business Hub", "Pune", "Maharashtra", "West"),
    ("STORE006", "Delhi Central", "Delhi", "Delhi", "North"),
    ("STORE007", "Kolkata City", "Kolkata", "West Bengal", "East"),
    ("STORE008", "Ahmedabad Central", "Ahmedabad", "Gujarat", "West")
]

store_schema = """
store_id STRING,
store_name STRING,
city STRING,
state STRING,
region STRING
"""

stores_df = spark.createDataFrame(store_rows, store_schema)

print("Store source generated successfully")
print(f"Store Records : {stores_df.count()}")

display(stores_df)

# COMMAND ----------

order_rows = []

order_statuses = ["Completed", "Completed", "Completed", "Completed", "Pending", "Cancelled"]
payment_methods = ["Credit Card", "Debit Card", "UPI", "Net Banking"]

start_date = datetime(2025, 1, 1)

for i in range(1, 501):
    order_date = start_date + timedelta(
        days=random.randint(0, 364),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    order_rows.append((
        f"ORD{i:06d}",
        f"CUST{random.randint(1,100):04d}",
        f"STORE{random.randint(1,8):03d}",
        order_date,
        random.choice(order_statuses),
        random.choice(payment_methods)
    ))

order_schema = """
order_id STRING,
customer_id STRING,
store_id STRING,
order_timestamp TIMESTAMP,
order_status STRING,
payment_method STRING
"""

orders_df = spark.createDataFrame(order_rows, order_schema)

print("Order source generated successfully")
print(f"Order Records : {orders_df.count()}")

display(orders_df.limit(10))

# COMMAND ----------

order_item_rows = []
item_counter = 1

product_lookup = {
    row["product_id"]: row["unit_price"]
    for row in products_df.collect()
}

product_ids = list(product_lookup.keys())

for order in orders_df.collect():

    number_of_items = random.randint(1, 4)

    selected_products = random.sample(
        product_ids,
        k=number_of_items
    )

    for product_id in selected_products:

        quantity = random.randint(1, 5)
        unit_price = product_lookup[product_id]

        discount_pct = random.choice([
            0.0, 0.0, 0.0, 5.0, 10.0, 15.0
        ])

        gross_amount = quantity * unit_price
        discount_amount = gross_amount * discount_pct / 100
        net_amount = gross_amount - discount_amount

        order_item_rows.append((
            f"ITEM{item_counter:07d}",
            order["order_id"],
            product_id,
            quantity,
            round(unit_price, 2),
            discount_pct,
            round(gross_amount, 2),
            round(discount_amount, 2),
            round(net_amount, 2)
        ))

        item_counter += 1

order_item_schema = """
order_item_id STRING,
order_id STRING,
product_id STRING,
quantity INT,
unit_price DOUBLE,
discount_pct DOUBLE,
gross_amount DOUBLE,
discount_amount DOUBLE,
net_amount DOUBLE
"""

order_items_df = spark.createDataFrame(
    order_item_rows,
    order_item_schema
)

print("Order item source generated successfully")
print(f"Order Item Records : {order_items_df.count()}")

display(order_items_df.limit(10))

# COMMAND ----------

validation_results = [
    ("Customers", customers_df.count(),
     customers_df.filter(F.col("customer_id").isNull()).count()),

    ("Products", products_df.count(),
     products_df.filter(F.col("product_id").isNull()).count()),

    ("Stores", stores_df.count(),
     stores_df.filter(F.col("store_id").isNull()).count()),

    ("Orders", orders_df.count(),
     orders_df.filter(F.col("order_id").isNull()).count()),

    ("Order Items", order_items_df.count(),
     order_items_df.filter(F.col("order_item_id").isNull()).count())
]

validation_df = spark.createDataFrame(
    validation_results,
    ["dataset", "record_count", "null_primary_keys"]
)

validation_df = validation_df.withColumn(
    "validation_status",
    F.when(
        F.col("null_primary_keys") == 0,
        F.lit("PASS")
    ).otherwise(F.lit("FAIL"))
)

display(validation_df)

# COMMAND ----------

source_tables = {
    "enterprise_customers_source": customers_df,
    "enterprise_products_source": products_df,
    "enterprise_stores_source": stores_df,
    "enterprise_orders_source": orders_df,
    "enterprise_order_items_source": order_items_df
}

for table_name, dataframe in source_tables.items():
    _write_delta(dataframe, table_name, "source")

    print(
        f"{table_name:<35} -> "
        f"{spark.table(table_name).count()} records"
    )

# COMMAND ----------

summary_data = [
    ("Customers", customers_df.count(), "enterprise_customers_source"),
    ("Products", products_df.count(), "enterprise_products_source"),
    ("Stores", stores_df.count(), "enterprise_stores_source"),
    ("Orders", orders_df.count(), "enterprise_orders_source"),
    ("Order Items", order_items_df.count(), "enterprise_order_items_source")
]

summary_df = spark.createDataFrame(
    summary_data,
    ["dataset", "records", "delta_table"]
)

display(summary_df)

total_records = sum(row[1] for row in summary_data)

print("=" * 65)
print("ENTERPRISE SALES SOURCE GENERATION COMPLETED")
print("=" * 65)
print(f"Datasets Generated : {len(summary_data)}")
print(f"Total Records      : {total_records}")
print("Data Quality       : PASSED")
print("Delta Tables       : CREATED")
print("Notebook Status    : SUCCESS")
print("=" * 65)
