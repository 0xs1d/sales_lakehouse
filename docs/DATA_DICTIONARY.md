# 📚 Enterprise Sales Lakehouse — Data Dictionary

## 📌 Overview

This document describes the primary datasets and business attributes used throughout the **Enterprise Sales Lakehouse** project.

The project models a typical enterprise sales environment containing customers, products, orders, sales transactions, processing metadata, data quality results, and analytical outputs.

---

## 👤 Customer Dataset

The customer dataset represents customers participating in sales transactions.

| Column | Description |
|---|---|
| `customer_id` | Unique identifier assigned to each customer |
| `customer_name` | Customer name |
| `email` | Customer email address |
| `city` | Customer city |
| `state` | Customer state or region |
| `country` | Customer country |
| `customer_segment` | Business classification of the customer |
| `created_date` | Date the customer record was created |

### Purpose

Customer information is used for:

- Customer-level sales analysis
- Revenue segmentation
- Geographic analysis
- Customer behavior analysis
- Business reporting

---

## 📦 Product Dataset

The product dataset contains information about products sold by the organization.

| Column | Description |
|---|---|
| `product_id` | Unique product identifier |
| `product_name` | Product name |
| `category` | High-level product category |
| `subcategory` | Detailed product classification |
| `unit_price` | Standard selling price of the product |
| `cost_price` | Cost associated with the product |
| `supplier` | Product supplier or source |
| `created_date` | Product record creation date |

### Purpose

Product data supports:

- Product performance analysis
- Category-level reporting
- Revenue analysis
- Profitability analysis
- Sales trend analysis

---

## 🛒 Orders Dataset

The orders dataset represents enterprise sales transactions.

| Column | Description |
|---|---|
| `order_id` | Unique identifier for each order |
| `customer_id` | Customer associated with the order |
| `product_id` | Product associated with the transaction |
| `order_date` | Date the order was created |
| `quantity` | Number of units ordered |
| `unit_price` | Selling price per unit |
| `discount` | Discount applied to the transaction |
| `sales_amount` | Calculated sales value |
| `order_status` | Current status of the order |
| `payment_method` | Payment method used for the transaction |

### Business Relationship

```text
Customer
    │
    └──── customer_id
              │
              ▼
            Orders
              ▲
              │
    ┌──── product_id
    │
Product
```

---

## 🥉 Bronze Layer Dataset

The Bronze layer stores raw or minimally processed source records.

Typical ingestion metadata includes:

| Column | Description |
|---|---|
| `ingestion_timestamp` | Timestamp when the record entered the Lakehouse |
| `source_system` | Source from which the record originated |
| `batch_id` | Identifier for the ingestion batch |
| `source_file` | Source file or logical source reference |

### Bronze Layer Objective

The Bronze layer provides:

- Raw data preservation
- Traceability
- Reprocessing capability
- Source-level auditing
- Ingestion monitoring

---

## 🥈 Silver Layer Dataset

The Silver layer contains cleaned and trusted sales information.

Typical transformations include:

- Data type standardization
- Null handling
- Duplicate removal
- Business-rule validation
- Invalid-record handling
- Derived-column generation
- Data quality checks

### Example Derived Attributes

| Column | Description |
|---|---|
| `gross_sales` | Sales amount before applicable adjustments |
| `discount_amount` | Monetary value of the applied discount |
| `net_sales` | Final sales amount after discount |
| `processing_timestamp` | Timestamp when Silver transformation occurred |
| `data_quality_status` | Result of applicable quality validation |

---

## 🥇 Gold Layer — Fact Sales

The Gold layer provides analytics-ready sales information.

A primary analytical output is the **Fact Sales** dataset.

| Column | Description |
|---|---|
| `order_id` | Sales transaction identifier |
| `customer_id` | Customer identifier |
| `product_id` | Product identifier |
| `order_date` | Transaction date |
| `quantity` | Number of units sold |
| `gross_sales` | Gross transaction value |
| `discount_amount` | Discount value |
| `net_sales` | Final sales value |
| `customer_segment` | Customer business segment |
| `product_category` | Product category |
| `order_status` | Transaction status |

The fact table also contains `date_key`, which joins to `gold_dim_date`.

## 📅 Gold Layer — Date Dimension

The Gold layer creates a calendar dimension covering the minimum and maximum order dates.

| Column | Description |
|---|---|
| `date_key` | Integer calendar key in `yyyyMMdd` format |
| `date` | Calendar date |
| `year` | Calendar year |
| `quarter` | Calendar quarter |
| `month` | Numeric month |
| `month_name` | Month name |
| `week_of_year` | Week number used for reporting |
| `day_of_month` | Day number within the month |
| `day_of_week` | Spark day-of-week number |
| `is_weekend` | Weekend indicator |

---

## 📊 Business KPIs

Gold-level analytical processing can generate business KPIs such as:

| KPI | Description |
|---|---|
| Total Sales | Total value of sales transactions |
| Net Revenue | Revenue after applicable discounts |
| Total Orders | Number of processed orders |
| Average Order Value | Average monetary value per order |
| Units Sold | Total quantity of products sold |
| Customer Revenue | Revenue grouped by customer |
| Category Revenue | Revenue grouped by product category |
| Sales Growth | Change in sales performance over time |

---

## 🔄 Incremental Processing Metadata

Incremental processing identifies newly arriving or changed data.

Typical metadata includes:

| Attribute | Description |
|---|---|
| `batch_id` | Processing batch identifier |
| `processing_timestamp` | Time the batch was processed |
| `record_status` | Processing status of the record |
| `watermark_value` | Value used to identify incremental records |
| `inserted_count` | Number of newly inserted records |
| `updated_count` | Number of updated records |

---

## ✅ Data Quality Metrics

The Data Quality and Reconciliation stage validates pipeline outputs.

Typical validation metrics include:

| Metric | Purpose |
|---|---|
| Source Record Count | Number of records received from source |
| Bronze Record Count | Number of records successfully ingested |
| Silver Record Count | Number of trusted transformed records |
| Gold Record Count | Number of analytics-ready records |
| Null Count | Number of missing required values |
| Duplicate Count | Number of duplicate records detected |
| Invalid Record Count | Records failing validation rules |
| Reconciliation Status | Indicates whether layer counts reconcile |

---

## 📋 Audit Dataset

Audit information provides operational visibility into pipeline execution.

Typical audit attributes include:

| Column | Description |
|---|---|
| `pipeline_name` | Name of the executed processing stage |
| `batch_id` | Processing batch identifier |
| `start_time` | Pipeline execution start time |
| `end_time` | Pipeline execution completion time |
| `status` | Execution result |
| `records_read` | Number of records read |
| `records_written` | Number of records written |
| `error_count` | Number of detected processing errors |

---

## 🔗 Data Lineage

The logical data lineage of the project is:

```text
Enterprise Source Data
        │
        ▼
Bronze Raw Data
        │
        ▼
Silver Trusted Data
        │
        ▼
Gold Analytical Data
        │
        ├── Sales Facts
        ├── Business KPIs
        ├── Customer Analytics
        └── Product Analytics
        │
        ▼
SQL / Power BI / Business Reporting
```

---

## 🛡️ Data Governance Principles

The project demonstrates Data Engineering practices designed around:

- Data traceability
- Data quality
- Layer separation
- Reconciliation
- Auditability
- Consistent business definitions
- Incremental processing
- Monitoring
- Analytics-ready data delivery

---

## 🎯 Purpose

This Data Dictionary provides a business and technical reference for the datasets processed within the **Enterprise Sales Lakehouse**.

It helps developers, Data Engineers, analysts, and reviewers understand how source information progresses from raw ingestion through trusted transformation and finally into business-ready analytical datasets.
