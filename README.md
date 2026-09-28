# 🌊 Enterprise Sales Lakehouse

![Azure](https://img.shields.io/badge/Microsoft_Azure-Data_Engineering-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?style=flat-square&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Apache_Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion_Architecture-00ADD8?style=flat-square)
![ADF](https://img.shields.io/badge/Azure_Data_Factory-Orchestration-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Analytics-336791?style=flat-square)
![Power BI](https://img.shields.io/badge/Power_BI-Reporting-F2C811?style=flat-square&logo=powerbi&logoColor=black)
[![Python CI](https://github.com/Gunasekaranravieng/Enterprise-sales-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Gunasekaranravieng/Enterprise-sales-lakehouse/actions/workflows/ci.yml)

### ⭐ Enterprise Data Engineering with Azure, Databricks, PySpark and modern Lakehouse architecture.

A production-grade Azure enterprise sales lakehouse demonstrating medallion architecture, Azure Data Factory, ADLS Gen2, Azure Databricks, Delta Lake, Unity Catalog, PySpark, incremental processing, data quality, auditability, and SQL analytics. 

---

## 📌 Project Overview

The **Enterprise Sales Lakehouse** models a complete enterprise sales data platform. It generates representative source data, processes it through Bronze, Silver, and Gold layers, validates quality, handles incremental changes, records audit information, and exposes business-ready analytical tables.

The solution follows the **Medallion Architecture**:

**Bronze → Silver → Gold**

The project focuses on practical Data Engineering concepts including:

- Data ingestion
- ETL / ELT pipelines
- Azure Data Factory orchestration
- Azure Data Lake Storage
- Databricks and PySpark transformations
- Delta Lake
- Medallion Architecture
- Incremental data processing
- Data quality validation
- Dimensional modelling
- Business KPI generation
- Audit logging
- Monitoring
- SQL analytics
- Power BI consumption

The repository contains Azure-native deployment artifacts and nine Azure Databricks PySpark notebooks. Cloud compilation and runtime execution require Azure, Databricks, Terraform, and Azure CLI access; static repository checks are included.

---

## 🎯 Business Scenario

An enterprise organization receives sales information from multiple operational sources.

The platform needs to process datasets such as:

- Customers
- Products
- Stores
- Orders
- Order Items
- Sales Transactions

Raw operational data may contain:

- Duplicate records
- Missing values
- Invalid data
- Incorrect data types
- Inconsistent formats
- Late-arriving records
- Updated business records

The platform addresses duplicates, missing and invalid values, inconsistent formats, updated records, late-arriving records, referential integrity, and layer-to-layer reconciliation.

---

## 🏗️ Solution Architecture

```text
                    ┌─────────────────────────┐
                    │   Enterprise Sources    │
                    │ CSV / Operational Data  │
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Azure Data Factory    │
                    │ Ingestion & Orchestration│
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │       ADLS Gen2         │
                    │     Raw Data Storage    │
                    └────────────┬────────────┘
                                 │
                                 ▼
                 ┌───────────────────────────────┐
                 │         BRONZE LAYER          │
                 │ Raw / Historical Data         │
                 └───────────────┬───────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │   Azure Databricks      │
                    │   PySpark Processing    │
                    └────────────┬────────────┘
                                 │
                                 ▼
                 ┌───────────────────────────────┐
                 │         SILVER LAYER          │
                 │ Cleaned / Validated Data      │
                 └───────────────┬───────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │       Delta Lake        │
                    │ Business Transformation │
                    └────────────┬────────────┘
                                 │
                                 ▼
                 ┌───────────────────────────────┐
                 │          GOLD LAYER           │
                 │ Analytics-Ready Data          │
                 └───────────────┬───────────────┘
                                 │
                         ┌───────┴────────┐
                         ▼                ▼
                        SQL            Power BI
                         │                │
                         └───────┬────────┘
                                 ▼
                         Business Analytics
```


## 🏛️ High-Level Design (HLD)

```text
Enterprise Sources
        │
        ▼
Azure Data Factory ── schedules, parameters, retries, dependencies
        │
        ▼
ADLS Gen2 ── source / bronze / silver / gold / audit / quarantine
        │
        ▼
Azure Databricks + Unity Catalog + Delta Lake
        │
        ├── Bronze: raw records and ingestion metadata
        ├── Silver: trusted, standardized, validated data
        └── Gold: facts, dimensions, KPIs, and aggregates
        │
        ▼
SQL analytics and downstream BI consumption
```

```mermaid
flowchart LR
    S[Enterprise Sources\nCSV / operational data] --> ADF[Azure Data Factory\nSchedules, parameters, retries]
    ADF --> ADLS[ADLS Gen2\nCloud lake storage]
    ADLS --> DBX[Azure Databricks\nPySpark + Delta Lake]
    KV[Azure Key Vault] -. secrets .-> ADF
    MI[Managed Identities + RBAC] -. access .-> ADLS
    UC[Unity Catalog] -. governance .-> DBX
    DBX --> B[Bronze\nRaw + metadata]
    B --> SL[Silver\nTrusted + validated]
    SL --> G[Gold\nFacts + dimensions + KPIs]
    G --> SQL[SQL Analytics]
    G --> BI[Downstream BI]
```

| Component | Responsibility |
|---|---|
| ADLS Gen2 | Cloud storage for lakehouse layers and operational files |
| Azure Data Factory | Bootstrap and incremental orchestration |
| Azure Databricks | Distributed PySpark and Delta processing |
| Unity Catalog | Catalog, schema, credentials, external locations, and grants |
| Azure Key Vault | Secret-management integration point |
| Managed identities | Passwordless Azure authentication |
| Bicep | Azure resource provisioning |
| Terraform | Databricks Unity Catalog provisioning |
| GitHub Actions | Syntax, contract, and data-quality validation |

## 🧱 Low-Level Design (LLD)

### Full/bootstrap pipeline

Defined in [pl_enterprise_sales_lakehouse.json](adf/pipelines/pl_enterprise_sales_lakehouse.json).

```text
01 Source Generation → 02 Bronze Ingestion → 03 Silver Transformation
→ 05 Incremental Merge → 04 Gold Analytics → 06 Quality/Reconciliation
→ 07 Audit/Monitoring → 08 Business Analytics → 09 Project Validation
```

```mermaid
flowchart TD
    F1[01 Source Generation] --> F2[02 Bronze Ingestion]
    F2 --> F3[03 Silver Transformation]
    F3 --> F5[05 Incremental Order and Item MERGE]
    F5 --> F4[04 Gold Analytics]
    F4 --> F6[06 Data Quality and Reconciliation]
    F6 --> F7[07 Audit and Monitoring]
    F7 --> F8[08 Performance and Business Analytics]
    F8 --> F9[09 Project Validation]

    F2 --> B[(Bronze Delta Tables)]
    F3 --> S[(Silver Delta Tables)]
    F4 --> G[(Gold Delta Tables)]
    F6 --> Q[(Quality Results)]
    F7 --> A[(Audit Tables)]
    F9 --> V{Validation score = 100%?}
    V -- Yes --> DONE[Pipeline succeeds]
    V -- No --> FAIL[Notebook raises failure]
```

### Daily incremental pipeline

Defined in [pl_enterprise_sales_lakehouse_incremental.json](adf/pipelines/pl_enterprise_sales_lakehouse_incremental.json). It does not regenerate source data.

```text
Incremental Source → 05 Order/Item MERGE → Rebuild Trusted Sales
→ 04 Gold and Date Dimension → 06 Quality → 07 Audit
→ 08 Analytics → 09 Validation
```

```mermaid
flowchart LR
    IS[(Incremental Source Delta)] --> O[Read new and changed orders]
    O --> OM{Delta MERGE on order_id}
    II[(Incremental Item Delta)] --> I[Read new and changed items]
    I --> IM{Delta MERGE on order_item_id}
    OM --> ST[Rebuild silver_trusted_sales]
    IM --> ST
    ST --> GF[Rebuild gold_fact_sales]
    GF --> DD[Rebuild gold_dim_date]
    GF --> DQ[Quality and reconciliation]
    DQ --> AUD[Audit and monitoring]
    AUD --> VAL[Final validation]
    VAL --> IDEM[Idempotent rerun check]
```


## 🥉 Bronze Layer — Raw Data

The Bronze layer is responsible for preserving source data with minimal transformation.

### Responsibilities

- Ingest source datasets
- Preserve original records
- Maintain historical information
- Add ingestion metadata
- Track source information
- Support downstream reprocessing

### Planned Bronze Tables

```text
bronze_customers
bronze_products
bronze_stores
bronze_orders
bronze_order_items
```

Typical metadata fields:

```text
ingestion_timestamp
source_file
batch_id
pipeline_run_id
```

---

## 🥈 Silver Layer — Clean & Validated Data

The Silver layer converts Bronze data into standardized and trusted enterprise datasets.

### Processing

- Schema validation
- Data type conversion
- Duplicate removal
- Null handling
- Date standardization
- String normalization
- Invalid-record handling
- Business-rule validation
- Derived columns
- Referential integrity checks

### Planned Silver Tables

```text
silver_customers
silver_products
silver_stores
silver_orders
silver_order_items
```

### Example PySpark Transformation

```python
from pyspark.sql import functions as F

clean_orders = (
    orders_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["order_id"])
    .withColumn(
        "order_date",
        F.to_date(F.col("order_date"))
    )
)
```

---

## 🥇 Gold Layer — Analytics-Ready Data

The Gold layer contains curated datasets optimized for reporting and business analytics.

The planned Gold model follows dimensional modelling principles.

### Dimension Tables

```text
dim_customer
dim_product
dim_store
dim_date
```

### Fact Table

```text
fact_sales
```

### Star Schema

```text
                 ┌─────────────────┐
                 │  dim_customer   │
                 └────────┬────────┘
                          │
                          │
┌───────────────┐         │         ┌───────────────┐
│  dim_product  │─────────┼─────────│   dim_store   │
└───────────────┘         │         └───────────────┘
                          │
                   ┌──────▼──────┐
                   │ fact_sales  │
                   └──────┬──────┘
                          │
                 ┌────────▼────────┐
                 │    dim_date     │
                 └─────────────────┘
```

---

## 🔄 Incremental Processing

The architecture is designed to support incremental data processing instead of reprocessing the complete dataset during every pipeline execution.

```text
Source
   │
   ▼
Watermark / Last Processed Value
   │
   ▼
Identify New or Updated Records
   │
   ▼
ADF Incremental Ingestion
   │
   ▼
Bronze Layer
   │
   ▼
PySpark Transformation
   │
   ▼
Delta MERGE
   │
   ▼
Silver / Gold
```

### Delta MERGE Concept

```sql
MERGE INTO silver_orders AS target
USING staging_orders AS source
ON target.order_id = source.order_id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
```

This pattern supports both newly created and updated business records.

---

## 🧪 Data Quality Framework

The solution includes a planned Data Quality framework to prevent invalid records from entering trusted datasets.

### Validation Rules

| Check | Purpose |
|---|---|
| Null Check | Validate mandatory fields |
| Duplicate Check | Prevent duplicate business records |
| Type Check | Validate expected data types |
| Range Check | Detect invalid numerical values |
| Date Check | Validate transaction dates |
| Referential Check | Validate entity relationships |
| Business Rule Check | Detect invalid business records |

### Example

```python
valid_sales = (
    sales_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") >= 0)
    .dropDuplicates(["order_id", "product_id"])
)
```

Invalid records can be isolated for investigation rather than silently entering curated datasets.

---

## 🚨 Error Handling & Quarantine

The architecture separates valid and invalid records.

```text
Incoming Data
      │
      ▼
   Validation
    ┌──┴──┐
    │     │
  Valid Invalid
    │     │
    ▼     ▼
 Silver  Quarantine
```

Potential failure scenarios include:

- Source unavailable
- Invalid schema
- Corrupted records
- Transformation failure
- Data quality failure
- Storage failure
- Notebook failure

---

## 📋 Audit and Monitoring

Audit tables are `audit_layer_metrics`, `audit_pipeline_runs`, and `audit_monitoring_health`.

Audit output contains pipeline stages, environment context, pipeline run IDs, layer counts, duplicate checks, null checks, negative-sales checks, and monitoring status.

## ☁️ Azure Infrastructure

The [infrastructure directory](infrastructure/) contains modular Azure deployment definitions.

### Bicep resources

- Resource group
- ADLS Gen2 StorageV2 account
- Source, Bronze, Silver, Gold, audit, quarantine, and checkpoint containers
- RBAC-enabled Azure Key Vault
- Azure Databricks workspace
- Databricks access connector with managed identity
- System-assigned-identity Azure Data Factory
- Storage Blob Data Contributor assignments
- Key Vault Secrets User assignment for ADF

Deploy with:

```bash
az login
az account set --subscription <subscription-id>
bash scripts/deploy-azure.sh infrastructure/main.dev.bicepparam
```

Production parameters are available in [main.prod.bicepparam](infrastructure/main.prod.bicepparam).

### Unity Catalog and ADLS authorization

The [Terraform module](infrastructure/terraform/) creates the `enterprise_sales` catalog, `sales` schema, ADLS managed-identity storage credential, layer external locations, and data grants.

```bash
cd infrastructure/terraform
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

The example variables intentionally contain placeholder workspace and principal values and must be replaced before deployment.


## 🧱 Azure Databricks Bundle

The [Databricks bundle](databricks/) defines development and production targets, a job cluster, Unity Catalog parameters, Azure storage parameters, notebook dependencies, and rerun control.

```bash
cd databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev enterprise_sales_lakehouse
```

## ⚙️ Azure Data Factory Orchestration

Azure Data Factory is designed to act as the orchestration layer.

### Planned Pipeline Flow

```text
Lookup Configuration
        │
        ▼
Get Metadata
        │
        ▼
Copy Activity
        │
        ▼
Bronze Storage
        │
        ▼
Databricks Notebook
        │
        ▼
Silver Transformation
        │
        ▼
Gold Transformation
        │
        ▼
Audit / Monitoring
```

ADF responsibilities include:

- Source ingestion
- Pipeline orchestration
- Parameterization
- Incremental-load control
- Databricks notebook execution
- Dependency management
- Failure handling
- Pipeline monitoring

---

## 📋 Audit & Monitoring

A production-oriented Data Engineering pipeline should capture operational metadata for every execution.

### Planned Audit Fields

```text
pipeline_name
pipeline_run_id
batch_id
source_name
start_timestamp
end_timestamp
records_read
records_written
records_rejected
pipeline_status
error_message
```

Typical status values:

```text
STARTED
SUCCESS
FAILED
```

This provides traceability across pipeline executions.

---

## 📊 Business KPIs

The Gold layer is designed to support analytics such as:

### Sales KPIs

- Total Revenue
- Total Orders
- Total Units Sold
- Average Order Value
- Average Selling Price

### Product KPIs

- Revenue by Product
- Units Sold by Product
- Top Performing Products
- Product Category Performance

### Customer KPIs

- Revenue by Customer
- Customer Purchase Frequency
- Customer Order Value
- Repeat Customer Analysis

### Regional KPIs

- Revenue by Store
- Revenue by Region
- Store Performance
- Regional Sales Contribution

### Time-Based KPIs

- Daily Revenue
- Monthly Revenue
- Month-over-Month Growth
- Year-over-Year Analysis

---


## 🧰 CI and Automated Checks

GitHub Actions validates notebook syntax, required artifacts, ADF JSON, repository contracts, SQL table contracts, data-quality rules, ADF notebook coverage, and Gold date-dimension presence.

```bash
python -m unittest discover --start-directory tests --pattern 'test_*.py' --verbose
```

## 🔐 Security and Operations

A production Azure implementation should follow security practices such as:

- Azure Key Vault for secrets
- Managed Identities
- Role-Based Access Control
- Least-privilege access
- Secure ADLS permissions
- Databricks secret management
- No credentials in source code


Before production use, review VNet injection, private endpoints, firewall policy, diagnostic settings, Log Analytics, alerting, service-principal permissions, and least-privilege access.

## 📈 Power BI Consumption

Gold-layer datasets are designed for downstream analytics and reporting.

### Planned Dashboard

```text
Executive Sales Overview
        │
        ├── Total Revenue
        ├── Total Orders
        ├── Average Order Value
        ├── Monthly Revenue Trend
        ├── Top Products
        ├── Regional Performance
        └── Customer Performance
```

---

## ⚡ Performance Considerations

The Lakehouse design considers techniques such as:

- Incremental processing
- Delta Lake MERGE
- Partition pruning
- Predicate filtering
- Efficient Spark transformations
- Appropriate file sizing
- Broadcast joins where appropriate
- Caching only where beneficial
- Optimized Gold aggregations


The project is implementation-complete at repository level. Remaining work is environment-specific Azure deployment and runtime validation.

# 📸 Execution Results

The following screenshots provide execution evidence from the implemented **Enterprise Sales Lakehouse** workflow in Databricks.

They demonstrate source generation, Bronze ingestion, Silver transformations, Gold analytics, incremental processing, data quality validation, monitoring, performance analysis, and final end-to-end validation.

**Implementation note:** The processing workflow was executed using Databricks. Azure Data Factory and ADLS Gen2 are represented as the target Azure production architecture and are not presented as executed cloud components.

## 1. Source Data Generation

Representative enterprise sales datasets were generated and validated before entering the Medallion Architecture.

![Customer Source Data](screenshots/01_customer_source_data.png)

![Source Data Quality Validation](screenshots/02_source_data_quality_validation.png)

---

## 2. Bronze Layer — Raw Ingestion

The Bronze layer preserves source records and adds ingestion metadata for traceability.

![Bronze Orders with Metadata](screenshots/04_bronze_orders_with_metadata.png)

![Bronze Reconciliation](screenshots/05_bronze_reconciliation.png)

![Bronze Ingestion Summary](screenshots/06_bronze_ingestion_summary.png)

---

## 3. Silver Layer — Transformation & Trusted Data

Bronze data is cleaned, standardized, validated, and transformed into trusted Silver datasets.

![Silver Orders Transformed](screenshots/07_silver_orders_transformed.png)

![Silver Trusted Sales](screenshots/08_silver_trusted_sales.png)

![Silver Data Quality](screenshots/09_silver_data_quality.png)

![Silver Transformation Summary](screenshots/10_silver_transformation_summary.png)

---

## 4. Gold Layer — Business Analytics

The Gold layer provides analytics-ready sales facts, KPIs, and regional business insights.

![Gold Fact Sales](screenshots/11_gold_fact_sales.png)

![Gold Business KPIs](screenshots/12_gold_business_kpis.png)

![Gold Regional Performance](screenshots/13_gold_regional_performance.png)

![Gold Analytics Summary](screenshots/14_gold_analytics_summary.png)

---

## 5. Incremental Processing — Delta MERGE

Incremental processing demonstrates insert/update handling and idempotent Delta Lake MERGE behavior.

![Incremental Sales Batch](screenshots/15_incremental_sales_batch.png)

![Incremental MERGE Validation](screenshots/16_incremental_merge_validation.png)

![Incremental Processing Summary](screenshots/17_incremental_processing_summary.png)

---

## 6. Enterprise Data Quality & Reconciliation

Cross-layer validation verifies data quality, reconciliation, and referential integrity.

![Enterprise Data Quality Checks](screenshots/18_enterprise_data_quality_checks.png)

![Layer Reconciliation](screenshots/19_layer_reconciliation.png)

![Data Quality Validation Summary](screenshots/20_data_quality_validation_summary.png)

---

## 7. Audit & Monitoring

Operational metrics and health checks provide visibility into pipeline execution and data health.

![Layer Audit Metrics](screenshots/21_layer_audit_metrics.png)

![Pipeline Health Monitoring](screenshots/22_pipeline_health_monitoring.png)

![Audit Monitoring Summary](screenshots/23_audit_monitoring_summary.png)

---

## 8. Performance & Business Analytics

Spark-based analytical processing demonstrates optimized aggregation, execution planning, and business analytics.

![Optimized Daily Sales](screenshots/24_optimized_daily_sales.png)

![Spark Execution Plan](screenshots/25_spark_execution_plan.png)

![Performance Analytics Summary](screenshots/26_performance_analytics_summary.png)

---

## 9. End-to-End Project Validation

The final validation verifies required Lakehouse tables, processing stages, data integrity, monitoring health, and overall project completion.

![Required Tables Validation](screenshots/27_required_tables_validation.png)

![Final Validation Matrix](screenshots/28_final_validation_matrix.png)

![Project Validation](screenshots/29_project_validation_100_percent.png)

---


## 📁 Repository Structure

```text
Enterprise-sales-lakehouse/
├── adf/                 # ADF linked services, datasets, pipelines, triggers
├── databricks/          # Azure Databricks bundle and job resources
├── infrastructure/      # Bicep and Unity Catalog Terraform
├── notebooks/           # PySpark/Delta processing notebooks
├── scripts/             # Azure and ADF deployment scripts
├── sql/                 # Business analytics SQL
├── tests/               # CI contract and data-quality tests
├── docs/                # Architecture, dictionary, flow, deployment docs
├── screenshots/         # Execution snapshots
├── requirements.txt
├── LICENSE
├── README.md            
```

## 👨‍💻 Author

### Sidharth Gupta

**Azure Data Engineer | Databricks | PySpark | Azure Data Factory | Delta Lake | SQL**


💼 **LinkedIn:**  [Sidharth Gupta](https://www.linkedin.com/in/netsid/)
