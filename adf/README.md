# Azure Data Factory artifacts

This directory contains Azure-native Data Factory definitions for the Enterprise Sales Lakehouse.

## Deployment order

1. Deploy `infrastructure/main.bicep` to create the resource group, ADLS Gen2 account, Key Vault, Azure Databricks workspace, and Data Factory.
2. Upload the notebooks to the configured Databricks workspace path.
3. Create the linked services from `linkedServices/`.
4. Create the dataset from `datasets/`.
5. Create `pipelines/pl_enterprise_sales_lakehouse.json` for the bootstrap/full run.
6. Create `pipelines/pl_enterprise_sales_lakehouse_incremental.json` for normal daily runs.
7. Create the trigger from `triggers/` after supplying environment-specific pipeline parameters.

The pipeline deliberately runs incremental processing before Gold generation so the Gold layer is rebuilt after the Silver merge. The source-generation notebook currently creates the project's representative source tables in Databricks; the ADLS landing linked service is provided for future file-based source ingestion and quarantine paths.

The daily trigger points to the incremental pipeline so normal runs do not regenerate source data. Run the full pipeline once to bootstrap the catalog tables.

ADF managed identity access to ADLS is granted by the Bicep deployment through the Storage Blob Data Contributor role. Production environments should narrow this role and use private endpoints/network rules before go-live.
