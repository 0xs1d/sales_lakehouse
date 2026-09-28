# Azure deployment design

The repository now contains Azure-native deployment artifacts in four modules:

```text
infrastructure/
  main.bicep
  main.dev.bicepparam
  main.prod.bicepparam
  modules/
    storage.bicep
    key-vault.bicep
    databricks.bicep
    data-factory.bicep
  terraform/
    databricks-unity-catalog.tf
    variables.tf
    dev.tfvars.example

adf/
  linkedServices/
  datasets/
  pipelines/
  triggers/

databricks/
  databricks.yml
  resources/jobs.yml

scripts/
  deploy-azure.sh
  deploy-adf.sh
```

## Provisioned Azure resources

The Bicep entry point provisions:

- A resource group
- ADLS Gen2-enabled StorageV2 account
- Source, Bronze, Silver, Gold, audit, quarantine, and checkpoint containers
- RBAC-enabled Azure Key Vault
- Azure Databricks workspace
- Azure Databricks access connector with managed identity
- System-assigned-identity Azure Data Factory
- Storage Blob Data Contributor access for the Data Factory identity and Databricks access connector
- Key Vault Secrets User access for the Data Factory identity

The Terraform layer creates the Unity Catalog catalog/schema, ADLS storage credential, external locations, and grants required by the notebooks.

## Deployment

```bash
az login
az account set --subscription <subscription-id>
bash scripts/deploy-azure.sh infrastructure/main.dev.bicepparam
```

Then configure the deployed workspace URL and storage account name in the Databricks bundle and deploy it:

```bash
cd databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

The first run is a bootstrap/full run. Subsequent daily runs target `pl_enterprise_sales_lakehouse_incremental`, which starts at notebook 05, rebuilds Gold after the merge, and finishes with quality, monitoring, analytics, and validation.

Apply the Databricks Unity Catalog layer after Bicep and before running the notebooks:

```bash
cd infrastructure/terraform
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

Create the ADF objects after the resource deployment:

```bash
bash scripts/deploy-adf.sh \
  rg-enterprise-sales-lakehouse-dev \
  adf-enterprise-sales-dev \
  esaleslakehousedev001 \
  /subscriptions/<subscription-id>/resourceGroups/rg-enterprise-sales-lakehouse-dev/providers/Microsoft.Databricks/workspaces/dbw-enterprise-sales-dev \
  https://<workspace-url> \
  kv-esales-lakehouse-dev
```

## Important deployment-time work

The templates intentionally do not contain credentials or subscription-specific values. The deployment operator must provide:

- Azure subscription and tenant access
- Globally unique resource names
- Databricks workspace URLs
- Databricks principal or service-principal name for Unity Catalog grants
- ADF pipeline parameter values
- Network and private-endpoint policy decisions
- Production RBAC review

Before enabling `adf/triggers/tr_enterprise_sales_daily.json`, replace its three `__...__` values with the deployed storage account name, Databricks workspace resource ID, and Databricks workspace URL.

The notebooks now consume Azure job parameters, set the configured Unity Catalog context, and write Delta tables to environment-scoped ADLS paths when a storage account is supplied.
