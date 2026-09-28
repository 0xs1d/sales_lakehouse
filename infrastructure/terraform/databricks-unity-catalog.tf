terraform {
  required_version = ">= 1.6.0"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

provider "databricks" {
  host                        = var.databricks_workspace_url
  azure_workspace_resource_id = var.databricks_workspace_resource_id
  azure_use_msi               = var.use_managed_identity
}

resource "databricks_catalog" "enterprise_sales" {
  name    = var.catalog_name
  comment = "Enterprise Sales Lakehouse catalog"
}

resource "databricks_schema" "sales" {
  catalog_name = databricks_catalog.enterprise_sales.name
  name         = var.schema_name
  comment      = "Curated enterprise sales data and audit tables"
}

resource "databricks_storage_credential" "adls_access_connector" {
  name = "${var.catalog_name}-adls-access"

  azure_managed_identity {
    access_connector_id = var.databricks_access_connector_id
  }

  comment = "Azure Databricks access connector for ADLS Gen2"
}

resource "databricks_external_location" "layer" {
  for_each = toset(var.adls_containers)

  name            = "${var.catalog_name}-${each.value}-location"
  url             = "abfss://${each.value}@${var.storage_account_name}.dfs.core.windows.net/${var.environment}"
  credential_name = databricks_storage_credential.adls_access_connector.name
  comment         = "${each.value} layer location for ${var.environment}"
}

resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.enterprise_sales.name

  grant {
    principal  = var.databricks_principal
    privileges = ["USE_CATALOG", "CREATE_SCHEMA"]
  }
}

resource "databricks_grants" "schema" {
  schema = "${databricks_catalog.enterprise_sales.name}.${databricks_schema.sales.name}"

  grant {
    principal  = var.databricks_principal
    privileges = ["USE_SCHEMA", "CREATE_TABLE", "MODIFY"]
  }
}

resource "databricks_grants" "external_location" {
  for_each = databricks_external_location.layer

  external_location = each.value.name

  grant {
    principal  = var.databricks_principal
    privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE"]
  }
}
