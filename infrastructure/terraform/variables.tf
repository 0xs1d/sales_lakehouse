variable "databricks_workspace_url" {
  description = "Azure Databricks workspace URL."
  type        = string
}

variable "databricks_workspace_resource_id" {
  description = "Azure resource ID of the Databricks workspace."
  type        = string
}

variable "databricks_access_connector_id" {
  description = "Azure resource ID of the Databricks access connector."
  type        = string
}

variable "storage_account_name" {
  description = "ADLS Gen2 storage account name."
  type        = string
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}

variable "catalog_name" {
  description = "Unity Catalog name."
  type        = string
  default     = "enterprise_sales"
}

variable "schema_name" {
  description = "Unity Catalog schema name."
  type        = string
  default     = "sales"
}

variable "databricks_principal" {
  description = "Workspace user, group, or service principal receiving data permissions."
  type        = string
  default     = "enterprise-sales-job-principal"
}

variable "use_managed_identity" {
  description = "Use Azure managed identity authentication for the Databricks provider."
  type        = bool
  default     = true
}

variable "adls_containers" {
  description = "ADLS containers exposed as Unity Catalog external locations."
  type        = list(string)
  default     = ["source", "bronze", "silver", "gold", "audit", "quarantine", "checkpoints"]
}
