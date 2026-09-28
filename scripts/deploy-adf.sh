#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 6 ]]; then
  echo "Usage: $0 <resource-group> <data-factory-name> <storage-account-name> <databricks-workspace-resource-id> <databricks-workspace-url> <key-vault-name>" >&2
  exit 1
fi

RESOURCE_GROUP="$1"
DATA_FACTORY_NAME="$2"
STORAGE_ACCOUNT_NAME="$3"
DATABRICKS_WORKSPACE_RESOURCE_ID="$4"
DATABRICKS_WORKSPACE_URL="$5"
KEY_VAULT_NAME="$6"

create_linked_service() {
  local definition="$1"
  az datafactory linked-service create \
    --resource-group "$RESOURCE_GROUP" \
    --factory-name "$DATA_FACTORY_NAME" \
    --name "$(basename "$definition" .json)" \
    --properties "@$definition"
}

create_dataset() {
  local definition="$1"
  az datafactory dataset create \
    --resource-group "$RESOURCE_GROUP" \
    --factory-name "$DATA_FACTORY_NAME" \
    --name "$(basename "$definition" .json)" \
    --properties "@$definition"
}

create_linked_service adf/linkedServices/ls_adls_gen2.json
create_linked_service adf/linkedServices/ls_azure_databricks.json
create_linked_service adf/linkedServices/ls_azure_key_vault.json
create_dataset adf/datasets/ds_adls_landing_binary.json

az datafactory pipeline create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY_NAME" \
  --name pl_enterprise_sales_lakehouse \
  --pipeline "@adf/pipelines/pl_enterprise_sales_lakehouse.json"

az datafactory pipeline create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY_NAME" \
  --name pl_enterprise_sales_lakehouse_incremental \
  --pipeline "@adf/pipelines/pl_enterprise_sales_lakehouse_incremental.json"

TRIGGER_FILE="$(mktemp)"
trap 'rm -f "$TRIGGER_FILE"' EXIT
sed \
  -e "s|__STORAGE_ACCOUNT_NAME__|$STORAGE_ACCOUNT_NAME|g" \
  -e "s|__DATABRICKS_WORKSPACE_RESOURCE_ID__|$DATABRICKS_WORKSPACE_RESOURCE_ID|g" \
  -e "s|__DATABRICKS_WORKSPACE_URL__|$DATABRICKS_WORKSPACE_URL|g" \
  adf/triggers/tr_enterprise_sales_daily.json > "$TRIGGER_FILE"

az datafactory trigger create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY_NAME" \
  --name tr_enterprise_sales_daily \
  --properties "@$TRIGGER_FILE"

az datafactory trigger start \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$DATA_FACTORY_NAME" \
  --name tr_enterprise_sales_daily

echo "ADF pipeline created. Configure workspace and environment parameters before triggering it."
echo "Storage account: $STORAGE_ACCOUNT_NAME"
echo "Key Vault: $KEY_VAULT_NAME"
