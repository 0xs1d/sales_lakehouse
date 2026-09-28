# Unity Catalog and ADLS authorization

This Terraform module is the Databricks-native layer that Bicep cannot provision directly. It creates the Unity Catalog catalog/schema, registers the Bicep-created Databricks access connector as an ADLS storage credential, exposes each ADLS layer as an external location, and grants the configured Databricks principal access.

Run it only after the Bicep deployment has completed and produced the workspace, storage account, and access connector resource IDs:

```bash
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

The example file contains dummy subscription/resource values by design. Replace them with the outputs from the Bicep deployment before applying.
