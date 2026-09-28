# Azure Databricks deployment bundle

This directory contains a Databricks Declarative Automation Bundle for the Azure Databricks workspace provisioned by `infrastructure/main.bicep`.

The job deploys the existing notebooks in dependency order:

`01 → 02 → 03 → 05 → 04 → 06 → 07 → 08 → 09`

Incremental processing runs before Gold generation so Gold is rebuilt after the Silver merge.

Before deployment, replace the workspace URL and storage-account placeholders through target variables or command-line variable overrides. Deploy with the Azure Databricks CLI from this directory:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev enterprise_sales_lakehouse
```

The bundle is Azure Databricks-native. It does not run Spark locally or substitute another execution engine.

The `catalog` and `schema` job parameters default to `enterprise_sales.sales`. Apply `infrastructure/terraform` before running the bundle with those defaults.
