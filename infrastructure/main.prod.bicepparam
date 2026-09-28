using './main.bicep'

param location = 'eastus'
param resourceGroupName = 'rg-enterprise-sales-lakehouse-prod'
param environment = 'prod'
param storageAccountName = 'esaleslakehouseprod001'
param keyVaultName = 'kv-esales-lakehouse-prod'
param dataFactoryName = 'adf-enterprise-sales-prod'
param databricksWorkspaceName = 'dbw-enterprise-sales-prod'
param databricksSku = 'premium'
param enablePublicNetworkAccess = true
