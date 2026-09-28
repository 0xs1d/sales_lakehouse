using './main.bicep'

param location = 'eastus'
param resourceGroupName = 'rg-enterprise-sales-lakehouse-dev'
param environment = 'dev'
param storageAccountName = 'esaleslakehousedev001'
param keyVaultName = 'kv-esales-lakehouse-dev'
param dataFactoryName = 'adf-enterprise-sales-dev'
param databricksWorkspaceName = 'dbw-enterprise-sales-dev'
param databricksSku = 'premium'
param enablePublicNetworkAccess = true
