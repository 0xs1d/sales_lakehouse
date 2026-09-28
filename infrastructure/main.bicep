targetScope = 'subscription'

@description('Azure subscription location for the resource group.')
param location string = deployment().location

@description('Resource group containing the Enterprise Sales Lakehouse resources.')
param resourceGroupName string

@description('Short environment name used in resource naming.')
@allowed([
  'dev'
  'test'
  'prod'
])
param environment string = 'dev'

@description('Globally unique storage account name. 3-24 lowercase alphanumeric characters.')
param storageAccountName string

@description('Globally unique Key Vault name.')
param keyVaultName string

@description('Globally unique Data Factory name.')
param dataFactoryName string

@description('Globally unique Databricks workspace name.')
param databricksWorkspaceName string

@description('Databricks pricing tier.')
@allowed([
  'standard'
  'premium'
  'trial'
])
param databricksSku string = 'premium'

@description('Whether the Databricks workspace should use public network access.')
param enablePublicNetworkAccess bool = true

resource resourceGroup 'Microsoft.Resources/resourceGroups@2022-09-01' = {
  name: resourceGroupName
  location: location
  tags: {
    project: 'enterprise-sales-lakehouse'
    environment: environment
    managedBy: 'bicep'
  }
}

module storage 'modules/storage.bicep' = {
  name: 'enterpriseSalesStorage'
  scope: resourceGroup
  params: {
    location: location
    storageAccountName: storageAccountName
    environment: environment
  }
}

module keyVault 'modules/key-vault.bicep' = {
  name: 'enterpriseSalesKeyVault'
  scope: resourceGroup
  params: {
    location: location
    keyVaultName: keyVaultName
    environment: environment
  }
}

module databricks 'modules/databricks.bicep' = {
  name: 'enterpriseSalesDatabricks'
  scope: resourceGroup
  params: {
    location: location
    workspaceName: databricksWorkspaceName
    sku: databricksSku
    enablePublicNetworkAccess: enablePublicNetworkAccess
    environment: environment
  }
}

module databricksAccessConnector 'modules/databricks-access-connector.bicep' = {
  name: 'enterpriseSalesDatabricksAccessConnector'
  scope: resourceGroup
  params: {
    location: location
    accessConnectorName: '${databricksWorkspaceName}-ac'
    environment: environment
  }
}

module dataFactory 'modules/data-factory.bicep' = {
  name: 'enterpriseSalesDataFactory'
  scope: resourceGroup
  params: {
    location: location
    factoryName: dataFactoryName
    environment: environment
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
  scope: resourceGroup
}

resource keyVaultResource 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: keyVaultName
  scope: resourceGroup
}

resource storageRoleForDataFactory 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, dataFactory.outputs.principalId, 'Storage Blob Data Contributor')
  scope: storageAccount
  properties: {
    principalId: dataFactory.outputs.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  }
}

resource storageRoleForDatabricks 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccount.id, databricksAccessConnector.outputs.principalId, 'Storage Blob Data Contributor')
  scope: storageAccount
  properties: {
    principalId: databricksAccessConnector.outputs.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  }
}

resource keyVaultRoleForDataFactory 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVaultResource.id, dataFactory.outputs.principalId, 'Key Vault Secrets User')
  scope: keyVaultResource
  properties: {
    principalId: dataFactory.outputs.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
  }
}

output resourceGroupId string = resourceGroup.id
output storageAccountId string = storage.outputs.resourceId
output storageAccountName string = storage.outputs.storageAccountName
output storageDfsEndpoint string = storage.outputs.dfsEndpoint
output keyVaultId string = keyVault.outputs.resourceId
output keyVaultUri string = keyVault.outputs.vaultUri
output databricksWorkspaceId string = databricks.outputs.resourceId
output databricksWorkspaceUrl string = databricks.outputs.workspaceUrl
output databricksAccessConnectorId string = databricksAccessConnector.outputs.resourceId
output dataFactoryId string = dataFactory.outputs.resourceId
output dataFactoryPrincipalId string = dataFactory.outputs.principalId
