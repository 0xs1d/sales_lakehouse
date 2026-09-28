param location string
param storageAccountName string
param environment string

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
  tags: {
    project: 'enterprise-sales-lakehouse'
    environment: environment
    dataPlatform: 'adls-gen2'
  }
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Enabled'
    accessTier: 'Hot'
  }
}

var containers = [
  'source'
  'bronze'
  'silver'
  'gold'
  'audit'
  'quarantine'
  'checkpoints'
]

resource blobServices 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  name: 'default'
  parent: storage
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [for name in containers: {
  name: name
  parent: blobServices
  properties: {
    publicAccess: 'None'
  }
}]

output resourceId string = storage.id
output storageAccountName string = storage.name
output dfsEndpoint string = 'https://${storage.name}.dfs.core.windows.net'
