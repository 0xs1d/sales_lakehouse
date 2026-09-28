param location string
param factoryName string
param environment string

resource factory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: factoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  tags: {
    project: 'enterprise-sales-lakehouse'
    environment: environment
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

output resourceId string = factory.id
output principalId string = factory.identity.principalId
output factoryName string = factory.name
