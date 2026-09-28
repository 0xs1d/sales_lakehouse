param location string
param accessConnectorName string
param environment string

resource accessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: accessConnectorName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  tags: {
    project: 'enterprise-sales-lakehouse'
    environment: environment
    purpose: 'adls-managed-identity-access'
  }
}

output resourceId string = accessConnector.id
output principalId string = accessConnector.identity.principalId
