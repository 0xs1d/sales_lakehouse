param location string
param workspaceName string
param sku string
param enablePublicNetworkAccess bool
param environment string

resource workspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: workspaceName
  location: location
  sku: {
    name: sku
  }
  tags: {
    project: 'enterprise-sales-lakehouse'
    environment: environment
  }
  properties: {
    managedResourceGroupId: resourceId('Microsoft.Resources/resourceGroups', '${workspaceName}-managed')
    publicNetworkAccess: enablePublicNetworkAccess ? 'Enabled' : 'Disabled'
    requiredNsgRules: 'AllRules'
  }
}

output resourceId string = workspace.id
output workspaceUrl string = workspace.properties.workspaceUrl
