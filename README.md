# DSS Plugin - Microsoft Graph API

This Dataiku DSS plugin provides python recipes to load data from Microsoft Graph API.

## Compatibility

* Dataiku DSS 12.0 or higher

## Connectors

### List Entra ID Users
Retrieves a comprehensive list of users from Microsoft Entra ID with detailed user information including contact details, account status, sign-in activity, and more. Can be used for user auditing, access reviews, and user management workflows.

For detailed usage instructions, see [List Entra ID Users Connector Documentation](./python-connectors/microsoft-graph_entra-users-list/README.md).

## Plugin configuration
The following parameter can be configured globally :
* **Tenant ID** : Microsoft Entra ID tenant ID
* **Client ID** : Microsoft Entra ID registered application client ID
* **Client secret** : Microsoft Entra ID registered application client secret
