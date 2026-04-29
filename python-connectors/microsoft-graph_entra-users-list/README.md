# List Entra ID Users Connector

## Overview

This connector retrieves a comprehensive list of users from Microsoft Entra ID (formerly Azure Active Directory) using the Microsoft Graph API. It provides detailed user information including contact details, user type, account status, and sign-in activity metadata.

## Prerequisites

### Plugin Configuration
Before using this connector, ensure the following plugin-level parameters are configured in Dataiku DSS:

- **Tenant ID** - Your Microsoft Entra ID tenant ID (UUID format)
- **Client ID** - The client ID of your registered Azure application
- **Client Secret** - The client secret of your registered Azure application

### API Permissions
The registered Azure application requires the following Microsoft Graph API permissions:

- `User.Read.All` - Read all user profiles

### Authentication Flow
The connector uses OAuth 2.0 Client Credentials flow to authenticate with Microsoft Graph API using the credentials configured at the plugin level.

## Connector Configuration

This connector has no dataset-specific parameters. All configuration is done at the plugin level.


## Output Schema

The connector returns a dataset with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| businessPhones | Array | List of business phone numbers |
| displayName | String | Display name of the user |
| givenName | String | Given name (first name) of the user |
| id | String | Unique identifier (object ID) of the user in Entra ID |
| jobTitle | String | Job title of the user |
| mail | String | Primary email address |
| mobilePhone | String | Mobile phone number |
| officeLocation | String | Office location |
| preferredLanguage | String | Preferred language setting |
| surname | String | Family name (last name) of the user |
| userPrincipalName | String | User principal name (UPN) - typically username@domain |
| userType | String | Type of user (Member or Guest) |
| signInSessionsValidFromDateTime | String | ISO 8601 timestamp when sign-in sessions became valid |
| securityIdentifier | String | Security identifier (SID) |
| lastPasswordChangeDateTime | String | ISO 8601 timestamp of last password change |
| externalUserState | String | State of external user (if applicable) |
| createdDateTime | String | ISO 8601 timestamp when user account was created |
| companyName | String | Company name associated with the user |
| assignedLicenses | Array | Array of license assignments |
| accountEnabled | Boolean | Whether the user account is enabled |
| signInActivity | Object | Sign-in activity information |

## Usage Example

1. **Configure Plugin Settings** (if not already done)
   - Navigate to **Plugin settings**
   - Enter your Azure application credentials:
     - Tenant ID
     - Client ID
     - Client Secret
   - Click **Save**

2. **Create a new Dataset**
   - In Dataiku DSS, go to **Flow > New Dataset**
   - Select **Microsoft Graph API** plugin
   - Choose **List Entra ID users** connector

3. **Use in Recipes**
   - The dataset can be used as input to Python, SQL, or Visual recipes
   - Filter, transform, or export user data as needed

## Data Retrieval Details

### API Endpoint
```
GET https://graph.microsoft.com/v1.0/users
```

### Query Parameters
The connector requests the following fields via `$select`:
- businessPhones
- displayName
- givenName
- id
- jobTitle
- mail
- mobilePhone
- officeLocation
- preferredLanguage
- surname
- userPrincipalName
- userType
- signInSessionsValidFromDateTime
- securityIdentifier
- lastPasswordChangeDateTime
- externalUserState
- createdDateTime
- companyName
- assignedLicenses
- accountEnabled
- signInActivity

### Pagination
- **Page Size** - 500 records per request (Microsoft Graph API limit)
- **Max Pages** - Limited to 100,000 records by default
- The connector automatically follows `@odata.nextLink` pagination tokens


## Troubleshooting

### Common Issues

**Error: "Entra ID tenant ID is missing in plugin settings"**
- Solution: Configure the Tenant ID in the plugin settings

**Error: "OAuth2 application client ID is missing in plugin settings"**
- Solution: Configure the Client ID in the plugin settings

**Error: "OAuth2 application client secret is missing in plugin settings"**
- Solution: Configure the Client Secret in the plugin settings

**Error: "401 Unauthorized"**
- Ensure the Client ID and Client Secret are correct and still valid
- Verify the application is registered in your Entra ID tenant
- Confirm the application has the required API permissions (`User.Read.All`)

**Slow Performance**
- Large Entra ID tenants (10,000+ users) may take time to retrieve
- Consider filtering at the recipe level if you only need specific users


## Limitations

- This connector is read-only (no write capability)
- Does not support incremental updates (always retrieves full dataset)
- Some advanced user properties may not be included in the output
- Group membership information is not included (separate API endpoint required)

## Support

For issues or feature requests, please refer to:
- [Plugin Repository](https://github.com/datarsense/dss-plugin-microsoft-graph)
- [Microsoft Graph API Documentation](https://docs.microsoft.com/en-us/graph/api/overview)
- [Dataiku Plugin Documentation](https://doc.dataiku.com/dss/latest/plugins/index.html)
