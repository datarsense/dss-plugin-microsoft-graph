# Generic Microsoft Graph API Query Connector

## Overview

This connector provides flexible, generic access to any Microsoft Graph API endpoint (v1.0 or beta). Unlike specific connectors tailored to particular endpoints, this connector allows you to query any endpoint with fully customizable parameters including HTTP method, OData query parameters, custom headers, and request body.

Use this connector when:
- You need to query Microsoft Graph endpoints not covered by specific connectors
- You want to use advanced OData query parameters ($filter, $search, etc.)
- You need to perform POST, PATCH, or DELETE operations
- You want to explore Microsoft Graph data with flexible filtering and field selection

## Prerequisites

### Plugin Configuration
Before using this connector, ensure the following plugin-level parameters are configured in Dataiku DSS:

- **Tenant ID** - Your Microsoft Entra ID (Azure AD) tenant ID (UUID format, e.g., `12345678-1234-1234-1234-123456789012`)
- **Client ID** - The client ID of your registered Azure application
- **Client Secret** - The client secret of your registered Azure application

For instructions on registering an Azure application, see [Dataiku Plugin Configuration](#plugin-configuration-setup).

### API Permissions
The registered Azure application requires appropriate Microsoft Graph API permissions based on the endpoint you're querying. Common permissions include:

- `User.Read.All` - Read all user profiles
- `Group.Read.All` - Read group information
- `Device.Read.All` - Read device information
- `Mail.Read` - Read user mailboxes
- `Directory.Read.All` - Read directory data
- And many others depending on your specific endpoint needs

Refer to [Microsoft Graph Permissions Reference](https://learn.microsoft.com/en-us/graph/permissions-reference) for the specific permissions required by each endpoint.

### Authentication Flow
The connector uses OAuth 2.0 Client Credentials flow to authenticate with Microsoft Graph API using credentials configured at the plugin level.

## Connector Configuration

### Required Parameters

#### API Version
- **Label:** API Version
- **Type:** Dropdown
- **Options:** `v1.0` (Production) or `beta` (Preview)
- **Default:** v1.0
- **Description:** Select the Microsoft Graph API version to use. Use `v1.0` for stable production endpoints, or `beta` for preview features (note: beta endpoints may change).

#### API Endpoint
- **Label:** API Endpoint
- **Type:** Text
- **Mandatory:** Yes
- **Description:** The API endpoint path to query. Do NOT include the base URL (`https://graph.microsoft.com`) or API version.

**Examples:**
- `/users` - List all users
- `/groups` - List all groups
- `/groups/{id}/members` - Get members of a specific group
- `/devices` - List all devices
- `/me` - Get current user information
- `/me/drive/root/children` - Get files in user's OneDrive root
- `/me/messages` - Get user's emails
- `/reports/authenticationMethods/userRegistrationDetails` - Get authentication method registrations

### Optional Parameters

#### HTTP Method
- **Label:** HTTP Method
- **Type:** Dropdown
- **Options:** `GET` (default), `POST`, `PATCH`, `DELETE`
- **Default:** GET
- **Description:** The HTTP method for the request. Use GET for data retrieval, POST for creation/execution, PATCH for updates, DELETE for removal.

#### $select - Select Fields
- **Label:** $select - Select Fields
- **Type:** Text
- **Description:** OData `$select` parameter: specify which fields to include in the response. Comma-separated list of property names.

**Examples:**
- `id,displayName,mail` - Include only ID, display name, and email
- `id,displayName,jobTitle,mail,department` - Multiple fields
- `*` - Include all fields (not recommended due to performance)

**Benefits:**
- Reduces response size and network bandwidth
- Improves performance by requesting only needed data
- Works on most endpoints

#### $filter - Filter Expression
- **Label:** $filter - Filter Expression
- **Type:** Text
- **Description:** OData `$filter` parameter: filter results based on conditions. Supports various operators and functions.

**Examples:**
- `accountEnabled eq true` - Only enabled user accounts
- `startswith(displayName, 'John')` - Names starting with "John"
- `department eq 'Engineering'` - Specific department
- `mail ne null` - Users with email addresses
- `createdDateTime ge 2024-01-01T00:00:00Z` - Created after specific date
- `(accountEnabled eq true) and (userType eq 'Member')` - Multiple conditions

**Supported Operators:**
- `eq` - Equal
- `ne` - Not equal
- `gt` - Greater than
- `ge` - Greater than or equal
- `lt` - Less than
- `le` - Less than or equal
- `startswith()` - String starts with
- `endswith()` - String ends with
- `contains()` - String contains
- `and` - Logical AND
- `or` - Logical OR
- `not()` - Logical NOT

**Note:** Not all endpoints support $filter. Check Microsoft Graph documentation for endpoint-specific support.

#### $top - Max Records Per Request
- **Label:** $top - Max Records Per Request
- **Type:** Integer
- **Description:** OData `$top` parameter: maximum number of records to retrieve per API request. Limits page size for pagination.

**Examples:**
- `10` - Retrieve 10 records per page
- `50` - Retrieve 50 records per page
- `500` - Maximum allowed for most endpoints

**Notes:**
- Different endpoints have different maximum $top values (typically 1-500)
- Lower values result in more API requests but less memory usage
- Higher values retrieve more data per request but use more bandwidth
- The connector will automatically paginate through all results

#### $search - Search Term
- **Label:** $search - Search Term
- **Type:** Text
- **Description:** OData `$search` parameter: full-text search capability. Only supported on specific endpoints.

**Examples:**
- `"John"` - Search for "John" in searchable fields
- `"john@contoso.com"` - Search for email address

**Supported Endpoints:**
- `/users` - Search by displayName, mail, mailNickname, givenName, surname
- `/groups` - Search by displayName, description
- `/messages` - Search in subject, body
- Other endpoints may also support $search

**Note:** $search is limited to specific endpoints; check documentation before using.

#### Custom HTTP Headers
- **Label:** Custom HTTP Headers
- **Type:** Text (JSON)
- **Description:** Additional HTTP headers to include in requests. Provide as a JSON object.

**Examples:**
- `{"Prefer": "outlook.timezone=UTC"}` - Request results in UTC timezone
- `{"Prefer": "return=representation"}` - Return updated resource after POST/PATCH
- `{"ConsistencyLevel": "eventual"}` - For advanced query scenarios

**Common Headers:**
- `Prefer` - Custom preferences (timezone, response format, etc.)
- `ConsistencyLevel` - Set to "eventual" for some advanced queries
- `Content-Type` - Usually set automatically; override if needed

#### Request Body (POST/PATCH)
- **Label:** Request Body (POST/PATCH)
- **Type:** Text (JSON)
- **Description:** JSON body for POST or PATCH requests. Required for POST/PATCH/DELETE methods that need to send data.

**Examples for Creating a Group:**
```json
{
  "displayName": "Engineering Team",
  "description": "Engineering department members",
  "mailEnabled": false,
  "securityEnabled": true
}
```

**Examples for Updating a User:**
```json
{
  "jobTitle": "Senior Engineer",
  "department": "Engineering"
}
```

**Examples for Sending Email:**
```json
{
  "message": {
    "subject": "Status Update",
    "body": {
      "contentType": "text",
      "content": "This is the email body"
    },
    "toRecipients": [
      {
        "emailAddress": {
          "address": "user@contoso.com"
        }
      }
    ]
  }
}
```

## Output Schema

The output schema is **dynamic** and depends on the API endpoint queried and the `$select` parameter used.

**Response Format:**
- For endpoints returning a collection: Records from the API response's `value` field
- For endpoints returning a single object: The entire response object
- Each record is a JSON object with properties corresponding to the selected fields

**Example Output for `/users` with `$select: id,displayName,mail`:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "displayName": "John Doe",
  "mail": "john@contoso.com"
}
```

### Discovering Output Fields

To discover available fields for an endpoint:

1. **Check Microsoft Graph Documentation:** Visit [Microsoft Graph API Reference](https://learn.microsoft.com/en-us/graph/api/overview)
2. **Query without $select:** Leave the `$select` parameter empty to retrieve all available fields
3. **Inspect Sample Data:** Create a dataset and preview it to see the returned fields
4. **Use Graph Explorer:** Visit [Graph Explorer](https://developer.microsoft.com/en-us/graph/graph-explorer) to test queries

## Usage Examples

### Example 1: List All Active Users with Specific Fields

**Configuration:**
- API Version: `v1.0`
- API Endpoint: `/users`
- HTTP Method: `GET`
- $select: `id,displayName,mail,jobTitle,department`
- $filter: `accountEnabled eq true`

**Result:** Active users with selected fields, filtered for enabled accounts.

### Example 2: Search for Devices with Specific Properties

**Configuration:**
- API Version: `v1.0`
- API Endpoint: `/devices`
- HTTP Method: `GET`
- $select: `id,displayName,operatingSystem,lastSignInDateTime`
- $filter: `startswith(displayName, 'DESKTOP')`

**Result:** Devices with display names starting with "DESKTOP".

### Example 3: List Group Members

**Configuration:**
- API Version: `v1.0`
- API Endpoint: `/groups/{group-id}/members`
- HTTP Method: `GET`
- $select: `id,displayName,userPrincipalName,mail`

**Result:** All members of the specified group.

### Example 4: Query with Record Limit for Preview

**Configuration:**
Same as Example 1, but dataset preview will retrieve only the first records based on the $top parameter or Dataiku's record_limit setting.

**Use Case:** Quickly preview data structure and sample records without retrieving the entire dataset.

### Example 5: Authentication Methods Report (Beta)

**Configuration:**
- API Version: `beta`
- API Endpoint: `/reports/authenticationMethods/userRegistrationDetails`
- HTTP Method: `GET`
- $select: `id,userDisplayName,userPrincipalName,isSystemPreferred`

**Result:** User authentication method registration details (beta feature).

## Data Retrieval Details

### Pagination
- **Automatic Pagination:** The connector automatically follows `@odata.nextLink` tokens to retrieve all pages
- **Page Handling:** Pages are transparently combined into a single result set
- **Performance:** Pagination is handled automatically; no user action required

### Record Limits
- **Record Limit Parameter:** The `records_limit` parameter in Dataiku's dataset preview mode limits the number of records retrieved
- **Usage:** When previewing a dataset, Dataiku passes `records_limit` to limit results for faster preview
- **Behavior:** The connector stops pagination after retrieving `records_limit` records (if limit > 0)
- **Setting:** User can configure `$top` to control the page size; `records_limit` controls total retrieval

**Example:** With `records_limit=100` and `$top=50`:
- First API request retrieves 50 records
- Second API request retrieves 50 records
- Connector stops (total 100 records reached)

### Query Parameters Reference
For comprehensive documentation on OData query parameters, see [Microsoft Graph Query Parameters](https://learn.microsoft.com/en-us/graph/query-parameters?tabs=http).

## Troubleshooting

### Common Issues

#### Error: "api_endpoint is mandatory and cannot be empty"
- **Cause:** The API Endpoint field was left blank
- **Solution:** Fill in the `api_endpoint` field with a valid endpoint (e.g., `/users`, `/groups`)

#### Error: "API request failed: 401 Unauthorized"
- **Cause:** Authentication credentials are invalid or expired
- **Solutions:**
  1. Verify the Tenant ID is correct
  2. Check that Client ID and Client Secret are still valid
  3. Confirm the Azure application is registered in your Entra ID tenant
  4. Verify the application has the required API permissions

#### Error: "API request failed: 403 Forbidden"
- **Cause:** The Azure application lacks required permissions for the endpoint
- **Solution:** Add the required Microsoft Graph permission to your Azure application (e.g., `User.Read.All` for `/users`)

#### Error: "API request failed: 404 Not Found"
- **Cause:** The API endpoint does not exist
- **Solutions:**
  1. Verify the endpoint path is correct (should start with `/`)
  2. Check that the endpoint is available in the selected API version (v1.0 vs. beta)
  3. Ensure any dynamic path parameters (like `{id}`) are correctly formatted

#### Error: "request_body must be valid JSON"
- **Cause:** The JSON in the request body field is malformed
- **Solution:** Validate JSON syntax (use a JSON validator tool or IDE)

#### Error: "custom_headers must be valid JSON"
- **Cause:** The JSON in the custom headers field is malformed
- **Solution:** Validate JSON syntax

#### Slow Performance
- **Cause:** Retrieving very large result sets (10,000+ records)
- **Solutions:**
  1. Add `$filter` conditions to reduce result set size
  2. Use `$select` to include only needed fields
  3. Increase `$top` to retrieve more records per request (reduce number of API calls)
  4. Preview the data with `records_limit` first

#### "Too Many Requests" or 429 Error
- **Cause:** API throttling due to too many requests
- **Behavior:** The connector automatically retries with exponential backoff
- **Solutions:**
  1. Reduce the frequency of dataset refreshes
  2. Decrease `$top` to make requests faster
  3. Wait and retry later
  4. Check Microsoft Graph [service limits](https://learn.microsoft.com/en-us/graph/throttling)

#### Timeout or "504 Bad Gateway"
- **Cause:** Request took too long to process
- **Behavior:** The connector automatically retries up to 5 times with exponential backoff
- **Solutions:**
  1. Add `$filter` to reduce result set
  2. Use `$select` to include fewer fields
  3. Preview with `records_limit` first
  4. Try querying during off-peak hours

### Debugging

To enable detailed logging:

1. In Dataiku DSS, navigate to **Administration > Settings > Logging**
2. Set the log level for the plugin to **DEBUG**
3. Review logs in the job output or **Administration > Logs**

Common debug information:
- API endpoint being queried
- Query parameters used
- Number of records retrieved per page
- Pagination link following
- Any API errors returned

## Limitations

- **Read-Only:** This connector only supports reading data (GET). Write operations (POST/PATCH/DELETE) return data but don't modify systems.
- **No Batch Requests:** Microsoft Graph batch endpoint not supported; query one endpoint at a time
- **Dynamic Schema:** Output schema depends on the endpoint and `$select` parameter; schema detection may be limited
- **Pagination Transparency:** Pagination is automatic but could affect performance with very large result sets
- **Endpoint-Specific Features:** Some endpoints have unique capabilities not exposed through standard OData parameters
- **No Subscriptions/Webhooks:** Real-time data subscriptions not supported

## Best Practices

### Performance Optimization
1. **Always use `$select`** - Specify only needed fields to reduce response size
2. **Use `$filter` wisely** - Filter at the API level rather than in Dataiku to reduce data transfer
3. **Increase `$top` gradually** - Test with small page sizes, then increase for better performance
4. **Check Endpoint Limits** - Different endpoints have different maximum `$top` values; consult documentation
5. **Preview First** - Use dataset preview with `records_limit` before executing full queries

### Data Quality
1. **Understand Null Fields** - Some users/groups may not have all properties populated
2. **Date/Time Formats** - Microsoft Graph returns ISO 8601 format; handle appropriately in downstream recipes
3. **Verify Permissions** - Ensure Azure app has permissions to read all data you expect
4. **Test with Small Subsets** - Always test filters and selections on sample data first

### Security
1. **Protect Client Secret** - Store client secret securely; use Dataiku's password field type
2. **Limit Permissions** - Grant Azure app only the minimum permissions needed
3. **Monitor Access** - Review Azure app activity and access logs regularly
4. **Rotate Credentials** - Periodically refresh client secrets
5. **Audit Data Exports** - Track what data is being exported from your Entra ID

### Query Optimization
1. **Combine Conditions** - Use multiple `$filter` conditions rather than multiple queries
2. **Date Range Filters** - Filter by date when querying time-series data
3. **Avoid * Select** - `$select: *` retrieves all fields; explicitly list needed fields
4. **Check Query Support** - Not all endpoints support all parameters; verify before configuring

## Plugin Configuration Setup

### Registering an Azure Application

1. **Sign In to Azure Portal:** Go to [portal.azure.com](https://portal.azure.com)
2. **Create Application:**
   - Navigate to **Azure Active Directory > App registrations**
   - Click **New registration**
   - Enter an application name (e.g., "Dataiku Microsoft Graph Connector")
   - Select **Accounts in this organizational directory only**
   - Click **Register**
3. **Add Client Secret:**
   - Go to **Certificates & secrets**
   - Click **New client secret**
   - Enter a description and expiration
   - Copy the **Value** (this is your Client Secret)
4. **Grant API Permissions:**
   - Go to **API permissions**
   - Click **Add a permission**
   - Select **Microsoft Graph**
   - Choose **Application permissions** (for daemon/service scenarios)
   - Select required permissions (e.g., `User.Read.All`, `Group.Read.All`)
   - Click **Grant admin consent**
5. **Collect Credentials:**
   - **Client ID:** From **Overview** tab
   - **Tenant ID:** From **Overview** tab
   - **Client Secret:** From **Certificates & secrets** tab (saved above)

### Configuring the Plugin in Dataiku

1. Go to **Administration > Plugins > Installed > Microsoft Graph API**
2. Click **Settings**
3. Fill in:
   - **Tenant ID:** Your Azure AD tenant ID
   - **Client ID:** Your application's client ID
   - **Client Secret:** Your application's client secret
4. Click **Save**

## Support & Resources

- **Plugin Repository:** [https://github.com/datarsense/dss-plugin-microsoft-graph](https://github.com/datarsense/dss-plugin-microsoft-graph)
- **Microsoft Graph API Documentation:** [https://learn.microsoft.com/en-us/graph/api/overview](https://learn.microsoft.com/en-us/graph/api/overview)
- **OData Query Parameters:** [https://learn.microsoft.com/en-us/graph/query-parameters](https://learn.microsoft.com/en-us/graph/query-parameters)
- **Graph Explorer:** [https://developer.microsoft.com/en-us/graph/graph-explorer](https://developer.microsoft.com/en-us/graph/graph-explorer)
- **Microsoft Graph Permissions Reference:** [https://learn.microsoft.com/en-us/graph/permissions-reference](https://learn.microsoft.com/en-us/graph/permissions-reference)
- **Dataiku Plugin Documentation:** [https://doc.dataiku.com/dss/latest/plugins/index.html](https://doc.dataiku.com/dss/latest/plugins/index.html)

For issues or feature requests, please open an issue on the plugin repository.
