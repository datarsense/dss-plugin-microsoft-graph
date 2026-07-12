# DSS Plugin - Microsoft Graph API

This Dataiku DSS plugin provides python recipes to load data from Microsoft Graph API.

## Compatibility

* Dataiku DSS 12.0 or higher

## Connectors

### Generic Microsoft Graph API Query (Recommended)
A flexible, generic connector to query any Microsoft Graph API endpoint (v1.0 or beta). Supports customizable HTTP methods (GET, POST, PATCH, DELETE), OData query parameters ($select, $filter, $top, $search), custom headers, and request bodies. Ideal for exploring Microsoft Graph data or querying endpoints not covered by specific connectors.

**Key Features:**
- Query any Microsoft Graph endpoint
- Support for OData query parameters ($select, $filter, $top, $search)
- Custom HTTP headers and request body for POST/PATCH operations
- Automatic pagination
- Record limiting for dataset previews
- Comprehensive documentation and examples

For detailed usage instructions, see [Generic Microsoft Graph API Query Connector Documentation](./python-connectors/microsoft-graph-generic-api-query/README.md).

### Intune Device Compliance Setting States
Retrieves device compliance setting states from Microsoft Intune, aggregated across all deviceCompliancePolicySettingStateSummaries. Returns a single unified dataset containing compliance states for all devices, optionally filtered by specific compliance policy summaries.

**Key Features:**
- Aggregates compliance setting states from all compliance policies
- Includes device, user, and compliance state information
- Optional filtering by compliance policy summary IDs
- Support for OData $select parameter to customize output fields
- Automatic pagination for large result sets
- Complete cross-organization compliance visibility

For detailed usage instructions, see [Intune Device Compliance Setting States Connector Documentation](./python-connectors/microsoft-graph_intune-devicecompliance-settingstates/README.md).

### List Entra ID Users
Retrieves a comprehensive list of users from Microsoft Entra ID with detailed user information including contact details, account status, sign-in activity, and more. Can be used for user auditing, access reviews, and user management workflows.

For detailed usage instructions, see [List Entra ID Users Connector Documentation](./python-connectors/microsoft-graph_entra-users-list/README.md).

## Plugin Configuration Setup

### Registering an Azure Application

1. **Sign In to Azure Portal:** Go to [portal.azure.com](https://portal.azure.com)
2. **Create Application:**
   - Navigate to **Azure Active Directory > App registrations**
   - Click **New registration**
   - Enter application name (e.g., "Dataiku Intune Connector")
   - Select **Accounts in this organizational directory only**
   - Click **Register**
3. **Add Client Secret:**
   - Go to **Certificates & secrets**
   - Click **New client secret**
   - Enter description and expiration
   - Copy the **Value** (this is your Client Secret)
4. **Grant API Permissions:**
   - Go to **API permissions**
   - Click **Add a permission**
   - Select **Microsoft Graph**
   - Choose **Application permissions**
   - Search for and select appropriate permissions
   - Click **Grant admin consent**
5. **Collect Credentials:**
   - **Client ID:** From **Overview** tab
   - **Tenant ID:** From **Overview** tab  
   - **Client Secret:** From **Certificates & secrets** tab

### Configuring the Plugin in Dataiku

1. Go to **Administration > Plugins > Installed > Microsoft Graph API**
2. Click **Settings**
3. Fill in:
   - **Tenant ID:** Your Azure AD tenant ID
   - **Client ID:** Your application's client ID
   - **Client Secret:** Your application's client secret
4. Click **Save**


## Testing

This plugin includes comprehensive pytest-based unit tests for all connectors.

### Running Tests

**Prerequisites:**
- Python 3.6+
- pytest
- mock library (usually included with pytest)

**Run all tests:**
```bash
pytest tests/ -v
```

**Run specific test file (Generic Connector):**
```bash
pytest tests/test_generic_connector.py -v
```

**Run specific test file (Intune Compliance Connector):**
```bash
pytest tests/test_intune_compliance_connector.py -v
```

**Run specific test class:**
```bash
pytest tests/test_generic_connector.py::TestParameterValidation -v
```

**Run specific test:**
```bash
pytest tests/test_generic_connector.py::TestParameterValidation::test_empty_api_endpoint_raises_error -v
```

**Run with coverage:**
```bash
pytest tests/ --cov=python-lib --cov=python-connectors -v
```

### Test Coverage

**Generic Connector Tests** (`test_generic_connector.py`):
- Parameter validation and error handling
- URL construction for different API versions
- OData query parameter passing ($select, $filter, $top, $search)
- Automatic pagination with @odata.nextLink
- Record limit enforcement
- Custom HTTP headers support
- POST/PATCH body handling
- All HTTP methods (GET, POST, PATCH, DELETE)
- Edge cases and error handling
- Authorization header validation
- Complete workflow integration tests

**Intune Compliance Connector Tests** (`test_intune_compliance_connector.py`):
- Output schema validation (all deviceComplianceSettingState fields)
- Aggregation across multiple compliance policy summaries
- Dataset coverage verification (all summaries included)
- Optional filtering by summary IDs
- Pagination at both summary and state levels
- Record limit enforcement for previews
- Field selection with $select parameter
- Edge cases (empty summaries, non-existent IDs)
- Integration tests with real sample data

### Test Markers

Tests are organized with pytest markers for selective execution:
- `@pytest.mark.unit` - Unit tests (default)
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.slow` - Slow running tests

Example: Run only unit tests
```bash
pytest tests/ -m unit -v
```
