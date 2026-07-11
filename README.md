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

### List Entra ID Users
Retrieves a comprehensive list of users from Microsoft Entra ID with detailed user information including contact details, account status, sign-in activity, and more. Can be used for user auditing, access reviews, and user management workflows.

For detailed usage instructions, see [List Entra ID Users Connector Documentation](./python-connectors/microsoft-graph_entra-users-list/README.md).

## Plugin Configuration
The following parameters can be configured globally:
* **Tenant ID** : Microsoft Entra ID tenant ID (UUID format)
* **Client ID** : Microsoft Entra ID registered application client ID
* **Client Secret** : Microsoft Entra ID registered application client secret

For detailed setup instructions, see the [Generic Microsoft Graph API Query Connector - Plugin Configuration Setup](./python-connectors/microsoft-graph-generic-api-query/README.md#plugin-configuration-setup) section.

## Testing

This plugin includes comprehensive pytest-based unit tests for the generic connector.

### Running Tests

**Prerequisites:**
- Python 3.6+
- pytest
- mock library (usually included with pytest)

**Run all tests:**
```bash
pytest tests/ -v
```

**Run specific test file:**
```bash
pytest tests/test_generic_connector.py -v
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

The test suite includes:
- **Parameter Validation Tests** - Verify invalid parameters are rejected with clear errors
- **URL Construction Tests** - Ensure correct URL building for different API versions
- **Query Parameters Tests** - Validate OData parameter passing ($select, $filter, $top, $search)
- **Pagination Tests** - Test automatic pagination with @odata.nextLink
- **Record Limit Tests** - Verify records_limit enforcement
- **Custom Headers Tests** - Test custom HTTP header support
- **Request Body Tests** - Validate POST/PATCH body handling
- **HTTP Methods Tests** - Test all supported HTTP methods (GET, POST, PATCH, DELETE)
- **Edge Cases Tests** - Handle empty results, malformed responses, etc.
- **Authorization Tests** - Verify token and header handling
- **Integration Tests** - Test complete workflows with multiple features

### Test Markers

Tests are organized with pytest markers for selective execution:
- `@pytest.mark.unit` - Unit tests (default)
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.slow` - Slow running tests

Example: Run only unit tests
```bash
pytest tests/ -m unit -v
```
