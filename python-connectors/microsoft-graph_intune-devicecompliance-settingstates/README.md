# Intune Device Compliance Setting States Connector

## Overview

This connector retrieves Microsoft Intune device compliance setting states from `deviceCompliancePolicySettingStateSummaries` and returns a single aggregated dataset containing all compliance setting states across all summaries in your tenant.

### What This Connector Does

1. **Queries all `deviceCompliancePolicySettingStateSummaries`** — Lists all device compliance policy setting state summaries in your tenant
2. **Retrieves setting states for each summary** — For each summary, queries its associated `deviceComplianceSettingStates`
3. **Aggregates into single dataset** — Returns all setting states as one unified dataset with `summaryId` field added for reference
4. **Supports filtering** — Optionally filter to specific summary IDs
5. **Supports pagination** — Automatically handles pagination for large result sets
6. **Respects preview limits** — Uses `records_limit` for efficient dataset previews

## Prerequisites

### Plugin Configuration
Before using this connector, ensure the following plugin-level parameters are configured in Dataiku DSS:

- **Tenant ID** - Your Microsoft Entra ID (Azure AD) tenant ID (UUID format)
- **Client ID** - The client ID of your registered Azure application
- **Client Secret** - The client secret of your registered Azure application

### API Permissions
The registered Azure application requires the following Microsoft Graph API permission:

- `DeviceManagementServiceConfig.Read.All` - Read Intune device configuration and compliance

For setup instructions, see [Plugin Configuration Setup](#plugin-configuration-setup).

### Authentication Flow
The connector uses OAuth 2.0 Client Credentials flow to authenticate with Microsoft Graph API.

## Connector Configuration

### Optional Parameters

#### Device Compliance Summary IDs
- **Type:** Multi-line text area
- **Description:** List of deviceCompliancePolicySettingStateSummaryIds to filter by
- **Format:** One ID per line
- **Default:** Empty (retrieves all summaries)

**Example:**
```
a1b2c3d4-e5f6-7890-abcd-ef1234567890
f9e8d7c6-b5a4-3210-f9e8-d7c6b5a43210
```

If left empty, the connector will retrieve all deviceCompliancePolicySettingStateSummaries and their setting states.

#### $select - Select Fields
- **Type:** Text
- **Description:** OData `$select` parameter to specify which fields to include
- **Format:** Comma-separated field names
- **Default:** Empty (returns all fields)

**Examples:**
- `id,setting,settingName,deviceName,state`
- `id,setting,settingName,deviceId,userId,state`

## Output Schema

The output is a single aggregated dataset containing **deviceComplianceSettingState** objects from all queried `deviceCompliancePolicySettingStateSummaries`. Each row represents a single compliance setting state for a device/user.

### Dataset Columns

| Column | Type | Description |
|--------|------|-------------|
| id | String | Unique identifier of the deviceComplianceSettingState |
| setting | String | Internal name of the compliance setting |
| settingName | String | Display/human-readable name of the compliance setting |
| deviceId | String | Unique identifier of the device |
| deviceName | String | Name/hostname of the device |
| userId | String | Unique identifier of the user |
| userEmail | String | Email address of the user |
| userName | String | Display name of the user |
| userPrincipalName | String | User principal name (UPN) - typically username@domain |
| deviceModel | String | Model or type of the device |
| state | String | Current compliance state. Values: `compliant`, `noncompliant`, `notApplicable`, `error`, etc. |
| complianceGracePeriodExpirationDateTime | DateTime | ISO 8601 timestamp when compliance grace period expires (if applicable) |
| summaryId | String | ID of the parent deviceCompliancePolicySettingStateSummary this state belongs to |

### Data Aggregation

- **Single Dataset:** All compliance setting states from all summaries are combined into one dataset
- **Cross-Summary Data:** Each row is tagged with its `summaryId` so you can identify which summary it came from
- **Complete Coverage:** By default, the dataset includes setting states for **all** deviceCompliancePolicySettingStateSummaries in your tenant
- **Optional Filtering:** Use the "Device Compliance Summary IDs" parameter to limit to specific summaries

### Example Dataset

When querying multiple summaries, you'll get results like:

| id | setting | settingName | deviceId | deviceName | userId | state | summaryId |
|----|---------|-------------|----------|-----------|--------|-------|-----------|
| 9905f955-... | encryption | Device Encryption | dev-001-uuid | LAPTOP-ABC | user1-uuid | compliant | summary-1-uuid |
| 9905f956-... | encryption | Device Encryption | dev-002-uuid | LAPTOP-XYZ | user2-uuid | noncompliant | summary-1-uuid |
| 9905f957-... | firewall | Windows Firewall | dev-001-uuid | LAPTOP-ABC | user1-uuid | compliant | summary-2-uuid |
| 9905f958-... | firewall | Windows Firewall | dev-003-uuid | DESKTOP-123 | user3-uuid | notApplicable | summary-2-uuid |

Notice how:
- Different summaries contain different settings
- Same device may appear in multiple summaries with different compliance settings
- The `summaryId` column identifies which summary each row came from

## Usage Examples

### Example 1: Retrieve All Compliance Setting States

**Configuration:**
- Device Compliance Summary IDs: (empty)
- $select: (empty)

**Result:** All compliance setting states from all summaries in your Intune tenant, with all fields.

### Example 2: Filter to Specific Summaries

**Configuration:**
- Device Compliance Summary IDs:
  ```
  a1b2c3d4-e5f6-7890-abcd-ef1234567890
  f9e8d7c6-b5a4-3210-f9e8-d7c6b5a43210
  ```
- $select: (empty)

**Result:** Compliance setting states only from the specified summaries.

### Example 3: Select Specific Fields

**Configuration:**
- Device Compliance Summary IDs: (empty)
- $select: `id,setting,settingName,deviceName,state`

**Result:** All compliance setting states, but only with ID, setting name, device name, and compliance state.

### Example 4: Combine Filtering and Field Selection

**Configuration:**
- Device Compliance Summary IDs:
  ```
  a1b2c3d4-e5f6-7890-abcd-ef1234567890
  ```
- $select: `id,setting,settingName,deviceName,state,userEmail`

**Result:** Specific summary's setting states with selected fields only.

## Data Retrieval Details

### API Endpoints Used

1. **First Query:** `GET /deviceManagement/deviceCompliancePolicySettingStateSummaries`
   - Retrieves list of all compliance policy setting state summaries
   - Or filters to specified IDs if provided

2. **Second Query (for each summary):** `GET /deviceManagement/deviceCompliancePolicySettingStateSummaries/{id}/deviceComplianceSettingStates`
   - Retrieves all compliance setting states for each summary
   - Supports `$select` parameter for field filtering

### Pagination

- **Automatic Pagination:** The connector automatically follows `@odata.nextLink` tokens at both levels (summaries and setting states)
- **Page Handling:** Pages are transparently combined into a single result set
- **Performance:** For large tenants with many summaries, initial queries may take time due to nested pagination

### Record Limits

- **Record Limit Parameter:** When previewing a dataset in Dataiku, `records_limit` limits the number of setting states retrieved
- **Behavior:** Stops retrieving additional setting states once limit is reached (stops processing new summaries)
- **Use:** Enables quick previews without retrieving entire large datasets

**Example:** With `records_limit=1000`:
- Retrieves summaries
- Gets setting states from first summary (e.g., 500 states)
- Gets setting states from second summary (e.g., 500 states)
- Total of 1000 states returned, stops processing further

## Troubleshooting

### Common Issues

#### Error: "Entra ID tenant ID is missing in plugin settings"
- **Solution:** Configure Tenant ID in plugin settings

#### Error: "401 Unauthorized"
- **Cause:** Authentication credentials invalid or expired
- **Solutions:**
  1. Verify Tenant ID, Client ID, and Client Secret are correct
  2. Confirm Azure application is registered in your Entra ID tenant
  3. Verify application has required API permissions (`DeviceManagementServiceConfig.Read.All`)

#### Error: "403 Forbidden"
- **Cause:** Azure application lacks required permissions
- **Solution:** Add `DeviceManagementServiceConfig.Read.All` permission to Azure application

#### Error: "Resource not found"
- **Cause:** Summary IDs are invalid or don't exist
- **Solution:** Verify summary IDs are correct UUIDs. Leave empty to retrieve all summaries first.

#### Slow Performance
- **Cause:** Large number of summaries or setting states
- **Solutions:**
  1. Filter to specific summary IDs if you don't need all summaries
  2. Use `$select` to reduce fields retrieved
  3. Preview dataset with `records_limit` first

### Debugging

Enable DEBUG logging in Dataiku to see:
- API endpoints being queried
- Number of summaries retrieved
- Number of setting states per summary
- Pagination links being followed
- Any API errors returned

## Limitations

- **Read-Only:** This connector only supports reading data; no write capability
- **Sequential Summary Processing:** Summaries are processed sequentially (not in parallel)
- **No Schema Detection:** Schema is not pre-detected; depends on API response at runtime
- **API Throttling:** May encounter rate limiting on very large result sets; connector has automatic retry logic
- **Nested Pagination:** Both summaries and setting states require pagination, which may slow large queries

## Best Practices

### Performance Optimization
1. **Use Record Limit for Preview** — Preview with `records_limit` before running full query
2. **Filter by Summary IDs** — If you don't need all summaries, filter to specific ones
3. **Select Specific Fields** — Use `$select` to include only needed fields
4. **Off-Peak Queries** — Run large queries during off-peak hours to avoid throttling

### Data Quality
1. **Verify Summary IDs** — Ensure provided summary IDs are valid
2. **Understand Null Fields** — Some fields may be null depending on compliance status
3. **Check Last Updated** — Results reflect current compliance state; historical data requires separate queries

### Security
1. **Protect Client Secret** — Store in Dataiku's secure password field
2. **Limit Permissions** — Grant only `DeviceManagementServiceConfig.Read.All` permission
3. **Audit Access** — Monitor who has access to compliance data
4. **Rotate Credentials** — Periodically refresh client secret


## Support & Resources

- **Microsoft Graph API Reference:** [https://learn.microsoft.com/en-us/graph/api/overview](https://learn.microsoft.com/en-us/graph/api/overview)
- **Device Compliance Policy Setting State Summary:** [https://learn.microsoft.com/en-us/graph/api/resources/intune-deviceconfig-devicecompliancepolicysettingstatesummary](https://learn.microsoft.com/en-us/graph/api/resources/intune-deviceconfig-devicecompliancepolicysettingstatesummary)
- **Device Compliance Setting State:** [https://learn.microsoft.com/en-us/graph/api/intune-deviceconfig-devicecompliancesettingstate-list](https://learn.microsoft.com/en-us/graph/api/intune-deviceconfig-devicecompliancesettingstate-list)
- **OData Query Parameters:** [https://learn.microsoft.com/en-us/graph/query-parameters](https://learn.microsoft.com/en-us/graph/query-parameters)
- **Dataiku Plugin Documentation:** [https://doc.dataiku.com/dss/latest/plugins/index.html](https://doc.dataiku.com/dss/latest/plugins/index.html)

For issues or feature requests, please open an issue on the plugin repository: [https://github.com/datarsense/dss-plugin-microsoft-graph](https://github.com/datarsense/dss-plugin-microsoft-graph)
