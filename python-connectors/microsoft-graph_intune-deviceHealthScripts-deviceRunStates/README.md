# Intune Device Health Script Device Run States Connector

## Overview

This connector retrieves the device run states for one Microsoft Intune device health script from Microsoft Graph. Each output row represents the execution state of the script on a managed device.

The connector uses the Microsoft Graph beta endpoint:

```
GET /beta/deviceManagement/deviceHealthScripts/{deviceHealthScriptId}/deviceRunStates?$expand=managedDevice
```

## Prerequisites

Configure these plugin-level parameters in Dataiku DSS:

- **Tenant ID** - Microsoft Entra ID tenant ID
- **Client ID** - Client ID of the registered Azure application
- **Client Secret** - Client secret of the registered Azure application

The Azure application must have an Intune/Microsoft Graph application permission that allows reading device health script run states. In most tenants this is `DeviceManagementConfiguration.Read.All`. Grant admin consent after adding the permission.

The connector authenticates with the OAuth 2.0 client credentials flow.

## Connector Configuration

### Device Health Script GUID

- **Parameter:** `deviceHealthScriptId`
- **Type:** String
- **Required:** Yes
- **Description:** The GUID of the Intune device health script to query

Example:

```
12345678-abcd-1234-abcd-1234567890ab
```

The script must exist in the tenant and be accessible to the registered application.

## Output Schema

The connector returns one row per device run state.

| Column | Type | Description |
|--------|------|-------------|
| `deviceId` | String | ID of the device associated with the run state |
| `deviceName` | String | Display name of the managed device |
| `managedDevice` | String | Complete expanded managed-device object serialized as JSON |
| `detectionState` | String | Detection state reported by the health script |
| `remediationState` | String | Remediation state reported by the health script |
| `preRemediationDetectionScriptOutput` | String | Output produced by the pre-remediation detection script |
| `preRemediationDetectionScriptError` | String | Error reported by the pre-remediation detection script |
| `postRemediationDetectionScriptOutput` | String | Output produced by the post-remediation detection script |
| `remediationScriptError` | String | Error reported by the remediation script |
| `lastStateUpdateDateTime` | Date | Date and time when the run state was last updated |

The four script output/error fields are cleaned before output: only alphanumeric characters are retained. This prevents free-text values from causing Dataiku parsing issues.

## Data Retrieval Details

### Pagination

The connector automatically follows Microsoft Graph `@odata.nextLink` values and combines all returned pages into one dataset.

### Preview Limits

Dataiku's `records_limit` is passed to the retrieval helper when generating rows, allowing dataset previews to stop after the requested number of records. Full dataset reads retrieve all available pages.

### Record Count

Dataiku record-count requests issue a separate API request and return the number of run states returned for the configured health script.

## Troubleshooting

### `401 Unauthorized`

- Verify the tenant ID, client ID, and client secret.
- Confirm that the application uses application permissions rather than delegated permissions.
- Check that the access token can be acquired for `https://graph.microsoft.com/.default`.

### `403 Forbidden`

- Add the required Intune application permission, such as `DeviceManagementConfiguration.Read.All`.
- Grant admin consent for the tenant.
- Confirm that the application is allowed to read the requested Intune resource.

### `404 Not Found`

- Verify that `deviceHealthScriptId` is a valid health script GUID.
- Confirm that the health script exists in the tenant.

### Empty Results

A valid health script may have no device run states if it has not run on any devices or if no devices currently have a reported state.

## Limitations

- Read-only connector; writing and append operations are not supported.
- Uses the Microsoft Graph beta API.
- Results describe the current run-state data available from Intune, not a historical execution log.
- Script output and error values lose formatting and punctuation during cleanup.

## Microsoft Resources

- [List device health script device run states](https://learn.microsoft.com/en-us/graph/api/intune-devices-devicehealthscriptdevicestate-list)
- [Microsoft Graph permissions reference](https://learn.microsoft.com/en-us/graph/permissions-reference)
- [Microsoft Graph query parameters](https://learn.microsoft.com/en-us/graph/query-parameters)
- [Dataiku Plugin Documentation](https://doc.dataiku.com/dss/latest/plugins/index.html)
