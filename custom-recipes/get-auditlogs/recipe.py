
# -*- coding: utf-8 -*-
import asyncio
import dataiku
import datetime
import dateutil
import json
import numpy as np
import os
import pandas as pd
import requests
import time

from dataiku import pandasutils as pdu
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config, get_plugin_config

from helpers import raise_if_missing_plugin_parameters, getPurviewLogs, getPurviewLogsRecords

from azure.identity import ClientSecretCredential
from msgraph_beta.generated.models.security.audit_log_record_type import AuditLogRecordType

# Retrieve plugin parameters
plugin_params = get_plugin_config()

# Raise an error if mandatory plugin parameters are missing
raise_if_missing_plugin_parameters(plugin_params)

# Define Microsoft Graph API connection settings
tenant_id = plugin_params['tenant_id']
client_id = plugin_params['client_id']
client_secret = plugin_params['client_secret']
purview_credentials = ClientSecretCredential(tenant_id, client_id, client_secret)

# Define Microsoft Graph API query params
start_datetime_str = get_recipe_config()['startDateTime']
start_datetime = dateutil.parser.parse(start_datetime_str) #datetime.datetime.fromisoformat(start_datetime_str)

end_datetime_str = get_recipe_config().get('endDateTime')
end_datetime = dateutil.parser.parse(end_datetime_str) #datetime.datetime.fromisoformat(end_datetime_str)

# Builf query record type filter
record_type_filters = []
filterRecordTypes = get_recipe_config().get('filterRecordTypes')
selectedRecordTypes = get_recipe_config().get('selectedRecordTypes')

if filterRecordTypes and "entraAudit" in selectedRecordTypes:
    record_type_filters = record_type_filters + [
        AuditLogRecordType.AzureActiveDirectory
    ]

if filterRecordTypes and "entraSignIn" in selectedRecordTypes:
    record_type_filters = record_type_filters + [
        AuditLogRecordType.AzureActiveDirectoryStsLogon,
        AuditLogRecordType.AzureActiveDirectoryAccountLogon
    ]

if filterRecordTypes and "sharepoint" in selectedRecordTypes:
    record_type_filters = record_type_filters + [
        AuditLogRecordType.SharePoint,
        AuditLogRecordType.SharePointAppPermissionOperation,
        AuditLogRecordType.SharePointCommentOperation,
        AuditLogRecordType.SharePointContentTypeOperation,
        AuditLogRecordType.SharePointFieldOperation,
        AuditLogRecordType.SharePointFileOperation,
        AuditLogRecordType.SharePointListItemOperation,
        AuditLogRecordType.SharePointListOperation,
        AuditLogRecordType.SharePointSearch,
        AuditLogRecordType.SharePointSharingOperation,
        AuditLogRecordType.OneDrive
    ]

if filterRecordTypes and "exchange" in selectedRecordTypes:
    record_type_filters = record_type_filters + [
        AuditLogRecordType.ExchangeAdmin,
        AuditLogRecordType.ExchangeAggregatedOperation,
        AuditLogRecordType.ExchangeItem,
        AuditLogRecordType.ExchangeItemAggregated,
        AuditLogRecordType.ExchangeItemGroup,
        AuditLogRecordType.ExchangeSearch
    ]

if filterRecordTypes and "teams" in selectedRecordTypes:
    record_type_filters = record_type_filters + [
        AuditLogRecordType.MicrosoftTeams,
        AuditLogRecordType.MicrosoftTeamsAdmin,
        AuditLogRecordType.MicrosoftTeamsAnalytics,
        AuditLogRecordType.MicrosoftTeamsDevice,
        AuditLogRecordType.MicrosoftTeamsSensitivityLabelAction,
        AuditLogRecordType.MicrosoftTeamsShifts,
        AuditLogRecordType.TeamsEasyApprovals,
        AuditLogRecordType.TeamsHealthcare,
        AuditLogRecordType.TeamsQuarantineMetadata,
        AuditLogRecordType.TeamsUpdates
    ]

# Query Purview logs
query = asyncio.run(getPurviewLogs(purview_credentials, start_datetime, end_datetime, record_type_filters))
print(query.id)

# Download results
result = getPurviewLogsRecords(purview_credentials, query.id)
  
# Write recipe outputs in output folder
main_output_name = get_output_names_for_role('main_output')[0]
folder = dataiku.Folder(main_output_name)
folder.write_json("purview_logs-"+start_datetime.strftime("%Y%m%d")+"-"+end_datetime.strftime("%Y%m%d")+".json", result)
