
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
from msgraph_beta import GraphServiceClient

from msgraph_beta.generated.models.security.audit_log_record import AuditLogRecord
from msgraph_beta.generated.models.security.audit_log_record_type import AuditLogRecordType
from msgraph_beta.generated.models.security.audit_log_record_type import AuditLogRecordType
from msgraph_beta.generated.models.security.audit_log_query_status import AuditLogQueryStatus
from msgraph_beta.generated.models.security.audit_log_record_collection_response import AuditLogRecordCollectionResponse


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

# Query Purview logs
record_type_filters = [
    AuditLogRecordType.AzureActiveDirectory,
    AuditLogRecordType.AzureActiveDirectoryStsLogon,
    AuditLogRecordType.AzureActiveDirectoryAccountLogon
  ]
query = asyncio.run(getPurviewLogs(purview_credentials, start_datetime, end_datetime, record_type_filters))
print(query.id)

# Download results
result = getPurviewLogsRecords(purview_credentials, query.id)
  
# Write recipe outputs in output folder
main_output_name = get_output_names_for_role('main_output')[0]
folder = dataiku.Folder(main_output_name)
folder.write_json("purview_logs-"+start_datetime.strftime("%Y%m%d")+"-"+end_datetime.strftime("%Y%m%d")+".json", result)
