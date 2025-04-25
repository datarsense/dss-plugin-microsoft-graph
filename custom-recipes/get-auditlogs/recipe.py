
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

from helpers import raise_if_missing_plugin_parameters

from azure.identity import ClientSecretCredential
from msgraph_beta import GraphServiceClient
from msgraph_beta.generated.models.security.audit_log_query import AuditLogQuery
from msgraph_beta.generated.models.security.audit_log_record import AuditLogRecord
from msgraph_beta.generated.models.security.audit_log_record_type import AuditLogRecordType
from msgraph_beta.generated.models.security.audit_log_record_type import AuditLogRecordType
from msgraph_beta.generated.models.security.audit_log_query_status import AuditLogQueryStatus
from msgraph_beta.generated.models.security.audit_log_record_collection_response import AuditLogRecordCollectionResponse

async def getPurviewLogs(credentials, queryStartDate, queryEndDate):
  msgraph_client = GraphServiceClient(credentials, ['https://graph.microsoft.com/.default'])

  request_body = AuditLogQuery(
    odata_type = "#microsoft.graph.security.auditLogQuery",
    display_name = "MsGraphQuery",
    filter_start_date_time = queryStartDate.isoformat(),
    filter_end_date_time = queryEndDate.isoformat(),
    # record_type_filters = [
    #   AuditLogRecordType.ExchangeAdmin,
    #   AuditLogRecordType.AzureActiveDirectory,
		#   AuditLogRecordType.AzureActiveDirectoryAccountLogon,
	  # ]
  )

  # Send Purview query and get query status
  queryResult = await msgraph_client.security.audit_log.queries.with_url('https://graph.microsoft.com/beta/security/auditLog/queries').post(request_body)
  
  # Wait during query exec
  while True:
    queryStatus = await msgraph_client.security.audit_log.queries.by_audit_log_query_id(queryResult.id).with_url('https://graph.microsoft.com/beta/security/auditLog/queries/' + str(queryResult.id)).get()
    print(queryStatus.status)
    if queryStatus.status == AuditLogQueryStatus.Succeeded:
      break
    else:
      time.sleep(30)

  # return query results
  #return await msgraph_client.security.audit_log.queries.by_audit_log_query_id(queryResult.id).records.get()
  return queryStatus


def getPurviewLogsRecords(credentials, queryResultId, pagination=True):
  token_result = credentials.get_token('https://graph.microsoft.com/.default')

  graph_results = []
  if hasattr(token_result, 'token'):
    headers = {'Authorization': 'Bearer ' + token_result.token}
    
    url = "https://graph.microsoft.com/beta/security/auditLog/queries/" + queryResultId + "/records"
    while url:
      try:
        graph_result = requests.get(url=url, headers=headers).json()
        graph_results.extend(graph_result['value'])
        if (pagination == True):
          url = graph_result['@odata.nextLink']
        else:
          url = None
      except:
        break

  #return await msgraph_client.security.audit_log.queries.by_audit_log_query_id('debfbef5-85a9-4da1-99b1-3b8761023e84').records.get()
  return graph_results

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
query = asyncio.run(getPurviewLogs(purview_credentials, start_datetime, end_datetime))
print(query.id)

# Download results
result = getPurviewLogsRecords(purview_credentials, query.id)
  
# Write recipe outputs in output folder
main_output_name = get_output_names_for_role('main_output')[0]
folder = dataiku.Folder(main_output_name)
folder.write_json("purview_logs-"+start_datetime.strftime("%Y%m%d")+"-"+end_datetime.strftime("%Y%m%d")+".json", result)
