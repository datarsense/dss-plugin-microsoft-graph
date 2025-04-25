import requests
import time

from msgraph_beta import GraphServiceClient
from msgraph_beta.generated.models.security.audit_log_query import AuditLogQuery
from msgraph_beta.generated.models.security.audit_log_query_status import AuditLogQueryStatus


# Raise an error if API authentication token is null or contains only blank chars
def raise_if_missing_plugin_parameters(plugin_params):
    if (not 'tenant_id' in plugin_params) or (plugin_params['tenant_id'] is None) or (plugin_params['tenant_id'].strip() == ""):
      raise Exception('Error : Entra ID tenant ID is missing in plugin settings')

    if (not 'client_id' in plugin_params) or (plugin_params['client_id'] is None) or (plugin_params['client_id'].strip() == ""):
      raise Exception('Error : OAuth2 application client ID id is missing in plugin settings')

    if (not 'client_secret' in plugin_params) or (plugin_params['client_secret'] is None) or (plugin_params['client_secret'].strip() == ""):
      raise Exception('Error : OAuth2 application client seccret is missing in plugin settings')


async def getPurviewLogs(credentials, queryStartDate, queryEndDate, queryRecordTypeFilters = []):
  msgraph_client = GraphServiceClient(credentials, ['https://graph.microsoft.com/.default'])

  request_body = AuditLogQuery(
    odata_type = "#microsoft.graph.security.auditLogQuery",
    display_name = "MsGraphQuery",
    filter_start_date_time = queryStartDate.isoformat(),
    filter_end_date_time = queryEndDate.isoformat(),
    record_type_filters = queryRecordTypeFilters
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

  