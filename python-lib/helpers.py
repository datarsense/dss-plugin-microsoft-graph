import logging
import requests
import time

from kiota_authentication_azure.azure_identity_authentication_provider import AzureIdentityAuthenticationProvider
from msgraph_beta import GraphServiceClient
from msgraph_beta.generated.models.security.audit_log_query import AuditLogQuery
from msgraph_beta.generated.models.security.audit_log_query_status import AuditLogQueryStatus
from msgraph_beta.graph_request_adapter import GraphRequestAdapter, options as graph_reqest_options
from msgraph_core import APIVersion, GraphClientFactory


logger = logging.getLogger(__name__)


# Create a Microsoft Graph Beta API client
def getBetaGraphServiceClient(creds, scopes):
  auth_provider = AzureIdentityAuthenticationProvider(creds, scopes=scopes)
  beta_http_client = GraphClientFactory.create_with_default_middleware(options=graph_reqest_options, api_version=APIVersion.beta)
  request_adapter = GraphRequestAdapter(auth_provider, client=beta_http_client)
  
  return GraphServiceClient(credentials=creds, request_adapter=request_adapter)


# Raise an error if API authentication token is null or contains only blank chars
def raise_if_missing_plugin_parameters(plugin_params):
    if (not 'tenant_id' in plugin_params) or (plugin_params['tenant_id'] is None) or (plugin_params['tenant_id'].strip() == ""):
      raise Exception('Error : Entra ID tenant ID is missing in plugin settings')

    if (not 'client_id' in plugin_params) or (plugin_params['client_id'] is None) or (plugin_params['client_id'].strip() == ""):
      raise Exception('Error : OAuth2 application client ID id is missing in plugin settings')

    if (not 'client_secret' in plugin_params) or (plugin_params['client_secret'] is None) or (plugin_params['client_secret'].strip() == ""):
      raise Exception('Error : OAuth2 application client seccret is missing in plugin settings')


async def getPurviewLogs(credentials, queryStartDate, queryEndDate, queryRecordTypeFilters = []):
  msgraph_client = getBetaGraphServiceClient(credentials, ['https://graph.microsoft.com/.default'])

  request_body = AuditLogQuery(
    odata_type = "#microsoft.graph.security.auditLogQuery",
    display_name = "MsGraphQuery",
    filter_start_date_time = queryStartDate.isoformat(),
    filter_end_date_time = queryEndDate.isoformat(),
    record_type_filters = queryRecordTypeFilters
  )

  # Send Purview query and get query status
  queryResult = await msgraph_client.security.audit_log.queries.post(request_body)
  
  # Wait during query exec
  while True:
    queryStatus = await msgraph_client.security.audit_log.queries.by_audit_log_query_id(queryResult.id).get()
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


def listEntraDevices(credentials, pagination=True):
  token_result = credentials.get_token('https://graph.microsoft.com/.default')

  graph_results = []
  if hasattr(token_result, 'token'):
    headers = {'Authorization': 'Bearer ' + token_result.token}
    
    url = "https://graph.microsoft.com/beta/devices"
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

  return graph_results


def listIntuneManagedDevices(credentials, pagination=True):
  token_result = credentials.get_token('https://graph.microsoft.com/.default')

  graph_results = []
  if hasattr(token_result, 'token'):
    headers = {'Authorization': 'Bearer ' + token_result.token}
    
    url = "https://graph.microsoft.com/beta/deviceManagement/managedDevices"
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

  return graph_results 


def listEntraUsers(access_token, page_size=500, page_limit=100000):
  graph_results = []
  headers = {'Authorization': 'Bearer ' + access_token}
  
  url = "https://graph.microsoft.com/v1.0/users?$select=businessPhones,displayName,givenName,id,jobTitle,mail,mobilePhone,officeLocation,preferredLanguage,surname,userPrincipalName,userType,signInSessionsValidFromDateTime,securityIdentifier,lastPasswordChangeDateTime,externalUserState,createdDateTime,companyName,assignedLicenses,accountEnabled,signInActivity"
  page = 0
  while url:
    try:
      graph_result = requests.get(url=url, headers=headers).json()
      if 'value' in graph_result:
        graph_results.extend(graph_result['value'])

      if ((page < page_limit) and '@odata.nextLink' in graph_result):
        url = graph_result['@odata.nextLink']
        page = page + 1
      else:
        url = None
    except:
      break

  return graph_results


# def listEntraUserSignInActivity(access_token, user_id):
#   graph_result = {}
  
#   headers = {'Authorization': 'Bearer ' + access_token}
#   url = f"https://graph.microsoft.com/v1.0/users/{user_id}?$select=signInActivity"
  
#   throttlingProtection = True
#   while throttlingProtection:
#     try:
#       graph_response = requests.get(url=url, headers=headers)
#       logger.info(graph_response.json())
#       if graph_response.status_code == 429:
#         time.sleep(30)
#       else:
#         graph_result = graph_response.json()
#        throttlingProtection = False
#     except:
#       logger.error(f"Unable to retrieve {user_id} signin activity")
#       throttlingProtection = False
  
#   return graph_result


def listEntraUserAuthenticationMethods(access_token, user_principal_name, pagination=True):
  graph_results = []
  headers = {'Authorization': 'Bearer ' + access_token}
  
  url = f"https://graph.microsoft.com/v1.0/users/{user_principal_name}/authentication/methods"
  throttlingProtection = True
  while throttlingProtection:
    try:
      graph_response = requests.get(url=url, headers=headers)
      if graph_response.status_code == 429:
        time.sleep(5)
      elif graph_response.status_code == 200:
        graph_results.extend(graph_response.json()['value'])
        throttlingProtection = False
      else:
        throttlingProtection = False
      throttlingProtection = False
    except:
      logger.error(f"Unable to retrieve {user_principal_name} authentication methods")
      throttlingProtection = False
  
  return graph_results


def listEntraUsersAuthenticationMethods(credentials, pagination=True):
  token_result = credentials.get_token('https://graph.microsoft.com/.default')

  graph_results = []
  if hasattr(token_result, 'token'):
    headers = {'Authorization': 'Bearer ' + token_result.token}
    
    url = "https://graph.microsoft.com/v1.0/reports/authenticationMethods/userRegistrationDetails"
    while url:
      try:
        graph_result = requests.get(url=url, headers=headers).json()
        if 'value' in graph_result:
          graph_results.extend(graph_result['value'])

        if (pagination == True and '@odata.nextLink' in graph_result):
          url = graph_result['@odata.nextLink']
        else:
          url = None
      except:
        break

  return graph_results


def addEntraGroupMemberById(access_token, groupId, userId):
  headers = {'Authorization': 'Bearer ' + access_token}
  
  url = f"https://graph.microsoft.com/v1.0/groups/{groupId}/members/$ref"
  
  body = {
    "@odata.id": f"https://graph.microsoft.com/v1.0/directoryObjects/{userId}"
  }
  
  try:
    graph_result = requests.post(url=url, headers=headers, json=body)
    if graph_result.status_code == 204:
      print(f"Successfully added user {userId} to group {groupId}")
    elif graph_result.status_code == 400:
      print(f"User {userId} already exists in group {groupId}")
    else:
      print(f"Error when trying to add user {userId} to group {groupId}")
      
  except:
    print(f"Error when trying to add user {userId} to group {groupId}")
