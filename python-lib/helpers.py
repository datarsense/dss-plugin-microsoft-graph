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


# This function wraps around standard requests calls and incorporates several best practices:
# Automatic Retries: Retries failed requests a configurable number of times.
# Exponential Backoff: Gradually increases the delay between retries (e.g., 1s, 2s, 4s…) to avoid overwhelming the API.
# Throttling Awareness: Specifically listens for 429, 503, and 504 status codes.
# Retry-After Header Support: If the API provides a Retry-After header (common with 429 responses), the function respects this specific delay.
# Session Management: Utilizes requests.Session() for connection pooling, improving performance for multiple calls to the same host.
# Clear Logging: Provides informative logs for each retry attempt and error.
def perform_request_with_retry(method: str, url: str, access_token: str, params: dict = None, json_payload: dict = None, max_retries: int = 5, session: requests.Session = None) -> requests.Response:
    """
    Performs an HTTP request with retries for throttling and transient errors.
    Uses a provided session or creates a new one.
    """
    local_session = session or requests.Session()
    delay = 1 # Initial delay in seconds
    headers = {'Authorization': 'Bearer ' + access_token}

    for attempt in range(max_retries):
        try:
            response = None
            if method.upper() == "GET":
                response = local_session.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                response = local_session.post(url, headers=headers, json=json_payload, params=params)
            elif method.upper() == "PATCH":
                response = local_session.patch(url, headers=headers, json=json_payload, params=params)
            elif method.upper() == "DELETE":
                response = local_session.delete(url, headers=headers, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if response.status_code in [429, 503, 504]: # Throttling or temporary server issues
                retry_after = int(response.headers.get("Retry-After", delay))
                logger.warning(
                    f"Request to {url} {method} Throttled/Unavailable (Status {response.status_code}). "
                    f"Retrying in {retry_after}s (Attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(retry_after)
                delay = min(delay * 2, 60) # Exponential backoff, max 60s
                continue # Retry the loop

            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx) not handled above
            return response

        except requests.exceptions.RequestException as e: # Catches network errors, timeouts, etc.
            logger.warning(
                f"Request to {url} {method} failed: {e} (Attempt {attempt + 1}/{max_retries})"
            )
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                logger.error(
                    f"Max retries reached for {url} {method}. Last error: {e}"
                )
                raise # Re-raise the last exception after max retries
        finally:
            if not session and local_session: # If session was created locally, close it
                local_session.close()
                
    raise Exception(f"Request {method} {url} failed after {max_retries} retries without a conclusive response or error.")


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


# Get a list of the group's direct members.
# A group can have users, organizational contacts, devices, service principals and other groups as members
def listEntraGroupMembers(access_token, group_id, pagination = True, records_limit=-1):
  graph_results = []
  graph_results_count = 0
  headers = {'Authorization': 'Bearer ' + access_token}
  
  url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members"
  while url and (records_limit == -1 or (records_limit > 0 and graph_results_count <= records_limit)):
    try:
      graph_result = requests.get(url=url, headers=headers).json()
      graph_results.extend(graph_result['value'])
      graph_results_count += len(graph_result['value'])
      if (pagination):
        url = graph_result['@odata.nextLink']
      else:
        url = None
    except:
      break

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


# Retrieve a list of authentication methods registered to a user. 
# The authentication methods are defined by the types derived from the authenticationMethod resource type
def listEntraUserAuthenticationMethodsDetails(access_token, user_identifier, pagination=True):
  graph_results = []
  headers = {'Authorization': 'Bearer ' + access_token}
  
  url = f"https://graph.microsoft.com/v1.0/users/{user_identifier}/authentication/methods"
  # throttlingProtection = True
  # while throttlingProtection:
  #   try:
  #     graph_response = requests.get(url=url, headers=headers)
  #     if graph_response.status_code == 429:
  #       h = graph_response.headers
  #       time.sleep(5)

  #     elif graph_response.status_code == 200:
  #       graph_results.extend(graph_response.json()['value'])
  #       throttlingProtection = False

  #     else:
  #       throttlingProtection = False
  #     throttlingProtection = False
  #   except:
  #     logger.error(f"Unable to retrieve {user_principal_name} authentication methods")
  #     throttlingProtection = False
  
  graph_response = perform_request_with_retry(method="GET", access_token=access_token, url=url)
  graph_results.extend(graph_response.json()['value'])
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
      print(f"Error when trying to add user {userId} to group {groupId}. Status code:{graph_result.status_code}. Graph API response: {graph_result.json()}")
      
  except Exception as e:
    print(f"Error when trying to add user {userId} to group {groupId}: {e}")
