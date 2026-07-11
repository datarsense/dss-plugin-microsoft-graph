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
def perform_request_with_retry(method: str, url: str, access_token: str, params: dict = None, json_payload: dict = None, custom_headers: dict = None, max_retries: int = 5, session: requests.Session = None) -> requests.Response:
    """
    Performs an HTTP request with retries for throttling and transient errors.
    Uses a provided session or creates a new one.
    """
    local_session = session or requests.Session()
    delay = 1 # Initial delay in seconds
    headers = {'Authorization': 'Bearer ' + access_token}
    
    # Merge custom headers if provided
    if custom_headers and isinstance(custom_headers, dict):
        headers.update(custom_headers)

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
      print(f"Successfully added directory object {userId} to group {groupId}")
    elif graph_result.status_code == 400:
      print(f"Object {userId} already exists in group {groupId}")
    else:
      print(f"Error when trying to add directory object {userId} to group {groupId}. Status code:{graph_result.status_code}. Graph API response: {graph_result.json()}")
      
  except Exception as e:
    print(f"Error when trying to add directory object {userId} to group {groupId}: {e}")


def queryGenericGraphAPI(access_token: str, api_endpoint: str, api_version: str = "v1.0", 
                         http_method: str = "GET", query_params: dict = None, 
                         request_body: dict = None, custom_headers: dict = None, 
                         pagination_enabled: bool = True, records_limit: int = -1,
                         max_retries: int = 5, session: requests.Session = None) -> list:
    """
    Generic function to query Microsoft Graph API endpoints with flexible parameters.
    
    Args:
        access_token (str): Valid Microsoft Graph API access token
        api_endpoint (str): API endpoint path (e.g., "/users", "/groups/{id}/members", "/me/drive/root/children")
        api_version (str): API version to use ("v1.0" or "beta"), defaults to "v1.0"
        http_method (str): HTTP method to use ("GET", "POST", "PATCH", "DELETE"), defaults to "GET"
        query_params (dict): OData query parameters (e.g., {"$select": "id,displayName", "$filter": "..."}), optional
        request_body (dict): JSON body for POST/PATCH requests, optional
        custom_headers (dict): Additional HTTP headers to include, optional
        pagination_enabled (bool): Whether to follow @odata.nextLink for pagination, defaults to True
        records_limit (int): Maximum number of records to retrieve. -1 means unlimited, defaults to -1
        max_retries (int): Maximum number of retry attempts for transient failures, defaults to 5
        session (requests.Session): Reusable session object for connection pooling, optional
    
    Returns:
        list: List of result objects from the API response's 'value' field
    
    Raises:
        ValueError: If api_endpoint is empty or invalid
        RuntimeError: If API request fails after max retries
        Exception: For invalid HTTP methods or other API errors
    """
    if not api_endpoint or not isinstance(api_endpoint, str):
        raise ValueError("api_endpoint must be a non-empty string")
    
    if api_version not in ["v1.0", "beta"]:
        raise ValueError("api_version must be either 'v1.0' or 'beta'")
    
    http_method = http_method.upper()
    if http_method not in ["GET", "POST", "PATCH", "DELETE"]:
        raise ValueError(f"http_method must be one of: GET, POST, PATCH, DELETE (got: {http_method})")
    
    # Build the full URL
    base_url = "https://graph.microsoft.com"
    if not api_endpoint.startswith("/"):
        api_endpoint = "/" + api_endpoint
    url = f"{base_url}/{api_version}{api_endpoint}"
    
    local_session = session or requests.Session()
    
    graph_results = []
    records_retrieved = 0
    delay = 1  # Initial delay for backoff
    
    try:
        while url and (records_limit == -1 or records_retrieved < records_limit):
            try:
                # Perform request with retry logic
                response = perform_request_with_retry(
                    method=http_method,
                    url=url,
                    access_token=access_token,
                    params=query_params,
                    json_payload=request_body,
                    custom_headers=custom_headers,
                    max_retries=max_retries,
                    session=local_session
                )
                
                graph_response = response.json()
                
                # Extract results from 'value' field if present
                if 'value' in graph_response and isinstance(graph_response['value'], list):
                    results_batch = graph_response['value']
                    
                    # Enforce records_limit by slicing the batch if needed
                    if records_limit > 0:
                        remaining_records = records_limit - records_retrieved
                        if len(results_batch) > remaining_records:
                            results_batch = results_batch[:remaining_records]
                    
                    graph_results.extend(results_batch)
                    records_retrieved += len(results_batch)
                    
                    logger.info(
                        f"Retrieved {len(results_batch)} records from {api_endpoint} "
                        f"(total: {records_retrieved})"
                    )
                else:
                    # If response doesn't have 'value' field, return entire response as single item
                    graph_results.append(graph_response)
                    records_retrieved += 1
                    logger.info(f"Retrieved non-paginated response from {api_endpoint}")
                
                # Handle pagination
                if pagination_enabled and '@odata.nextLink' in graph_response:
                    if records_limit == -1 or records_retrieved < records_limit:
                        url = graph_response['@odata.nextLink']
                        logger.debug(f"Following pagination link for {api_endpoint}")
                    else:
                        url = None  # Stop pagination if we've reached records_limit
                else:
                    url = None  # No more pages
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to query {api_endpoint}: {e}")
                raise RuntimeError(f"API request failed: {e}") from e
            except (KeyError, ValueError) as e:
                logger.error(f"Error parsing response from {api_endpoint}: {e}")
                raise RuntimeError(f"Failed to parse API response: {e}") from e
                
    finally:
        if not session and local_session:
            local_session.close()
    
    logger.info(f"Successfully retrieved {len(graph_results)} records from {api_endpoint}")
    return graph_results
