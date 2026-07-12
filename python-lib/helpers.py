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


def listIntuneDeviceComplianceSettingStates(access_token, summary_ids=None, query_select=None, 
                                            pagination=True, records_limit=-1):
    """
    Query Intune deviceCompliancePolicySettingStateSummaries and retrieve all deviceComplianceSettingStates.
    
    Returns deviceComplianceSettingState objects with the following fields:
    - id: Unique identifier of the setting state
    - setting: Name of the compliance setting
    - settingName: Display name of the compliance setting
    - deviceId: Unique identifier of the device
    - deviceName: Name/hostname of the device
    - userId: Unique identifier of the user
    - userEmail: Email address of the user
    - userName: Display name of the user
    - userPrincipalName: User principal name (UPN)
    - deviceModel: Model/type of the device
    - state: Compliance state (compliant, noncompliant, notApplicable, error, etc.)
    - complianceGracePeriodExpirationDateTime: When grace period expires (if applicable)
    - summaryId: ID of the parent deviceCompliancePolicySettingStateSummary (added for reference)
    
    Args:
        access_token (str): Valid Microsoft Graph API access token
        summary_ids (list): List of deviceCompliancePolicySettingStateSummaryIds to filter by. 
                           If None, retrieves all summaries.
        query_select (str): OData $select parameter for deviceComplianceSettingStates fields
        pagination (bool): Whether to follow pagination tokens
        records_limit (int): Maximum number of compliance setting states to retrieve (-1 = unlimited)
    
    Returns:
        list: Aggregated list of deviceComplianceSettingState objects from all queried summaries
        
    Raises:
        RuntimeError: If API requests fail
    """
    graph_results = []
    graph_results_count = 0
    headers = {'Authorization': 'Bearer ' + access_token}
    
    try:
        # Step 1: Get all deviceCompliancePolicySettingStateSummaries (or filter to specific IDs)
        summaries = []
        summaries_url = "https://graph.microsoft.com/v1.0/deviceManagement/deviceCompliancePolicySettingStateSummaries"
        
        logger.info("Retrieving device compliance policy setting state summaries...")
        
        while summaries_url:
            try:
                summaries_response = requests.get(url=summaries_url, headers=headers).json()
                if 'value' in summaries_response:
                    summaries.extend(summaries_response['value'])
                
                # Pagination for summaries
                if pagination and '@odata.nextLink' in summaries_response:
                    summaries_url = summaries_response['@odata.nextLink']
                else:
                    summaries_url = None
            except Exception as e:
                logger.error(f"Failed to retrieve compliance policy setting state summaries: {e}")
                break
        
        # Filter summaries by IDs if provided
        original_summary_count = len(summaries)
        if summary_ids:
            summaries = [s for s in summaries if s.get('id') in summary_ids]
            logger.info(f"Filtered from {original_summary_count} to {len(summaries)} summaries matching provided IDs")
        else:
            logger.info(f"Retrieved {len(summaries)} compliance policy setting state summaries")
        
        if not summaries:
            logger.warning("No summaries found matching criteria")
            return graph_results
        
        # Step 2: For each summary, retrieve its deviceComplianceSettingStates
        summaries_processed = 0
        for summary in summaries:
            summary_id = summary.get('id')
            if not summary_id:
                logger.warning("Summary found without ID, skipping")
                continue
            
            # Build query parameters
            query_params = {}
            if query_select:
                query_params['$select'] = query_select
            
            # Construct states URL for this summary
            states_url = f"https://graph.microsoft.com/v1.0/deviceManagement/deviceCompliancePolicySettingStateSummaries/{summary_id}/deviceComplianceSettingStates"
            
            logger.debug(f"Retrieving compliance setting states for summary {summary_id}...")
            
            # Paginate through all states for this summary
            while states_url and (records_limit == -1 or graph_results_count < records_limit):
                try:
                    states_response = requests.get(url=states_url, headers=headers, params=query_params).json()
                    
                    if 'value' in states_response:
                        states_batch = states_response['value']
                        
                        # Enforce record limit
                        if records_limit > 0:
                            remaining = records_limit - graph_results_count
                            if len(states_batch) > remaining:
                                states_batch = states_batch[:remaining]
                        
                        # Add summary_id to each state for reference
                        for state in states_batch:
                            state['summaryId'] = summary_id
                        
                        graph_results.extend(states_batch)
                        graph_results_count += len(states_batch)
                        
                        logger.info(
                            f"Retrieved {len(states_batch)} compliance setting states from summary {summary_id} "
                            f"(total across all summaries: {graph_results_count})"
                        )
                    
                    # Pagination for states
                    if pagination and '@odata.nextLink' in states_response and (records_limit == -1 or graph_results_count < records_limit):
                        states_url = states_response['@odata.nextLink']
                    else:
                        states_url = None
                        
                except Exception as e:
                    logger.error(f"Failed to retrieve compliance setting states for summary {summary_id}: {e}")
                    states_url = None
            
            summaries_processed += 1
            
            # Stop processing more summaries if record limit reached
            if records_limit > 0 and graph_results_count >= records_limit:
                logger.info(f"Record limit of {records_limit} reached after processing {summaries_processed} of {len(summaries)} summaries")
                break
        
        logger.info(f"Successfully retrieved {len(graph_results)} total compliance setting states from {summaries_processed} summaries")
        return graph_results
        
    except Exception as e:
        logger.error(f"Error retrieving device compliance setting states: {e}")
        raise RuntimeError(f"Failed to retrieve device compliance setting states: {e}") from e


def countIntuneDeviceComplianceSettingStates(access_token, summary_ids=None, query_select=None):
    """
    Count total deviceComplianceSettingStates efficiently using $count query parameter.
    
    Retrieves summaries first (to get IDs and filter if needed), then counts states
    for each summary using $count=true instead of fetching all state objects.
    This is significantly faster and requires minimal bandwidth.
    
    Args:
        access_token (str): Valid Microsoft Graph API access token
        summary_ids (list): List of deviceCompliancePolicySettingStateSummaryIds to filter by.
                           If None, retrieves all summaries.
        query_select (str): OData $select parameter (unused for count, included for API consistency)
    
    Returns:
        int: Total count of deviceComplianceSettingStates across all queried summaries
        
    Raises:
        RuntimeError: If API requests fail
    """
    headers = {'Authorization': 'Bearer ' + access_token}
    total_count = 0
    
    try:
        # Step 1: Get all deviceCompliancePolicySettingStateSummaries (or filter to specific IDs)
        summaries = []
        summaries_url = "https://graph.microsoft.com/v1.0/deviceManagement/deviceCompliancePolicySettingStateSummaries"
        
        logger.info("Retrieving device compliance policy setting state summaries for counting...")
        
        while summaries_url:
            try:
                summaries_response = requests.get(url=summaries_url, headers=headers).json()
                if 'value' in summaries_response:
                    summaries.extend(summaries_response['value'])
                
                # Pagination for summaries
                if '@odata.nextLink' in summaries_response:
                    summaries_url = summaries_response['@odata.nextLink']
                else:
                    summaries_url = None
            except Exception as e:
                logger.error(f"Failed to retrieve compliance policy setting state summaries: {e}")
                raise RuntimeError(f"Failed to retrieve summaries for counting: {e}") from e
        
        # Filter summaries by IDs if provided
        original_summary_count = len(summaries)
        if summary_ids:
            summaries = [s for s in summaries if s.get('id') in summary_ids]
            logger.info(f"Filtered from {original_summary_count} to {len(summaries)} summaries for counting")
        else:
            logger.info(f"Retrieved {len(summaries)} compliance policy setting state summaries for counting")
        
        if not summaries:
            logger.warning("No summaries found matching criteria")
            return 0
        
        # Step 2: For each summary, use $count=true to get count without fetching data
        for summary in summaries:
            summary_id = summary.get('id')
            if not summary_id:
                logger.warning("Summary found without ID, skipping")
                continue
            
            # Construct count URL for this summary
            count_url = f"https://graph.microsoft.com/v1.0/deviceManagement/deviceCompliancePolicySettingStateSummaries/{summary_id}/deviceComplianceSettingStates"
            
            try:
                # Use $count=true to get count as integer string
                count_response = requests.get(
                    url=count_url,
                    headers=headers,
                    params={'$count': 'true'}
                )
                count_response.raise_for_status()
                
                # $count=true returns integer count as text
                summary_count = int(count_response.text)
                total_count += summary_count
                
                logger.debug(f"Summary {summary_id}: {summary_count} compliance setting states")
                
            except Exception as e:
                logger.error(f"Failed to count compliance setting states for summary {summary_id}: {e}")
                raise RuntimeError(f"Failed to count states for summary {summary_id}: {e}") from e
        
        logger.info(f"Total compliance setting states count: {total_count} from {len(summaries)} summaries")
        return total_count
        
    except Exception as e:
        logger.error(f"Error counting device compliance setting states: {e}")
        raise RuntimeError(f"Failed to count device compliance setting states: {e}") from e
