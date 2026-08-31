"""
Generic Microsoft Graph API Connector for Dataiku DSS

This connector provides flexible access to any Microsoft Graph API endpoint,
allowing users to query v1.0 or beta API endpoints with customizable parameters
including HTTP method, OData query parameters, custom headers, and request body.

Supports:
- GET, POST, PATCH, DELETE HTTP methods
- OData query parameters ($select, $filter, $top, $search)
- Custom HTTP headers
- Request body for POST/PATCH operations
- Automatic pagination via @odata.nextLink
- Record limiting for dataset previews
- Retry logic for transient failures
"""

import logging
import json
from math import ceil
from dataiku.connector import Connector
from azure.identity import ClientSecretCredential

from helpers import raise_if_missing_plugin_parameters, queryGenericGraphAPI

logger = logging.getLogger(__name__)


class GenericGraphAPIConnector(Connector):
    """
    Generic connector for querying Microsoft Graph API endpoints.
    
    Supports flexible configuration of API endpoint, HTTP method, and query parameters.
    Automatically handles pagination and respects record limits for dataset previews.
    """

    def __init__(self, config, plugin_config):
        """
        Initialize the connector with configuration.
        
        Args:
            config (dict): Connector-level configuration including API parameters
            plugin_config (dict): Plugin-level configuration including Azure credentials
        """
        Connector.__init__(self, config, plugin_config)

        # Validate mandatory plugin parameters (Azure credentials)
        raise_if_missing_plugin_parameters(self.plugin_config)

        # Initialize Azure authentication
        tenant_id = self.plugin_config.get('tenant_id')
        client_id = self.plugin_config.get('client_id')
        client_secret = self.plugin_config.get('client_secret')
        self.credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        self._access_token = None

        # Validate connector parameters
        self._validate_config()

    def _validate_config(self):
        """
        Validate connector configuration parameters.
        
        Raises:
            ValueError: If required parameters are missing or invalid
        """
        if not self.config.get('api_endpoint'):
            raise ValueError("api_endpoint is mandatory and cannot be empty")

        api_endpoint = self.config.get('api_endpoint', '').strip()
        if not api_endpoint:
            raise ValueError("api_endpoint cannot be empty or whitespace-only")

        api_version = self.config.get('api_version', 'v1.0')
        if api_version not in ['v1.0', 'beta']:
            raise ValueError(f"api_version must be 'v1.0' or 'beta', got: {api_version}")

        http_method = self.config.get('http_method', 'GET').upper()
        if http_method not in ['GET', 'POST', 'PATCH', 'DELETE']:
            raise ValueError(f"http_method must be GET, POST, PATCH, or DELETE, got: {http_method}")

        # Validate JSON fields
        if self.config.get('custom_headers'):
            try:
                json.loads(self.config.get('custom_headers'))
            except json.JSONDecodeError:
                raise ValueError("custom_headers must be valid JSON")

        if self.config.get('request_body'):
            try:
                json.loads(self.config.get('request_body'))
            except json.JSONDecodeError:
                raise ValueError("request_body must be valid JSON")

    def _get_access_token(self) -> str:
        """
        Acquire or refresh an access token for Microsoft Graph API.
        
        Returns:
            str: Valid access token
            
        Raises:
            RuntimeError: If token acquisition fails
        """
        import time
        try:
            # Check if token exists and hasn't expired
            if (self._access_token is None) or (self._access_token and self._access_token.expires_on < time.time()):
                token = self.credential.get_token("https://graph.microsoft.com/.default")
                self._access_token = token
                logger.info("Access token acquired successfully")
            
            return self._access_token.token
        
        except Exception as e:
            logger.error(f"Failed to acquire access token: {e}")
            raise RuntimeError(f"Token acquisition failed: {e}") from e

    def _build_query_params(self) -> dict:
        """
        Build OData query parameters from connector configuration.
        
        Returns:
            dict: Dictionary of OData query parameters for the API request
        """
        query_params = {}

        # Add $select parameter
        select = self.config.get('query_select', '').strip()
        if select:
            query_params['$select'] = select
            logger.debug(f"Added $select: {select}")

        # Add $filter parameter
        filter_expr = self.config.get('query_filter', '').strip()
        if filter_expr:
            query_params['$filter'] = filter_expr
            logger.debug(f"Added $filter: {filter_expr}")

        # Add $top parameter
        top = self.config.get('query_top')
        if top:
            try:
                top_int = int(top)
                if top_int > 0:
                    query_params['$top'] = top_int
                    logger.debug(f"Added $top: {top_int}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid $top value: {top}, ignoring")

        # Add $search parameter
        search = self.config.get('query_search', '').strip()
        if search:
            query_params['$search'] = f'"{search}"'  # Must be quoted
            logger.debug(f"Added $search: {search}")

        return query_params

    def _parse_custom_headers(self) -> dict:
        """
        Parse custom HTTP headers from JSON string in configuration.
        
        Returns:
            dict: Dictionary of custom headers, or empty dict if none provided
        """
        headers_json = self.config.get('custom_headers', '').strip()
        if not headers_json:
            return {}

        try:
            headers = json.loads(headers_json)
            if not isinstance(headers, dict):
                logger.warning("custom_headers must be a JSON object, ignoring")
                return {}
            logger.debug(f"Parsed custom headers: {list(headers.keys())}")
            return headers
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse custom_headers JSON: {e}, ignoring")
            return {}

    def _parse_request_body(self) -> dict:
        """
        Parse request body from JSON string in configuration.
        
        Returns:
            dict: Request body as dictionary, or None if not provided
        """
        body_json = self.config.get('request_body', '').strip()
        if not body_json:
            return None

        try:
            body = json.loads(body_json)
            if not isinstance(body, dict):
                logger.warning("request_body must be a JSON object, ignoring")
                return None
            logger.debug(f"Parsed request body with {len(body)} fields")
            return body
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse request_body JSON: {e}, ignoring")
            return None

    def get_read_schema(self):
        """
        Return read schema. Dynamic - depends on API endpoint and $select parameter.
        
        Returns:
            None: Schema is dynamic based on the API response
        """
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=-1):
        """
        Generate rows from the Microsoft Graph API query.
        
        Args:
            dataset_schema: Dataset schema (not used)
            dataset_partitioning: Partitioning info (not used)
            partition_id: Partition ID (not used)
            records_limit (int): Maximum records to retrieve. -1 = unlimited. Used for preview.
        
        Yields:
            dict: Each record from the API response
        """
        try:
            logger.info(f"Starting data retrieval with records_limit={records_limit}")

            # Extract configuration
            api_endpoint = self.config.get('api_endpoint', '').strip()
            api_version = self.config.get('api_version', 'v1.0')
            http_method = self.config.get('http_method', 'GET').upper()

            if records_limit > 0:
                logger.info(f"Building sample dataset - retrieving only {records_limit} records")

            # Get access token
            access_token = self._get_access_token()

            # Build query parameters
            query_params = self._build_query_params()

            # Parse custom headers and request body
            custom_headers = self._parse_custom_headers()
            request_body = self._parse_request_body()

            # Query the API
            results = queryGenericGraphAPI(
                access_token=access_token,
                api_endpoint=api_endpoint,
                api_version=api_version,
                http_method=http_method,
                query_params=query_params,
                request_body=request_body,
                custom_headers=custom_headers,
                pagination_enabled=True,
                records_limit=records_limit
            )

            logger.info(f"Retrieved {len(results)} records from API")

            # Yield each record
            for record in results:
                yield record

        except Exception as e:
            logger.error(f"Error during data retrieval: {e}")
            raise

    def get_writer(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, write_mode="OVERWRITE"):
        """
        Get writer for write operations. Not supported for this read-only connector.
        
        Raises:
            NotImplementedError: Always - this is a read-only connector
        """
        raise NotImplementedError("This connector is read-only")

    def get_partitioning(self):
        """
        Get partitioning info. Not supported for this connector.
        
        Raises:
            NotImplementedError: Always - partitioning not supported
        """
        raise NotImplementedError("Partitioning not supported")

    def list_partitions(self, partitioning):
        """
        List partitions. Not supported for this connector.
        
        Returns:
            list: Empty list - no partitioning
        """
        return []

    def partition_exists(self, partitioning, partition_id):
        """
        Check if partition exists. Not supported for this connector.
        
        Raises:
            NotImplementedError: Always - partitioning not supported
        """
        raise NotImplementedError("Partitioning not supported")

    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Get record count. Queries the API without record limit.
        
        Note: This operation may take a long time for large result sets due to pagination.
        
        Returns:
            int: Total number of records returned by the API
        """
        try:
            logger.info("Counting total records (may take time for large datasets)")

            api_endpoint = self.config.get('api_endpoint', '').strip()
            api_version = self.config.get('api_version', 'v1.0')
            http_method = self.config.get('http_method', 'GET').upper()

            access_token = self._get_access_token()
            query_params = self._build_query_params()
            custom_headers = self._parse_custom_headers()
            request_body = self._parse_request_body()

            results = queryGenericGraphAPI(
                access_token=access_token,
                api_endpoint=api_endpoint,
                api_version=api_version,
                http_method=http_method,
                query_params=query_params,
                request_body=request_body,
                custom_headers=custom_headers,
                pagination_enabled=True,
                records_limit=-1  # No limit for full count
            )

            count = len(results)
            logger.info(f"Total record count: {count}")
            return count

        except Exception as e:
            logger.error(f"Error counting records: {e}")
            raise
