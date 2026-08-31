"""
Intune Device Compliance Setting States Connector for Dataiku DSS

Retrieves all deviceComplianceSettingStates across Microsoft Intune
deviceCompliancePolicySettingStateSummaries with optional filtering by summary IDs.
"""

import logging
from dataiku.connector import Connector
from azure.identity import ClientSecretCredential

from helpers import raise_if_missing_plugin_parameters, listIntuneDeviceComplianceSettingStates, countIntuneDeviceComplianceSettingStates

logger = logging.getLogger(__name__)


class IntuneDeviceComplianceSettingStates(Connector):
    """
    Connector for querying Intune device compliance setting states.
    
    Retrieves all deviceComplianceSettingStates from
    deviceCompliancePolicySettingStateSummaries, optionally filtered by summary IDs.
    Aggregates all setting states across all summaries into a single dataset.
    """

    def __init__(self, config, plugin_config):
        """
        Initialize the connector with configuration.
        
        Args:
            config (dict): Connector-level configuration
            plugin_config (dict): Plugin-level configuration with Azure credentials
        """
        Connector.__init__(self, config, plugin_config)

        # Validate mandatory plugin parameters
        raise_if_missing_plugin_parameters(self.plugin_config)

        # Initialize Azure authentication
        tenant_id = self.plugin_config.get('tenant_id')
        client_id = self.plugin_config.get('client_id')
        client_secret = self.plugin_config.get('client_secret')
        self.credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        self._access_token = None

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
            if (self._access_token is None) or (self._access_token and self._access_token.expires_on < time.time()):
                token = self.credential.get_token("https://graph.microsoft.com/.default")
                self._access_token = token
                logger.info("Access token acquired successfully")
            
            return self._access_token.token
        
        except Exception as e:
            logger.error(f"Failed to acquire access token: {e}")
            raise RuntimeError(f"Token acquisition failed: {e}") from e

    def _parse_summary_ids(self):
        """
        Parse summary IDs from connector configuration.
        
        Returns:
            list: List of summary IDs, or None if not provided
        """
        summary_ids_text = self.config.get('summary_ids', '').strip()
        if not summary_ids_text:
            return None
        
        # Parse one ID per line
        summary_ids = [id_str.strip() for id_str in summary_ids_text.split('\n') 
                      if id_str.strip()]
        
        if summary_ids:
            logger.info(f"Filtering to {len(summary_ids)} specific summary IDs")
        
        return summary_ids if summary_ids else None

    def get_read_schema(self):
        """
        Return read schema. Schema is dynamic based on API response.
        
        Returns:
            None: Schema is determined dynamically from API
        """
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, 
                     partition_id=None, records_limit=-1):
        """
        Generate rows from Intune device compliance setting states.
        
        Args:
            dataset_schema: Dataset schema (not used)
            dataset_partitioning: Partitioning info (not used)
            partition_id: Partition ID (not used)
            records_limit (int): Maximum records to retrieve (-1 = unlimited)
        
        Yields:
            dict: Each deviceComplianceSettingState record
        """
        try:
            if records_limit > 0:
                logger.info(f"Building sample dataset - retrieving only {records_limit} records")

            # Get access token
            access_token = self._get_access_token()

            # Parse optional summary IDs filter
            summary_ids = self._parse_summary_ids()

            # Get optional $select parameter
            query_select = self.config.get('query_select', '').strip() or None

            # Query the API
            results = listIntuneDeviceComplianceSettingStates(
                access_token=access_token,
                summary_ids=summary_ids,
                query_select=query_select,
                pagination=True,
                records_limit=records_limit
            )

            logger.info(f"Retrieved {len(results)} compliance setting states")

            # Yield each record
            for record in results:
                yield record

        except Exception as e:
            logger.error(f"Error during data retrieval: {e}")
            raise

    def get_writer(self, dataset_schema=None, dataset_partitioning=None, 
                  partition_id=None, write_mode="OVERWRITE"):
        """Get writer. Not supported - this is read-only."""
        raise NotImplementedError("This connector is read-only")

    def get_partitioning(self):
        """Get partitioning. Not supported."""
        raise NotImplementedError("Partitioning not supported")

    def list_partitions(self, partitioning):
        """List partitions. Returns empty list."""
        return []

    def partition_exists(self, partitioning, partition_id):
        """Check partition exists. Not supported."""
        raise NotImplementedError("Partitioning not supported")

    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Get record count efficiently using $count query parameter.
        
        Uses $count=true for deviceComplianceSettingStates to avoid fetching
        all records - just counts them, which is much faster and requires minimal bandwidth.
        
        Returns:
            int: Total number of compliance setting states
        """
        try:
            logger.info("Counting total compliance setting states using $count")

            access_token = self._get_access_token()
            summary_ids = self._parse_summary_ids()
            query_select = self.config.get('query_select', '').strip() or None

            count = countIntuneDeviceComplianceSettingStates(
                access_token=access_token,
                summary_ids=summary_ids,
                query_select=query_select
            )

            logger.info(f"Total record count: {count}")
            return count

        except Exception as e:
            logger.error(f"Error counting records: {e}")
            raise
