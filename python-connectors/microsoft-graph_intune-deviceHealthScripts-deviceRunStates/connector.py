# import the base class for the custom dataset
import json
import logging
import time

from math import *
from six.moves import xrange
from dataiku.connector import Connector

from helpers import raise_if_missing_plugin_parameters, listIntuneDeviceHealthScriptDeviceRunStates
from azure.identity import ClientSecretCredential

logger = logging.getLogger(__name__)

class GetIntuneDeviceHealthScriptdeviceRunStates(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        # Raise an error if mandatory plugin parameters are missing
        raise_if_missing_plugin_parameters(self.plugin_config)

        # Define Microsoft Graph API connection settings
        tenant_id = self.plugin_config.get('tenant_id')
        client_id = self.plugin_config.get('client_id')
        client_secret = self.plugin_config.get('client_secret')
        self.credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        self._access_token = None
        self.deviceHealthScriptId = self.config.get('deviceHealthScriptId')


    def _get_access_token(self) -> str:
        """
        Acquire an access token for Microsoft Graph API.
        
        Returns:
            str: Valid access token.
            
        Raises:
            RuntimeError: If token acquisition fails.
        """
        try:
            if (self._access_token is None) or (self._access_token and self._access_token.expires_on < time.time()):
                token = self.credential.get_token(
                    "https://graph.microsoft.com/.default"
                )
                self._access_token = token

            return self._access_token.token
        
        except Exception as e:
            logger.error(f"Failed to acquire access token: {e}")
            raise RuntimeError(f"Token acquisition failed: {e}") from e


    def get_read_schema(self):
        return {
            "columns": [
                {"name": "deviceId", "type": "string"},
                {"name": "deviceName", "type": "string"},
                {"name": "managedDevice", "type": "string"},
                {"name": "detectionState", "type": "string"},
                {"name": "remediationState", "type": "string"},
                {"name": "preRemediationDetectionScriptOutput", "type": "string"},
                {"name": "preRemediationDetectionScriptError", "type": "string"},
                {"name": "postRemediationDetectionScriptOutput", "type": "string"},
                {"name": "remediationScriptError", "type": "string"},
                {"name": "lastStateUpdateDateTime", "type": "date"}
            ]
        }

    
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
        result = listIntuneDeviceHealthScriptDeviceRunStates(self._get_access_token(), deviceHealthScriptId=self.deviceHealthScriptId, records_limit=records_limit)
        
        for deviceRunStates in result:              
            managed_device = deviceRunStates.get("managedDevice") or {}
            
            yield {
                "deviceId": deviceRunStates.get("id"),
                "deviceName": managed_device.get("deviceName", ""),
                "managedDevice": json.dumps(managed_device),
                "detectionState": deviceRunStates.get("detectionState",),
                "remediationState": deviceRunStates.get("remediationState"),
                "preRemediationDetectionScriptOutput": self._cleanup_string(deviceRunStates.get("preRemediationDetectionScriptOutput")), # Removed non alphanumeric chars in free text properties to avoid Dataiku parsing errors
                "preRemediationDetectionScriptError": self._cleanup_string(deviceRunStates.get("preRemediationDetectionScriptError")), # Removed non alphanumeric chars in free text properties to avoid Dataiku parsing errors
                "postRemediationDetectionScriptOutput": self._cleanup_string(deviceRunStates.get("postRemediationDetectionScriptOutput")), # Removed non alphanumeric chars in free text properties to avoid Dataiku parsing errors
                "remediationScriptError": self._cleanup_string(deviceRunStates.get("remediationScriptError")), # Removed non alphanumeric chars in free text properties to avoid Dataiku parsing errors
                "lastStateUpdateDateTime": deviceRunStates.get("lastStateUpdateDateTime")
            }


    def get_writer(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, write_mode="OVERWRITE"):
        raise NotImplementedError


    def get_partitioning(self):
        raise NotImplementedError


    def list_partitions(self, partitioning):
        return []


    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError


    def get_records_count(self, partitioning=None, partition_id=None):
         result = listIntuneDeviceHealthScriptDeviceRunStates(self._get_access_token(), deviceHealthScriptId=self.deviceHealthScriptId)
         return len(result)
     
    def _cleanup_string(self, s):
        return (''.join(e for e in s if e.isalnum()))