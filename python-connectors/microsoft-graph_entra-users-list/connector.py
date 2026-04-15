# import the base class for the custom dataset
import logging
import time

from math import *
from six.moves import xrange
from dataiku.connector import Connector

from helpers import raise_if_missing_plugin_parameters, listEntraUsers, listEntraUserAuthenticationMethods
from azure.identity import ClientSecretCredential

logger = logging.getLogger(__name__)

class ListEntraUsers(Connector):

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
        self.getAuthenticationMethods = self.config.get('getAuthenticationMethods')


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
        return None

    
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
        if (records_limit > 0):
            if(records_limit >= 500):
                page_size = 500
                page_count = ceil(records_limit/page_size)
            elif (records_limit > 0 and records_limit < 500):
                page_size = records_limit
                page_count = 1
            result = listEntraUsers(self._get_access_token(), page_size, page_count)
        else:
            result = listEntraUsers(self._get_access_token())
        
        
 
        for user in result:
            user_row = user
            
            # Enrich data with user registered authentication methods
            #user_row["signInActivity"] = listEntraUserSignInActivity(self._get_access_token(), user["id"])
            if self.getAuthenticationMethods:
                user_row["authenticationMethods"] = listEntraUserAuthenticationMethods(self._get_access_token(), user["userPrincipalName"])
                
            yield user


    def get_writer(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, write_mode="OVERWRITE"):
        raise NotImplementedError


    def get_partitioning(self):
        raise NotImplementedError


    def list_partitions(self, partitioning):
        return []


    def partition_exists(self, partitioning, partition_id):
        raise NotImplementedError


    def get_records_count(self, partitioning=None, partition_id=None):
         result = listEntraUsers(self._get_access_token())
         return len(result)