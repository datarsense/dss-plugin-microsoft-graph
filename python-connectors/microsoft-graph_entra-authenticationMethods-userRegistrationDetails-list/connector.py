# import the base class for the custom dataset
from six.moves import xrange
from dataiku.connector import Connector

from helpers import raise_if_missing_plugin_parameters, listEntraUsersAuthenticationMethods
from azure.identity import ClientSecretCredential


class ListEntraUsersAuthenticationMethods(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        # Raise an error if mandatory plugin parameters are missing
        raise_if_missing_plugin_parameters(self.plugin_config)

        # Define Microsoft Graph API connection settings
        tenant_id = self.plugin_config.get('tenant_id')
        client_id = self.plugin_config.get('client_id')
        client_secret = self.plugin_config.get('client_secret')
        self.purview_credentials = ClientSecretCredential(tenant_id, client_id, client_secret)


    def get_read_schema(self):
        return None

    
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
        result = listEntraUsersAuthenticationMethods(self.purview_credentials)
 
        for user in  result:
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
        raise NotImplementedError