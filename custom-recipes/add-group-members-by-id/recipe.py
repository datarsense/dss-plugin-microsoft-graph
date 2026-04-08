
# -*- coding: utf-8 -*-
import dataiku
import time

from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config, get_plugin_config

from helpers import raise_if_missing_plugin_parameters, addEntraGroupMemberById

from azure.identity import ClientSecretCredential

# Retrieve plugin parameters
plugin_params = get_plugin_config()

# Raise an error if mandatory plugin parameters are missing
raise_if_missing_plugin_parameters(plugin_params)

# Define Microsoft Graph API connection settings
tenant_id = plugin_params['tenant_id']
client_id = plugin_params['client_id']
client_secret = plugin_params['client_secret']
graph_credentials = ClientSecretCredential(tenant_id, client_id, client_secret)
access_token = None # Global variable used as access_token cache for _get_access_token() function

def _get_access_token() -> str:
    """
    Acquire an access token for Microsoft Graph API.
    
    Returns:
        str: Valid access token.
        
    Raises:
        RuntimeError: If token acquisition fails.
    """
    global access_token
    try:
        if (access_token is None) or (access_token and access_token.expires_on < time.time()):
            token = graph_credentials.get_token(
                "https://graph.microsoft.com/.default"
            )
            access_token = token

        return access_token.token
    
    except Exception as e:
        print(f"Failed to acquire access token: {e}")
        raise RuntimeError(f"Token acquisition failed: {e}") from e


# Retrieve input dataset as a Python dataframe
input_A_names = get_input_names_for_role('main_input')
input_A_dataset = dataiku.Dataset(input_A_names[0])
input_A_dataset_df = input_A_dataset.get_dataframe()

# Fill Entra ID group
groupId = get_recipe_config()['group_id']
userIdsColumn = get_recipe_config()['user_id_column']
input_A_dataset_df.apply(lambda x: addEntraGroupMemberById(_get_access_token(), groupId, x[userIdsColumn]), axis=1)

# Write recipe outputs in output dataset
main_output_name = get_output_names_for_role('main_output')
output_A_dataset = [dataiku.Dataset(name) for name in main_output_name][0]

output_dataset_df = input_A_dataset_df
output_A_dataset.write_with_schema(output_dataset_df)