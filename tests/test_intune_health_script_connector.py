"""
Unit tests for the Intune device health script device run states connector.
"""

import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest


# Mock optional DSS, Azure, and Microsoft Graph dependencies before imports.
class DummyConnector:
    def __init__(self, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config


dataiku_module = MagicMock()
dataiku_connector_module = MagicMock()
dataiku_connector_module.Connector = DummyConnector
dataiku_module.connector = dataiku_connector_module
sys.modules["dataiku"] = dataiku_module
sys.modules["dataiku.connector"] = dataiku_connector_module
sys.modules["azure"] = MagicMock()
sys.modules["azure.identity"] = MagicMock()
sys.modules["kiota_authentication_azure"] = MagicMock()
sys.modules["kiota_authentication_azure.azure_identity_authentication_provider"] = MagicMock()
sys.modules["msgraph_beta"] = MagicMock()
sys.modules["msgraph_beta.generated"] = MagicMock()
sys.modules["msgraph_beta.generated.models"] = MagicMock()
sys.modules["msgraph_beta.generated.models.security"] = MagicMock()
sys.modules["msgraph_beta.generated.models.security.audit_log_query"] = MagicMock()
sys.modules["msgraph_beta.generated.models.security.audit_log_query_status"] = MagicMock()
sys.modules["msgraph_beta.graph_request_adapter"] = MagicMock()
sys.modules["msgraph_core"] = MagicMock()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../python-lib"))
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__),
        "../python-connectors/microsoft-graph_intune-deviceHealthScripts-deviceRunStates",
    ),
)

from helpers import listIntuneDeviceHealthScriptDeviceRunStates
from connector import GetIntuneDeviceHealthScriptdeviceRunStates


SCRIPT_ID = "script-123"
ACCESS_TOKEN = "access-token"


@pytest.fixture
def connector():
    with patch("connector.ClientSecretCredential") as credential_class:
        instance = GetIntuneDeviceHealthScriptdeviceRunStates(
            {"deviceHealthScriptId": SCRIPT_ID},
            {"tenant_id": "tenant", "client_id": "client", "client_secret": "secret"},
        )
        instance.credential = credential_class.return_value
        instance.credential.get_token.return_value = SimpleNamespace(
            token=ACCESS_TOKEN,
            expires_on=10**12,
        )
        return instance


def api_response(value, next_link=None):
    response = MagicMock()
    payload = {"value": value}
    if next_link:
        payload["@odata.nextLink"] = next_link
    response.json.return_value = payload
    return response


class TestConnectorSchema:
    def test_schema_contains_expected_columns(self, connector):
        schema = connector.get_read_schema()

        assert schema["columns"] == [
            {"name": "deviceId", "type": "string"},
            {"name": "deviceName", "type": "string"},
            {"name": "managedDevice", "type": "string"},
            {"name": "detectionState", "type": "string"},
            {"name": "remediationState", "type": "string"},
            {"name": "preRemediationDetectionScriptOutput", "type": "string"},
            {"name": "preRemediationDetectionScriptError", "type": "string"},
            {"name": "postRemediationDetectionScriptOutput", "type": "string"},
            {"name": "remediationScriptError", "type": "string"},
            {"name": "lastStateUpdateDateTime", "type": "date"},
        ]


class TestConnectorRows:
    @patch("connector.listIntuneDeviceHealthScriptDeviceRunStates")
    def test_generate_rows_maps_and_cleans_values(self, list_states, connector):
        managed_device = {"id": "device-1", "deviceName": "LAPTOP-01", "osVersion": "10.0"}
        list_states.return_value = [
            {
                "id": "run-state-1",
                "managedDevice": managed_device,
                "detectionState": "success",
                "remediationState": "remediated",
                "preRemediationDetectionScriptOutput": "before\nOK!",
                "preRemediationDetectionScriptError": "none",
                "postRemediationDetectionScriptOutput": "after: OK",
                "remediationScriptError": "error: none",
                "lastStateUpdateDateTime": "2026-08-24T10:00:00Z",
            }
        ]

        rows = list(connector.generate_rows(records_limit=10))

        assert rows == [
            {
                "deviceId": "run-state-1",
                "deviceName": "LAPTOP-01",
                "managedDevice": json.dumps(managed_device),
                "detectionState": "success",
                "remediationState": "remediated",
                "preRemediationDetectionScriptOutput": "beforeOK",
                "preRemediationDetectionScriptError": "none",
                "postRemediationDetectionScriptOutput": "afterOK",
                "remediationScriptError": "errornone",
                "lastStateUpdateDateTime": "2026-08-24T10:00:00Z",
            }
        ]
        list_states.assert_called_once_with(ACCESS_TOKEN, deviceHealthScriptId=SCRIPT_ID, records_limit=10)

    @patch("connector.listIntuneDeviceHealthScriptDeviceRunStates")
    def test_generate_rows_uses_cached_access_token(self, list_states, connector):
        connector._access_token = SimpleNamespace(token=ACCESS_TOKEN, expires_on=10**12)
        list_states.return_value = []

        list(connector.generate_rows())

        list_states.assert_called_once_with(ACCESS_TOKEN, deviceHealthScriptId=SCRIPT_ID, records_limit=-1)
        connector.credential.get_token.assert_not_called()

    @patch("connector.listIntuneDeviceHealthScriptDeviceRunStates")
    def test_get_records_count_returns_number_of_states(self, list_states, connector):
        connector._access_token = SimpleNamespace(token=ACCESS_TOKEN, expires_on=10**12)
        list_states.return_value = [{"id": "one"}, {"id": "two"}]

        assert connector.get_records_count() == 2
        list_states.assert_called_once_with(ACCESS_TOKEN, deviceHealthScriptId=SCRIPT_ID)


class TestHealthScriptHelper:
    @patch("helpers.requests.get")
    def test_helper_follows_pagination_and_expands_managed_device(self, mock_get):
        first_page = api_response([{"id": "one"}], "https://graph.microsoft.com/beta/next-page")
        second_page = api_response([{"id": "two"}])
        mock_get.side_effect = [first_page, second_page]

        result = listIntuneDeviceHealthScriptDeviceRunStates(ACCESS_TOKEN, SCRIPT_ID)

        assert result == [{"id": "one"}, {"id": "two"}]
        assert mock_get.call_args_list == [
            call(
                url=(
                    "https://graph.microsoft.com/beta/deviceManagement/"
                    "deviceHealthScripts/script-123/deviceRunStates?$expand=managedDevice"
                ),
                headers={"Authorization": "Bearer access-token"},
            ),
            call(
                url="https://graph.microsoft.com/beta/next-page",
                headers={"Authorization": "Bearer access-token"},
            ),
        ]

    @patch("helpers.requests.get")
    def test_helper_returns_empty_list_for_empty_response(self, mock_get):
        mock_get.return_value = api_response([])

        assert listIntuneDeviceHealthScriptDeviceRunStates(ACCESS_TOKEN, SCRIPT_ID) == []
        mock_get.assert_called_once()

    @patch("helpers.requests.get")
    def test_helper_wraps_request_errors(self, mock_get):
        mock_get.side_effect = ValueError("invalid response")

        with pytest.raises(RuntimeError, match="Failed to retrieve device health script device run states"):
            listIntuneDeviceHealthScriptDeviceRunStates(ACCESS_TOKEN, SCRIPT_ID)
