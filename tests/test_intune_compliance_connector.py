"""
Unit tests for Intune Device Compliance Setting States Connector

Tests cover:
- Parameter parsing and validation
- API query construction
- Aggregation of setting states across multiple summaries
- Pagination handling
- Record limit enforcement
- Summary ID filtering
- Field selection with $select
- Error handling
- Output format validation
"""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch
import sys
import os

# Mock Microsoft Graph dependencies
from unittest.mock import MagicMock
sys.modules['kiota_authentication_azure'] = MagicMock()
sys.modules['kiota_authentication_azure.azure_identity_authentication_provider'] = MagicMock()
sys.modules['msgraph_beta'] = MagicMock()
sys.modules['msgraph_beta.generated'] = MagicMock()
sys.modules['msgraph_beta.generated.models'] = MagicMock()
sys.modules['msgraph_beta.generated.models.security'] = MagicMock()
sys.modules['msgraph_beta.generated.models.security.audit_log_query'] = MagicMock()
sys.modules['msgraph_beta.generated.models.security.audit_log_query_status'] = MagicMock()
sys.modules['msgraph_beta.graph_request_adapter'] = MagicMock()
sys.modules['msgraph_core'] = MagicMock()
sys.modules['azure'] = MagicMock()
sys.modules['azure.identity'] = MagicMock()

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../python-lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../python-connectors/microsoft-graph_intune-devicecompliance-settingstates'))

from helpers import listIntuneDeviceComplianceSettingStates, countIntuneDeviceComplianceSettingStates


# ==================== Sample Data ====================

SAMPLE_DEVICE_COMPLIANCE_SETTING_STATE = {
    "id": "9905f955-f955-9905-55f9-059955f90599",
    "setting": "deviceComplianceSettingState",
    "settingName": "Device Encryption Required",
    "deviceId": "device-001-uuid",
    "deviceName": "LAPTOP-ABC123",
    "userId": "user-001-uuid",
    "userEmail": "john.doe@contoso.com",
    "userName": "John Doe",
    "userPrincipalName": "john.doe@contoso.com",
    "deviceModel": "Lenovo ThinkPad X1 Carbon",
    "state": "compliant",
    "complianceGracePeriodExpirationDateTime": "2026-12-31T23:56:44.951111-08:00"
}

SAMPLE_SETTING_STATES = [
    {
        "id": "state-001",
        "setting": "encryption",
        "settingName": "Device Encryption",
        "deviceId": "device-001",
        "deviceName": "LAPTOP-ABC",
        "userId": "user-001",
        "userEmail": "user1@contoso.com",
        "userName": "User One",
        "userPrincipalName": "user1@contoso.com",
        "deviceModel": "Lenovo ThinkPad",
        "state": "compliant",
        "complianceGracePeriodExpirationDateTime": "2026-12-31T23:56:44.951111-08:00"
    },
    {
        "id": "state-002",
        "setting": "firewall",
        "settingName": "Windows Firewall",
        "deviceId": "device-002",
        "deviceName": "LAPTOP-XYZ",
        "userId": "user-002",
        "userEmail": "user2@contoso.com",
        "userName": "User Two",
        "userPrincipalName": "user2@contoso.com",
        "deviceModel": "Dell Latitude",
        "state": "noncompliant",
        "complianceGracePeriodExpirationDateTime": "2026-06-30T12:00:00.000000-08:00"
    },
    {
        "id": "state-003",
        "setting": "antivirus",
        "settingName": "Antivirus Required",
        "deviceId": "device-003",
        "deviceName": "DESKTOP-123",
        "userId": "user-003",
        "userEmail": "user3@contoso.com",
        "userName": "User Three",
        "userPrincipalName": "user3@contoso.com",
        "deviceModel": "HP ProDesk",
        "state": "notApplicable",
        "complianceGracePeriodExpirationDateTime": None
    }
]


# ==================== Fixtures ====================

@pytest.fixture
def valid_access_token():
    """Return a valid mock access token"""
    return 'mock_access_token_12345'


# ==================== Test: Output Format and Fields ====================

class TestOutputFormat:
    """Test that output contains correct deviceComplianceSettingState fields"""

    @patch('helpers.requests.get')
    def test_output_contains_all_required_fields(self, mock_get, valid_access_token):
        """Test output contains all deviceComplianceSettingState fields"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [{'id': 'summary-1'}]
        }
        
        states_response = MagicMock()
        states_response.json.return_value = {
            'value': [SAMPLE_DEVICE_COMPLIANCE_SETTING_STATE.copy()]
        }
        
        mock_get.side_effect = [summaries_response, states_response]
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        assert len(result) == 1
        state = result[0]
        
        # Verify all required fields are present
        required_fields = [
            'id', 'setting', 'settingName', 'deviceId', 'deviceName',
            'userId', 'userEmail', 'userName', 'userPrincipalName',
            'deviceModel', 'state', 'complianceGracePeriodExpirationDateTime',
            'summaryId'
        ]
        
        for field in required_fields:
            assert field in state, f"Missing required field: {field}"

    @patch('helpers.requests.get')
    def test_sample_data_format(self, mock_get, valid_access_token):
        """Test with provided sample data format"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [{'id': 'summary-encryption-policy'}]
        }
        
        sample_state = SAMPLE_DEVICE_COMPLIANCE_SETTING_STATE.copy()
        states_response = MagicMock()
        states_response.json.return_value = {'value': [sample_state]}
        
        mock_get.side_effect = [summaries_response, states_response]
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        assert len(result) == 1
        state = result[0]
        
        # Verify sample data fields
        assert state['id'] == "9905f955-f955-9905-55f9-059955f90599"
        assert state['setting'] == "deviceComplianceSettingState"
        assert state['settingName'] == "Device Encryption Required"
        assert state['deviceId'] == "device-001-uuid"
        assert state['state'] == "compliant"
        assert state['summaryId'] == "summary-encryption-policy"


# ==================== Test: Aggregation Across Summaries ====================

class TestAggregationAcrossSummaries:
    """Test aggregation of setting states across multiple summaries"""

    @patch('helpers.requests.get')
    def test_aggregates_states_from_all_summaries(self, mock_get, valid_access_token):
        """Test aggregates setting states from multiple summaries into single dataset"""
        
        # Two summaries
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-encryption'},
                {'id': 'summary-firewall'}
            ]
        }
        
        # States for first summary (encryption)
        states_response_1 = MagicMock()
        states_response_1.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[0].copy()]
        }
        
        # States for second summary (firewall)
        states_response_2 = MagicMock()
        states_response_2.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[1].copy()]
        }
        
        mock_get.side_effect = [summaries_response, states_response_1, states_response_2]
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        # Should have states from both summaries
        assert len(result) == 2
        
        # Verify summaryId is added to each state
        assert result[0]['summaryId'] == 'summary-encryption'
        assert result[0]['settingName'] == 'Device Encryption'
        
        assert result[1]['summaryId'] == 'summary-firewall'
        assert result[1]['settingName'] == 'Windows Firewall'

    @patch('helpers.requests.get')
    def test_aggregation_with_multiple_states_per_summary(self, mock_get, valid_access_token):
        """Test aggregates multiple states from multiple summaries"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-1'},
                {'id': 'summary-2'}
            ]
        }
        
        # Summary 1 has 2 states
        states_response_1 = MagicMock()
        states_response_1.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[0].copy(), SAMPLE_SETTING_STATES[1].copy()]
        }
        
        # Summary 2 has 1 state
        states_response_2 = MagicMock()
        states_response_2.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[2].copy()]
        }
        
        mock_get.side_effect = [summaries_response, states_response_1, states_response_2]
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        # Should have 3 total states
        assert len(result) == 3
        
        # First two from summary-1
        assert result[0]['summaryId'] == 'summary-1'
        assert result[1]['summaryId'] == 'summary-1'
        
        # One from summary-2
        assert result[2]['summaryId'] == 'summary-2'

    @patch('helpers.requests.get')
    def test_dataset_covers_all_summaries_by_default(self, mock_get, valid_access_token):
        """Test that dataset includes data for ALL summaries when no filter provided"""
        
        # Without filter, should retrieve all summaries
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-1'},
                {'id': 'summary-2'},
                {'id': 'summary-3'}
            ]
        }
        
        states_response = MagicMock()
        states_response.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[0].copy()]
        }
        
        mock_get.side_effect = [
            summaries_response,
            states_response, states_response, states_response
        ]
        
        # Call without summary_ids (should get all)
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        # Should retrieve states from all 3 summaries
        assert len(result) == 3


# ==================== Test: Summary ID Filtering ====================

class TestSummaryIDFiltering:
    """Test filtering to specific summary IDs"""

    @patch('helpers.requests.get')
    def test_filters_to_specific_summary_ids(self, mock_get, valid_access_token):
        """Test filtering to specific summary IDs"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-1'},
                {'id': 'summary-2'},
                {'id': 'summary-3'}
            ]
        }
        
        # Only query states for summary-2
        states_response = MagicMock()
        states_response.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[1].copy()]
        }
        
        mock_get.side_effect = [summaries_response, states_response]
        
        result = listIntuneDeviceComplianceSettingStates(
            access_token=valid_access_token,
            summary_ids=['summary-2']
        )
        
        # Should only get states from summary-2
        assert len(result) == 1
        assert result[0]['summaryId'] == 'summary-2'

    @patch('helpers.requests.get')
    def test_filters_to_multiple_summary_ids(self, mock_get, valid_access_token):
        """Test filtering to multiple specific summary IDs"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-1'},
                {'id': 'summary-2'},
                {'id': 'summary-3'}
            ]
        }
        
        # Query states for summary-1 and summary-3
        states_response_1 = MagicMock()
        states_response_1.json.return_value = {'value': [SAMPLE_SETTING_STATES[0].copy()]}
        
        states_response_3 = MagicMock()
        states_response_3.json.return_value = {'value': [SAMPLE_SETTING_STATES[2].copy()]}
        
        mock_get.side_effect = [summaries_response, states_response_1, states_response_3]
        
        result = listIntuneDeviceComplianceSettingStates(
            access_token=valid_access_token,
            summary_ids=['summary-1', 'summary-3']
        )
        
        # Should get states from summary-1 and summary-3
        assert len(result) == 2
        assert result[0]['summaryId'] == 'summary-1'
        assert result[1]['summaryId'] == 'summary-3'


# ==================== Test: Pagination ====================

class TestPagination:
    """Test pagination handling"""

    @patch('helpers.requests.get')
    def test_pagination_across_summaries(self, mock_get, valid_access_token):
        """Test pagination through multiple pages of summaries"""
        
        # Multiple pages of summaries
        summaries_page1 = MagicMock()
        summaries_page1.json.return_value = {
            'value': [{'id': 'summary-1'}],
            '@odata.nextLink': 'https://example.com/summaries/page2'
        }
        
        summaries_page2 = MagicMock()
        summaries_page2.json.return_value = {
            'value': [{'id': 'summary-2'}]
        }
        
        states_response = MagicMock()
        states_response.json.return_value = {'value': [SAMPLE_SETTING_STATES[0].copy()]}
        
        mock_get.side_effect = [
            summaries_page1, summaries_page2,  # Two pages of summaries
            states_response, states_response   # States for each summary
        ]
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        # Should get states from both pages of summaries
        assert len(result) == 2

    @patch('helpers.requests.get')
    def test_pagination_across_states(self, mock_get, valid_access_token):
        """Test pagination through multiple pages of setting states"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [{'id': 'summary-1'}]
        }
        
        # Multiple pages of states
        states_page1 = MagicMock()
        states_page1.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[0].copy(), SAMPLE_SETTING_STATES[1].copy()],
            '@odata.nextLink': 'https://example.com/states/page2'
        }
        
        states_page2 = MagicMock()
        states_page2.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[2].copy()]
        }
        
        mock_get.side_effect = [summaries_response, states_page1, states_page2]
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        # Should get all states from both pages
        assert len(result) == 3


# ==================== Test: Record Limit ====================

class TestRecordLimit:
    """Test record_limit enforcement"""

    @patch('helpers.requests.get')
    def test_record_limit_stops_retrieval(self, mock_get, valid_access_token):
        """Test record_limit stops retrieval at specified count"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-1'},
                {'id': 'summary-2'},
                {'id': 'summary-3'}
            ]
        }
        
        # Summary 1 has 2 states
        states_response_1 = MagicMock()
        states_response_1.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[0].copy(), SAMPLE_SETTING_STATES[1].copy()]
        }
        
        # Summary 2 has 1 state
        states_response_2 = MagicMock()
        states_response_2.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[2].copy()]
        }
        
        mock_get.side_effect = [summaries_response, states_response_1, states_response_2]
        
        result = listIntuneDeviceComplianceSettingStates(
            access_token=valid_access_token,
            records_limit=3
        )
        
        # Should only get 3 states
        assert len(result) == 3


# ==================== Test: Query Parameters ====================

class TestQueryParameters:
    """Test query parameter support"""

    @patch('helpers.requests.get')
    def test_select_parameter_passed(self, mock_get, valid_access_token):
        """Test $select parameter is passed to API"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [{'id': 'summary-1'}]
        }
        
        states_response = MagicMock()
        states_response.json.return_value = {
            'value': [SAMPLE_SETTING_STATES[0].copy()]
        }
        
        mock_get.side_effect = [summaries_response, states_response]
        
        listIntuneDeviceComplianceSettingStates(
            access_token=valid_access_token,
            query_select='id,setting,settingName,deviceName,state'
        )
        
        # Verify $select was passed in states query
        states_call = mock_get.call_args_list[1]
        assert states_call[1]['params']['$select'] == 'id,setting,settingName,deviceName,state'


# ==================== Test: Edge Cases ====================

class TestEdgeCases:
    """Test edge cases"""

    @patch('helpers.requests.get')
    def test_empty_summaries_returns_empty_dataset(self, mock_get, valid_access_token):
        """Test empty summaries returns empty dataset"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {'value': []}
        
        mock_get.return_value = summaries_response
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        assert len(result) == 0

    @patch('helpers.requests.get')
    def test_no_matching_summary_ids(self, mock_get, valid_access_token):
        """Test filtering to non-existent summary IDs returns empty"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-1'},
                {'id': 'summary-2'}
            ]
        }
        
        mock_get.return_value = summaries_response
        
        result = listIntuneDeviceComplianceSettingStates(
            access_token=valid_access_token,
            summary_ids=['non-existent-id']
        )
        
        assert len(result) == 0


# ==================== Integration Tests ====================

class TestIntegration:
    """Integration tests with real sample data"""

    @patch('helpers.requests.get')
    def test_full_workflow_with_sample_data(self, mock_get, valid_access_token):
        """Test complete workflow with provided sample data"""
        
        # Three summaries with different compliance policies
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'encryption-policy-summary'},
                {'id': 'firewall-policy-summary'},
                {'id': 'antivirus-policy-summary'}
            ]
        }
        
        # Each summary has setting states
        states_encryption = [SAMPLE_SETTING_STATES[0].copy()]
        states_firewall = [SAMPLE_SETTING_STATES[1].copy()]
        states_antivirus = [SAMPLE_SETTING_STATES[2].copy()]
        
        response_encryption = MagicMock()
        response_encryption.json.return_value = {'value': states_encryption}
        
        response_firewall = MagicMock()
        response_firewall.json.return_value = {'value': states_firewall}
        
        response_antivirus = MagicMock()
        response_antivirus.json.return_value = {'value': states_antivirus}
        
        mock_get.side_effect = [
            summaries_response,
            response_encryption, response_firewall, response_antivirus
        ]
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        # Should get all 3 states
        assert len(result) == 3
        
        # Verify complete sample format
        assert result[0] == {
            **SAMPLE_SETTING_STATES[0],
            'summaryId': 'encryption-policy-summary'
        }
        assert result[1] == {
            **SAMPLE_SETTING_STATES[1],
            'summaryId': 'firewall-policy-summary'
        }
        assert result[2] == {
            **SAMPLE_SETTING_STATES[2],
            'summaryId': 'antivirus-policy-summary'
        }

    @patch('helpers.requests.get')
    def test_dataset_contains_all_summaries_data(self, mock_get, valid_access_token):
        """Test dataset contains data for all summaries across organization"""
        
        # Simulate organization with 5 compliance policy summaries
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': f'policy-summary-{i}'} for i in range(1, 6)
            ]
        }
        
        # Each summary gets its own states response to ensure different summary IDs
        states_responses = []
        for i in range(1, 6):
            response = MagicMock()
            state = SAMPLE_SETTING_STATES[0].copy()
            state['id'] = f'state-from-summary-{i}'
            response.json.return_value = {'value': [state]}
            states_responses.append(response)
        
        mock_get.side_effect = [summaries_response] + states_responses
        
        result = listIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        # Should have states from all 5 summaries
        assert len(result) == 5
        
        # Verify each result has different summary ID
        summary_ids = [state['summaryId'] for state in result]
        assert summary_ids == [f'policy-summary-{i}' for i in range(1, 6)]


# ==================== Test: Count Optimization with $count Parameter ====================

class TestCountOptimization:
    """Test efficient counting using $count=true parameter"""

    @patch('helpers.requests.get')
    def test_count_returns_integer(self, mock_get, valid_access_token):
        """Test $count=true returns integer count"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [{'id': 'summary-1'}]
        }
        
        count_response = MagicMock()
        count_response.text = '42'
        
        mock_get.side_effect = [summaries_response, count_response]
        
        count = countIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        assert count == 42
        assert isinstance(count, int)

    @patch('helpers.requests.get')
    def test_count_multiple_summaries(self, mock_get, valid_access_token):
        """Test $count aggregates counts from multiple summaries"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-1'},
                {'id': 'summary-2'},
                {'id': 'summary-3'}
            ]
        }
        
        # Different counts for each summary
        count_responses = []
        for count_value in [10, 25, 15]:
            response = MagicMock()
            response.text = str(count_value)
            count_responses.append(response)
        
        mock_get.side_effect = [summaries_response] + count_responses
        
        total_count = countIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        assert total_count == 50  # 10 + 25 + 15

    @patch('helpers.requests.get')
    def test_count_with_summary_id_filter(self, mock_get, valid_access_token):
        """Test $count respects summary ID filtering"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [
                {'id': 'summary-1'},
                {'id': 'summary-2'},
                {'id': 'summary-3'}
            ]
        }
        
        # Only count for summary-1 and summary-3
        count_responses = []
        for count_value in [20, 30]:  # Only 2 responses for filtered summaries
            response = MagicMock()
            response.text = str(count_value)
            count_responses.append(response)
        
        mock_get.side_effect = [summaries_response] + count_responses
        
        total_count = countIntuneDeviceComplianceSettingStates(
            access_token=valid_access_token,
            summary_ids=['summary-1', 'summary-3']
        )
        
        assert total_count == 50  # 20 + 30

    @patch('helpers.requests.get')
    def test_count_empty_summaries(self, mock_get, valid_access_token):
        """Test $count returns 0 for empty summaries"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {'value': []}
        
        mock_get.return_value = summaries_response
        
        total_count = countIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        assert total_count == 0

    @patch('helpers.requests.get')
    def test_count_uses_count_parameter(self, mock_get, valid_access_token):
        """Test that $count=true parameter is actually used"""
        
        summaries_response = MagicMock()
        summaries_response.json.return_value = {
            'value': [{'id': 'summary-1'}]
        }
        
        count_response = MagicMock()
        count_response.text = '100'
        
        mock_get.side_effect = [summaries_response, count_response]
        
        countIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        # Verify the $count parameter was passed in the states query
        states_call = mock_get.call_args_list[1]
        assert states_call[1]['params']['$count'] == 'true'

    @patch('helpers.requests.get')
    def test_count_pagination_summaries(self, mock_get, valid_access_token):
        """Test $count works with paginated summaries"""
        
        # Multiple pages of summaries
        summaries_page1 = MagicMock()
        summaries_page1.json.return_value = {
            'value': [{'id': 'summary-1'}],
            '@odata.nextLink': 'https://example.com/page2'
        }
        
        summaries_page2 = MagicMock()
        summaries_page2.json.return_value = {
            'value': [{'id': 'summary-2'}]
        }
        
        count_response1 = MagicMock()
        count_response1.text = '15'
        
        count_response2 = MagicMock()
        count_response2.text = '25'
        
        mock_get.side_effect = [
            summaries_page1, summaries_page2,
            count_response1, count_response2
        ]
        
        total_count = countIntuneDeviceComplianceSettingStates(access_token=valid_access_token)
        
        assert total_count == 40  # 15 + 25


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
