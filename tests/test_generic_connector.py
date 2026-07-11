"""
Unit tests for Generic Microsoft Graph API Connector

Tests cover:
- Parameter validation and error handling
- Pagination logic with @odata.nextLink
- Record limit enforcement
- HTTP method routing (GET, POST, PATCH, DELETE)
- Custom headers and request body parsing
- Edge cases (empty results, malformed responses)
- Token management
- Query parameter building
"""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch, call
import sys
import os

# Mock all Microsoft Graph SDK dependencies before importing helpers
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

# Add python-connectors and python-lib to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../python-lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../python-connectors/microsoft-graph-generic-api-query'))

from helpers import queryGenericGraphAPI


# ==================== Fixtures ====================

@pytest.fixture
def mock_session():
    """Create a mock requests.Session object"""
    return MagicMock()


@pytest.fixture
def mock_response():
    """Create a mock API response"""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        'value': [
            {'id': '1', 'displayName': 'User 1'},
            {'id': '2', 'displayName': 'User 2'}
        ]
    }
    return response


@pytest.fixture
def valid_access_token():
    """Return a valid mock access token"""
    return 'mock_access_token_12345'


# ==================== Test: Parameter Validation ====================

class TestParameterValidation:
    """Test validation of input parameters"""

    def test_empty_api_endpoint_raises_error(self, valid_access_token, mock_session):
        """Test that empty api_endpoint raises ValueError"""
        with pytest.raises(ValueError, match="api_endpoint must be a non-empty string"):
            queryGenericGraphAPI(
                access_token=valid_access_token,
                api_endpoint="",
                session=mock_session
            )

    def test_none_api_endpoint_raises_error(self, valid_access_token, mock_session):
        """Test that None api_endpoint raises ValueError"""
        with pytest.raises(ValueError, match="api_endpoint must be a non-empty string"):
            queryGenericGraphAPI(
                access_token=valid_access_token,
                api_endpoint=None,
                session=mock_session
            )

    def test_invalid_api_version_raises_error(self, valid_access_token, mock_session):
        """Test that invalid api_version raises ValueError"""
        with pytest.raises(ValueError, match="api_version must be either 'v1.0' or 'beta'"):
            queryGenericGraphAPI(
                access_token=valid_access_token,
                api_endpoint="/users",
                api_version="v2.0",
                session=mock_session
            )

    def test_invalid_http_method_raises_error(self, valid_access_token, mock_session):
        """Test that invalid http_method raises ValueError"""
        with pytest.raises(ValueError, match="http_method must be one of"):
            queryGenericGraphAPI(
                access_token=valid_access_token,
                api_endpoint="/users",
                http_method="PUT",
                session=mock_session
            )

    def test_valid_api_versions(self, valid_access_token, mock_session, mock_response):
        """Test that both valid API versions are accepted"""
        mock_session.get.return_value = mock_response

        # Test v1.0
        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            api_version="v1.0",
            session=mock_session
        )
        assert len(result) == 2

        # Test beta
        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            api_version="beta",
            session=mock_session
        )
        assert len(result) == 2

    def test_valid_http_methods(self, valid_access_token, mock_session, mock_response):
        """Test that all valid HTTP methods are accepted"""
        mock_session.get.return_value = mock_response
        mock_session.post.return_value = mock_response
        mock_session.patch.return_value = mock_response
        mock_session.delete.return_value = mock_response

        for method in ["GET", "POST", "PATCH", "DELETE"]:
            result = queryGenericGraphAPI(
                access_token=valid_access_token,
                api_endpoint="/users",
                http_method=method,
                session=mock_session
            )
            assert len(result) == 2

    def test_http_method_case_insensitive(self, valid_access_token, mock_session, mock_response):
        """Test that HTTP method is case-insensitive"""
        mock_session.get.return_value = mock_response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            http_method="get",
            session=mock_session
        )
        assert len(result) == 2


# ==================== Test: URL Construction ====================

class TestURLConstruction:
    """Test correct URL building with API endpoint and version"""

    @patch('helpers.perform_request_with_retry')
    def test_url_construction_v1_0(self, mock_perform_request, valid_access_token, mock_response):
        """Test URL construction for v1.0 API"""
        mock_perform_request.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            api_version="v1.0"
        )

        mock_perform_request.assert_called_once()
        call_args = mock_perform_request.call_args
        assert "https://graph.microsoft.com/v1.0/users" == call_args[1]['url']

    @patch('helpers.perform_request_with_retry')
    def test_url_construction_beta(self, mock_perform_request, valid_access_token, mock_response):
        """Test URL construction for beta API"""
        mock_perform_request.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/devices",
            api_version="beta"
        )

        mock_perform_request.assert_called_once()
        call_args = mock_perform_request.call_args
        assert "https://graph.microsoft.com/beta/devices" == call_args[1]['url']

    @patch('helpers.perform_request_with_retry')
    def test_endpoint_slash_normalization(self, mock_perform_request, valid_access_token, mock_response):
        """Test that endpoint slash is handled correctly"""
        mock_perform_request.return_value = mock_response

        # Without leading slash
        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="users"
        )

        mock_perform_request.assert_called_once()
        call_args = mock_perform_request.call_args
        assert "https://graph.microsoft.com/v1.0/users" == call_args[1]['url']

    @patch('helpers.perform_request_with_retry')
    def test_complex_endpoint_path(self, mock_perform_request, valid_access_token, mock_response):
        """Test complex endpoint paths with multiple segments"""
        mock_perform_request.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/groups/abc-123/members"
        )

        mock_perform_request.assert_called_once()
        call_args = mock_perform_request.call_args
        assert "https://graph.microsoft.com/v1.0/groups/abc-123/members" == call_args[1]['url']


# ==================== Test: Query Parameters ====================

class TestQueryParameters:
    """Test building and passing OData query parameters"""

    def test_query_select_parameter(self, valid_access_token, mock_session, mock_response):
        """Test $select parameter is passed correctly"""
        mock_session.get.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            query_params={"$select": "id,displayName,mail"},
            session=mock_session
        )

        call_args = mock_session.get.call_args
        assert call_args[1]['params']['$select'] == "id,displayName,mail"

    def test_query_filter_parameter(self, valid_access_token, mock_session, mock_response):
        """Test $filter parameter is passed correctly"""
        mock_session.get.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            query_params={"$filter": "accountEnabled eq true"},
            session=mock_session
        )

        call_args = mock_session.get.call_args
        assert call_args[1]['params']['$filter'] == "accountEnabled eq true"

    def test_multiple_query_parameters(self, valid_access_token, mock_session, mock_response):
        """Test multiple query parameters together"""
        mock_session.get.return_value = mock_response

        query_params = {
            "$select": "id,displayName",
            "$filter": "accountEnabled eq true",
            "$top": 50
        }

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            query_params=query_params,
            session=mock_session
        )

        call_args = mock_session.get.call_args
        assert call_args[1]['params'] == query_params

    def test_empty_query_params(self, valid_access_token, mock_session, mock_response):
        """Test with empty query parameters"""
        mock_session.get.return_value = mock_response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            query_params={},
            session=mock_session
        )

        assert len(result) == 2


# ==================== Test: Pagination ====================

class TestPagination:
    """Test pagination with @odata.nextLink"""

    def test_pagination_with_next_link(self, valid_access_token, mock_session):
        """Test pagination follows @odata.nextLink"""
        # First response with nextLink
        response1 = MagicMock()
        response1.status_code = 200
        response1.json.return_value = {
            'value': [
                {'id': '1', 'displayName': 'User 1'},
                {'id': '2', 'displayName': 'User 2'}
            ],
            '@odata.nextLink': 'https://graph.microsoft.com/v1.0/users?$skip=2'
        }

        # Second response without nextLink
        response2 = MagicMock()
        response2.status_code = 200
        response2.json.return_value = {
            'value': [
                {'id': '3', 'displayName': 'User 3'}
            ]
        }

        mock_session.get.side_effect = [response1, response2]

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            pagination_enabled=True,
            session=mock_session
        )

        # Should have 3 records from both pages
        assert len(result) == 3
        assert result[0]['id'] == '1'
        assert result[2]['id'] == '3'

    def test_pagination_disabled(self, valid_access_token, mock_session):
        """Test that pagination can be disabled"""
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {
            'value': [
                {'id': '1', 'displayName': 'User 1'},
                {'id': '2', 'displayName': 'User 2'}
            ],
            '@odata.nextLink': 'https://graph.microsoft.com/v1.0/users?$skip=2'
        }

        mock_session.get.return_value = response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            pagination_enabled=False,
            session=mock_session
        )

        # Should only have first page's 2 records
        assert len(result) == 2
        # get should be called only once
        assert mock_session.get.call_count == 1

    def test_pagination_multiple_pages(self, valid_access_token, mock_session):
        """Test pagination with more than 2 pages"""
        response1 = MagicMock()
        response1.json.return_value = {
            'value': [{'id': '1'}, {'id': '2'}],
            '@odata.nextLink': 'https://example.com/page2'
        }

        response2 = MagicMock()
        response2.json.return_value = {
            'value': [{'id': '3'}, {'id': '4'}],
            '@odata.nextLink': 'https://example.com/page3'
        }

        response3 = MagicMock()
        response3.json.return_value = {
            'value': [{'id': '5'}]
        }

        mock_session.get.side_effect = [response1, response2, response3]

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            pagination_enabled=True,
            session=mock_session
        )

        assert len(result) == 5
        assert mock_session.get.call_count == 3


# ==================== Test: Record Limit ====================

class TestRecordLimit:
    """Test records_limit parameter enforcement"""

    def test_record_limit_stops_pagination(self, valid_access_token, mock_session):
        """Test that records_limit stops pagination early"""
        response1 = MagicMock()
        response1.json.return_value = {
            'value': [{'id': '1'}, {'id': '2'}, {'id': '3'}],
            '@odata.nextLink': 'https://example.com/page2'
        }

        response2 = MagicMock()
        response2.json.return_value = {
            'value': [{'id': '4'}, {'id': '5'}]
        }

        mock_session.get.side_effect = [response1, response2]

        # Request only 3 records
        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            pagination_enabled=True,
            records_limit=3,
            session=mock_session
        )

        # Should get exactly 3 records from first page
        assert len(result) == 3
        # Should only make 1 API call (not follow nextLink)
        assert mock_session.get.call_count == 1

    def test_record_limit_slices_batch(self, valid_access_token, mock_session):
        """Test that partial pages are sliced correctly"""
        response = MagicMock()
        response.json.return_value = {
            'value': [{'id': '1'}, {'id': '2'}, {'id': '3'}, {'id': '4'}, {'id': '5'}]
        }

        mock_session.get.return_value = response

        # Request only 3 records from 5-record response
        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            records_limit=3,
            session=mock_session
        )

        assert len(result) == 3
        assert result[0]['id'] == '1'
        assert result[2]['id'] == '3'

    def test_unlimited_records(self, valid_access_token, mock_session):
        """Test that -1 means unlimited records"""
        response = MagicMock()
        response.json.return_value = {
            'value': [{'id': '1'}, {'id': '2'}, {'id': '3'}]
        }

        mock_session.get.return_value = response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            records_limit=-1,
            session=mock_session
        )

        # Should get all records
        assert len(result) == 3

    def test_record_limit_zero(self, valid_access_token, mock_session):
        """Test that records_limit=0 retrieves 0 records"""
        response = MagicMock()
        response.json.return_value = {
            'value': [{'id': '1'}, {'id': '2'}]
        }

        mock_session.get.return_value = response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            records_limit=0,
            session=mock_session
        )

        # With limit=0, should not retrieve any records
        assert len(result) == 0


# ==================== Test: Custom Headers ====================

class TestCustomHeaders:
    """Test custom HTTP headers"""

    @patch('helpers.perform_request_with_retry')
    def test_custom_headers_passed(self, mock_perform_request, valid_access_token, mock_response):
        """Test custom headers are passed in request"""
        mock_perform_request.return_value = mock_response

        custom_headers = {
            "Prefer": "outlook.timezone=UTC",
            "ConsistencyLevel": "eventual"
        }

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            custom_headers=custom_headers
        )

        # Check that perform_request_with_retry was called with custom headers
        call_args = mock_perform_request.call_args
        # Custom headers should be passed as custom_headers parameter
        assert call_args[1]['custom_headers'] == custom_headers

    @patch('helpers.perform_request_with_retry')
    def test_empty_custom_headers(self, mock_perform_request, valid_access_token, mock_response):
        """Test empty custom headers dict"""
        mock_perform_request.return_value = mock_response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            custom_headers={},
            session=None
        )

        assert len(result) == 2


# ==================== Test: Request Body ====================

class TestRequestBody:
    """Test request body for POST/PATCH operations"""

    def test_post_with_request_body(self, valid_access_token, mock_session, mock_response):
        """Test POST request with JSON body"""
        mock_session.post.return_value = mock_response

        request_body = {
            "displayName": "New Group",
            "mailEnabled": False
        }

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/groups",
            http_method="POST",
            request_body=request_body,
            session=mock_session
        )

        call_args = mock_session.post.call_args
        assert call_args[1]['json'] == request_body
        assert len(result) == 2

    def test_patch_with_request_body(self, valid_access_token, mock_session, mock_response):
        """Test PATCH request with JSON body"""
        mock_session.patch.return_value = mock_response

        request_body = {
            "jobTitle": "Senior Engineer"
        }

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users/user-id",
            http_method="PATCH",
            request_body=request_body,
            session=mock_session
        )

        call_args = mock_session.patch.call_args
        assert call_args[1]['json'] == request_body


# ==================== Test: HTTP Methods ====================

class TestHTTPMethods:
    """Test different HTTP methods are called correctly"""

    def test_get_method(self, valid_access_token, mock_session, mock_response):
        """Test GET method"""
        mock_session.get.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            http_method="GET",
            session=mock_session
        )

        mock_session.get.assert_called_once()

    def test_post_method(self, valid_access_token, mock_session, mock_response):
        """Test POST method"""
        mock_session.post.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/groups",
            http_method="POST",
            request_body={"displayName": "Test"},
            session=mock_session
        )

        mock_session.post.assert_called_once()

    def test_patch_method(self, valid_access_token, mock_session, mock_response):
        """Test PATCH method"""
        mock_session.patch.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users/id",
            http_method="PATCH",
            request_body={"jobTitle": "Engineer"},
            session=mock_session
        )

        mock_session.patch.assert_called_once()

    def test_delete_method(self, valid_access_token, mock_session):
        """Test DELETE method"""
        response = MagicMock()
        response.status_code = 204
        response.json.return_value = {}

        mock_session.delete.return_value = response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users/id",
            http_method="DELETE",
            session=mock_session
        )

        mock_session.delete.assert_called_once()


# ==================== Test: Edge Cases ====================

class TestEdgeCases:
    """Test edge cases and special scenarios"""

    def test_empty_value_array(self, valid_access_token, mock_session):
        """Test response with empty value array"""
        response = MagicMock()
        response.json.return_value = {'value': []}

        mock_session.get.return_value = response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            session=mock_session
        )

        assert len(result) == 0

    def test_response_without_value_field(self, valid_access_token, mock_session):
        """Test response without value field (single object response)"""
        response = MagicMock()
        response.json.return_value = {
            'id': 'user-123',
            'displayName': 'John Doe'
        }

        mock_session.get.return_value = response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/me",
            session=mock_session
        )

        # Should return the entire response as single item
        assert len(result) == 1
        assert result[0]['id'] == 'user-123'

    def test_malformed_json_raises_error(self, valid_access_token, mock_session):
        """Test handling of malformed JSON response"""
        response = MagicMock()
        response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)

        mock_session.get.return_value = response

        with pytest.raises(RuntimeError, match="Failed to parse API response"):
            queryGenericGraphAPI(
                access_token=valid_access_token,
                api_endpoint="/users",
                session=mock_session
            )

    def test_very_large_records_limit(self, valid_access_token, mock_session, mock_response):
        """Test with very large records_limit"""
        mock_session.get.return_value = mock_response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            records_limit=999999,
            session=mock_session
        )

        # Should still work
        assert len(result) == 2

    def test_none_custom_headers(self, valid_access_token, mock_session, mock_response):
        """Test with None custom_headers"""
        mock_session.get.return_value = mock_response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            custom_headers=None,
            session=mock_session
        )

        assert len(result) == 2

    def test_none_request_body(self, valid_access_token, mock_session, mock_response):
        """Test with None request_body"""
        mock_session.get.return_value = mock_response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            request_body=None,
            session=mock_session
        )

        assert len(result) == 2


# ==================== Test: Authorization Header ====================

class TestAuthorizationHeader:
    """Test authorization header handling"""

    def test_authorization_header_present(self, valid_access_token, mock_session, mock_response):
        """Test that Authorization header is always present"""
        mock_session.get.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            session=mock_session
        )

        call_args = mock_session.get.call_args
        headers = call_args[1]['headers']
        assert 'Authorization' in headers
        assert headers['Authorization'] == f'Bearer {valid_access_token}'

    def test_authorization_header_format(self, valid_access_token, mock_session, mock_response):
        """Test Authorization header format"""
        mock_session.get.return_value = mock_response

        queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            session=mock_session
        )

        call_args = mock_session.get.call_args
        auth_header = call_args[1]['headers']['Authorization']
        assert auth_header.startswith('Bearer ')


# ==================== Integration Tests ====================

class TestIntegration:
    """Integration tests combining multiple features"""

    def test_full_workflow_with_all_parameters(self, valid_access_token, mock_session):
        """Test complete workflow with all parameters"""
        response = MagicMock()
        response.json.return_value = {
            'value': [
                {'id': '1', 'displayName': 'User 1', 'mail': 'user1@contoso.com'},
                {'id': '2', 'displayName': 'User 2', 'mail': 'user2@contoso.com'}
            ]
        }

        mock_session.get.return_value = response

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            api_version="v1.0",
            http_method="GET",
            query_params={
                "$select": "id,displayName,mail",
                "$filter": "accountEnabled eq true"
            },
            custom_headers={"Prefer": "outlook.timezone=UTC"},
            pagination_enabled=True,
            records_limit=100,
            session=mock_session
        )

        assert len(result) == 2
        assert result[0]['id'] == '1'
        mock_session.get.assert_called_once()

    def test_pagination_with_record_limit_and_query_params(self, valid_access_token, mock_session):
        """Test pagination combined with record limit and query parameters"""
        response1 = MagicMock()
        response1.json.return_value = {
            'value': [{'id': '1'}, {'id': '2'}],
            '@odata.nextLink': 'https://example.com/page2'
        }

        response2 = MagicMock()
        response2.json.return_value = {
            'value': [{'id': '3'}, {'id': '4'}]
        }

        mock_session.get.side_effect = [response1, response2]

        result = queryGenericGraphAPI(
            access_token=valid_access_token,
            api_endpoint="/users",
            query_params={"$select": "id"},
            pagination_enabled=True,
            records_limit=3,
            session=mock_session
        )

        # Should get 3 records: 2 from first page, 1 from second
        assert len(result) == 3
        # Should make 2 API calls
        assert mock_session.get.call_count == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
