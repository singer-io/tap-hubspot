import unittest
import requests
from unittest.mock import patch, MagicMock

import tap_hubspot
from tap_hubspot import (
    check_stream_access,
    _get_accessible_streams,
    discover_schemas,
    ENDPOINTS,
    Stream,
    SourceUnavailableException,
    HubspotForbiddenError,
    STREAMS,
    STREAM_ACCESS_ENDPOINTS,
)


class TestCheckStreamAccess(unittest.TestCase):
    """Tests for the check_stream_access function."""

    @patch('tap_hubspot.request')
    def test_returns_true_when_accessible(self, mock_request):
        """Stream endpoint responding normally should return True."""
        mock_request.return_value = MagicMock()
        result = check_stream_access("contacts")
        self.assertTrue(result)

    @patch('tap_hubspot.request')
    def test_returns_false_on_403(self, mock_request):
        """Stream endpoint returning 403 should return False."""
        mock_request.side_effect = SourceUnavailableException("Forbidden")
        result = check_stream_access("contacts")
        self.assertFalse(result)

    @patch('tap_hubspot.request')
    def test_returns_true_for_unknown_stream(self, mock_request):
        """Unknown streams not in STREAM_ACCESS_ENDPOINTS should return True."""
        result = check_stream_access("unknown_stream_xyz")
        self.assertTrue(result)
        mock_request.assert_not_called()

    @patch('tap_hubspot.request')
    def test_returns_true_for_child_stream(self, mock_request):
        """Child streams (form_submissions, list_memberships, contacts_by_company)
        are not in STREAM_ACCESS_ENDPOINTS and should return True."""
        result = check_stream_access("form_submissions")
        self.assertTrue(result)
        mock_request.assert_not_called()

    @patch('tap_hubspot.request')
    def test_reraises_non_403_errors(self, mock_request):
        """Non-403 errors should be re-raised, not suppressed."""
        response = MagicMock()
        response.status_code = 500
        error = requests.exceptions.HTTPError(response=response)
        mock_request.side_effect = error
        with self.assertRaises(requests.exceptions.HTTPError):
            check_stream_access("contacts")

    @patch('tap_hubspot.request')
    def test_returns_false_on_http_error_403(self, mock_request):
        """HTTPError with 403 status code should return False."""
        response = MagicMock()
        response.status_code = 403
        error = requests.exceptions.HTTPError(response=response)
        mock_request.side_effect = error
        result = check_stream_access("contacts")
        self.assertFalse(result)

    @patch('tap_hubspot.post_search_endpoint')
    def test_contact_lists_uses_post(self, mock_post):
        """contact_lists stream should use POST method for access check."""
        mock_post.return_value = MagicMock()
        result = check_stream_access("contact_lists")
        self.assertTrue(result)
        mock_post.assert_called_once()

    @patch('tap_hubspot.post_search_endpoint')
    def test_contact_lists_returns_false_on_403(self, mock_post):
        """contact_lists returning 403 via POST should return False."""
        mock_post.side_effect = SourceUnavailableException("Forbidden")
        result = check_stream_access("contact_lists")
        self.assertFalse(result)

    @patch('tap_hubspot.request')
    def test_all_parent_streams_have_access_config(self, mock_request):
        """All parent streams in STREAMS should have an entry in STREAM_ACCESS_ENDPOINTS."""
        parent_streams = [s for s in STREAMS if not s.parent_tap_stream_id]
        for stream in parent_streams:
            self.assertIn(
                stream.tap_stream_id,
                STREAM_ACCESS_ENDPOINTS,
                f"Parent stream '{stream.tap_stream_id}' missing from STREAM_ACCESS_ENDPOINTS",
            )


class TestGetAccessibleStreams(unittest.TestCase):
    """Tests for the _get_accessible_streams function."""

    def _make_streams(self):
        """Create a sample set of streams for testing."""
        return [
            Stream("contacts", None, ["id"], "updatedAt", "INCREMENTAL"),
            Stream("companies", None, ["companyId"], "property_hs_lastmodifieddate", "INCREMENTAL"),
            Stream("deals", None, ["dealId"], "property_hs_lastmodifieddate", "INCREMENTAL"),
            Stream("contacts_by_company", None, ["company-id", "contact-id"], None, "FULL_TABLE", "companies"),
        ]

    @patch('tap_hubspot.check_stream_access')
    def test_all_accessible(self, mock_check):
        """When all streams are accessible, all should be returned."""
        mock_check.return_value = True
        streams = self._make_streams()
        result = _get_accessible_streams(streams)
        self.assertEqual(len(result), 4)

    @patch('tap_hubspot.check_stream_access')
    def test_partial_access(self, mock_check):
        """Only accessible streams should be returned."""
        def side_effect(name):
            return name != "deals"
        mock_check.side_effect = side_effect
        streams = self._make_streams()
        result = _get_accessible_streams(streams)
        result_ids = [s.tap_stream_id for s in result]
        self.assertIn("contacts", result_ids)
        self.assertIn("companies", result_ids)
        self.assertNotIn("deals", result_ids)
        # contacts_by_company should still be present (parent 'companies' is accessible)
        self.assertIn("contacts_by_company", result_ids)

    @patch('tap_hubspot.check_stream_access')
    def test_child_excluded_when_parent_inaccessible(self, mock_check):
        """Child streams should be excluded when their parent is not accessible."""
        def side_effect(name):
            return name != "companies"
        mock_check.side_effect = side_effect
        streams = self._make_streams()
        result = _get_accessible_streams(streams)
        result_ids = [s.tap_stream_id for s in result]
        self.assertNotIn("companies", result_ids)
        self.assertNotIn("contacts_by_company", result_ids)
        self.assertIn("contacts", result_ids)

    @patch('tap_hubspot.check_stream_access')
    def test_all_inaccessible_raises_exception(self, mock_check):
        """If ALL parent streams are inaccessible, HubspotForbiddenError should be raised."""
        mock_check.return_value = False
        streams = self._make_streams()
        with self.assertRaises(HubspotForbiddenError):
            _get_accessible_streams(streams)

    @patch('tap_hubspot.check_stream_access')
    def test_check_not_called_for_child_streams(self, mock_check):
        """check_stream_access should NOT be called for child streams."""
        mock_check.return_value = True
        streams = self._make_streams()
        _get_accessible_streams(streams)
        # check_stream_access should only be called for parent streams
        called_streams = [call[0][0] for call in mock_check.call_args_list]
        self.assertNotIn("contacts_by_company", called_streams)

    @patch('tap_hubspot.check_stream_access')
    def test_single_accessible_stream_no_error(self, mock_check):
        """Even if only one parent stream is accessible, no error should be raised."""
        def side_effect(name):
            return name == "contacts"
        mock_check.side_effect = side_effect
        streams = self._make_streams()
        result = _get_accessible_streams(streams)
        result_ids = [s.tap_stream_id for s in result]
        self.assertEqual(result_ids, ["contacts"])


class TestDiscoverSchemasWithAccessCheck(unittest.TestCase):
    """Tests for discover_schemas with access checking integration."""

    @patch('tap_hubspot.generate_custom_streams', return_value=[])
    @patch('tap_hubspot.load_discovered_schema')
    @patch('tap_hubspot._get_accessible_streams')
    def test_discover_uses_accessible_streams(self, mock_accessible, mock_load_schema, mock_custom):
        """discover_schemas should only iterate over accessible streams."""
        mock_stream = Stream("contacts", None, ["id"], "updatedAt", "INCREMENTAL")
        mock_accessible.return_value = [mock_stream]
        mock_load_schema.return_value = ({"properties": {"id": {"type": "string"}}}, [])

        result = discover_schemas()
        self.assertEqual(len(result['streams']), 1)
        self.assertEqual(result['streams'][0]['tap_stream_id'], 'contacts')

    @patch('tap_hubspot.generate_custom_streams', return_value=[])
    @patch('tap_hubspot.load_discovered_schema')
    @patch('tap_hubspot._get_accessible_streams')
    def test_discover_excludes_inaccessible_streams(self, mock_accessible, mock_load_schema, mock_custom):
        """Streams not in the accessible list should not appear in catalog."""
        mock_stream_contacts = Stream("contacts", None, ["id"], "updatedAt", "INCREMENTAL")
        # Only contacts is accessible; deals is excluded by _get_accessible_streams
        mock_accessible.return_value = [mock_stream_contacts]
        mock_load_schema.return_value = ({"properties": {"id": {"type": "string"}}}, [])

        result = discover_schemas()
        stream_ids = [s['tap_stream_id'] for s in result['streams']]
        self.assertIn("contacts", stream_ids)
        self.assertNotIn("deals", stream_ids)

    @patch('tap_hubspot.generate_custom_streams', return_value=[])
    @patch('tap_hubspot.load_discovered_schema')
    @patch('tap_hubspot._get_accessible_streams')
    def test_discover_handles_schema_load_failure(self, mock_accessible, mock_load_schema, mock_custom):
        """If a schema load fails with SourceUnavailableException, that stream is skipped."""
        mock_stream = Stream("contacts", None, ["id"], "updatedAt", "INCREMENTAL")
        mock_accessible.return_value = [mock_stream]
        mock_load_schema.side_effect = SourceUnavailableException("403 Forbidden")

        # Patch CONFIG to avoid NoneType error in warning message
        with patch.dict(tap_hubspot.CONFIG, {'access_token': 'test_token', 'api_key': None}):
            result = discover_schemas()
        self.assertEqual(len(result['streams']), 0)

    @patch('tap_hubspot._get_accessible_streams')
    def test_discover_raises_when_no_streams_accessible(self, mock_accessible):
        """discover_schemas should propagate HubspotForbiddenError."""
        mock_accessible.side_effect = HubspotForbiddenError("No access")
        with self.assertRaises(HubspotForbiddenError):
            discover_schemas()


class TestStreamAccessEndpointsMapping(unittest.TestCase):
    """Tests to validate the STREAM_ACCESS_ENDPOINTS configuration."""

    def test_all_endpoints_exist(self):
        """All endpoint keys in STREAM_ACCESS_ENDPOINTS must exist in ENDPOINTS."""
        for stream_name, config in STREAM_ACCESS_ENDPOINTS.items():
            endpoint = config["endpoint"]
            self.assertIn(
                endpoint,
                ENDPOINTS,
                f"Stream '{stream_name}' references endpoint '{endpoint}' not in ENDPOINTS dict",
            )

    def test_no_child_streams_in_access_endpoints(self):
        """Child streams should not appear in STREAM_ACCESS_ENDPOINTS."""
        child_streams = [s.tap_stream_id for s in STREAMS if s.parent_tap_stream_id]
        for child in child_streams:
            self.assertNotIn(
                child,
                STREAM_ACCESS_ENDPOINTS,
                f"Child stream '{child}' should not be in STREAM_ACCESS_ENDPOINTS",
            )

    def test_post_method_streams_have_body(self):
        """Streams using POST method should have a body configured."""
        for stream_name, config in STREAM_ACCESS_ENDPOINTS.items():
            if config.get("method") == "POST":
                self.assertIn(
                    "body",
                    config,
                    f"Stream '{stream_name}' uses POST but has no 'body' configured",
                )


if __name__ == '__main__':
    unittest.main()
