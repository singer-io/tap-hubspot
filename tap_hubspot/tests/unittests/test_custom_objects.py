import unittest
from unittest.mock import patch
from tap_hubspot import add_custom_streams, STREAMS, sync_custom_object_records, Context

MOCK_CATALOG = {
    "streams": [
        {
            "stream": "cars",
            "tap_stream_id": "cars",
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": ["null", "string"]},
                    "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                    "property_model": {"type": ["null", "string"]},
                },
            },
            "metadata": [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "table-key-properties": ["id"],
                        "forced-replication-method": "INCREMENTAL",
                        "valid-replication-keys": ["updatedAt"],
                        "selected": True,
                    },
                },
                {
                    "breadcrumb": ["properties", "id"],
                    "metadata": {"inclusion": "automatic"},
                },
                {
                    "breadcrumb": ["properties", "updatedAt"],
                    "metadata": {"inclusion": "automatic"},
                },
                {
                    "breadcrumb": ["properties", "property_model"],
                    "metadata": {"inclusion": "available", "selected": True},
                },
            ],
        }
    ]
}


class TestAddCustomStreams(unittest.TestCase):
    @patch("tap_hubspot.get_url", return_value="fake_custom_objects_schema_url")
    @patch("tap_hubspot.load_shared_schema_refs", return_value="fake_refs")
    @patch("tap_hubspot.gen_request_custom_objects")
    @patch("tap_hubspot.utils.load_json")
    @patch("tap_hubspot.parse_custom_schema")
    @patch("tap_hubspot.singer.resolve_schema_references")
    @patch("builtins.open", create=True)
    @patch("tap_hubspot.LOGGER.warning")
    def test_add_custom_streams(
        self,
        mock_warning,
        mock_open,
        mock_resolve_schema,
        mock_parse_custom_schema,
        mock_load_json,
        mock_gen_request_custom_objects,
        mock_load_shared_schema_refs,
        mock_get_url,
    ):
        """
        test the flow of definition add_custom_streams
        """

        # Set up mocks and fake data
        fake_mode = "DISCOVER"
        fake_custom_object = {
            "name": "fake_object",
            "properties": {"prop1": "type1", "prop2": "type2"},
        }
        fake_custom_objects_schema_url = "fake_custom_objects_schema_url"
        fake_final_schema = {
            "type": "object",
            "properties": {"property_fake_object": "fake_value"},
        }

        # Set up mock return values
        mock_gen_request_custom_objects.return_value = [fake_custom_object]
        mock_load_json.return_value = {
            "type": "object",
            "properties": {"properties": {}},
        }
        mock_parse_custom_schema.return_value = {"prop1": "type1", "prop2": "type2"}
        mock_resolve_schema.return_value = fake_final_schema
        mock_get_url.return_value = fake_custom_objects_schema_url

        initial_streams_len = len(STREAMS)
        # Call the function
        add_custom_streams(fake_mode)

        post_streams_len = len(STREAMS)
        # Verify the expected calls
        mock_gen_request_custom_objects.assert_called_once_with(
            "custom_objects_schema",
            fake_custom_objects_schema_url,
            {},
            "results",
            "paging",
        )
        mock_load_shared_schema_refs.assert_called_once()
        mock_get_url.assert_called_once_with("custom_objects_schema")
        mock_parse_custom_schema.assert_called_once_with(
            "fake_object", {"prop1": "type1", "prop2": "type2"}, isCustomObject=True
        )
        mock_resolve_schema.assert_called_once_with(
            {
                "type": "object",
                "properties": {
                    "properties": {
                        "type": "object",
                        "properties": {"prop1": "type1", "prop2": "type2"},
                    },
                    "property_prop1": "type1",
                    "property_prop2": "type2",
                },
            },
            "fake_refs",
        )
        mock_warning.assert_not_called()  # No warning should be issued in this case
        self.assertGreater(post_streams_len, initial_streams_len)

    @patch("tap_hubspot.gen_request_custom_objects")
    @patch("tap_hubspot.get_start", return_value="2023-07-07T00:00:00Z")
    @patch("tap_hubspot.get_selected_property_fields", return_value="model")
    def test_sync_custom_objects(
        self, mock_property, mock_start_date, mock_custom_objects
    ):
        """
        Test the synchronization of custom objects.
        """

        # Set up mocks and fake data
        STATE = {"currently_syncing": "cars"}
        ctx = Context(MOCK_CATALOG)
        stream_id = "cars"
        mock_custom_objects.return_value = [
            {
                "id": "11111",
                "properties": {"model": "Frontier"},
                "updatedAt": "2023-11-09T13:14:22.956Z",
            }
        ]
        expected_output = {
            "currently_syncing": "cars",
            "bookmarks": {"cars": {"updatedAt": "2023-11-09T13:14:22.956000Z"}},
        }

        # Call the function
        actual_output = sync_custom_object_records(STATE, ctx, stream_id)
        # Verify the expected calls
        self.assertEqual(expected_output, actual_output)
