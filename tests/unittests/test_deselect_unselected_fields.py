import unittest
from unittest.mock import patch, MagicMock
from tap_hubspot import deselect_unselected_fields, do_sync, CONFIG


class TestDoSync(unittest.TestCase):

    @patch('tap_hubspot.deselect_unselected_fields')
    @patch('tap_hubspot.generate_custom_streams')
    @patch('tap_hubspot.clean_state')
    @patch('tap_hubspot.Context')
    @patch('tap_hubspot.validate_dependencies')
    @patch('tap_hubspot.get_streams_to_sync')
    @patch('tap_hubspot.get_selected_streams')
    @patch('tap_hubspot.singer')
    def test_do_sync_select_fields_by_default_none(self, mock_singer, mock_get_selected_streams, mock_get_streams_to_sync, mock_validate_dependencies, mock_Context, mock_clean_state, mock_generate_custom_streams, mock_deselect_unselected_fields):
        """Test the default behavior of the do_sync function. When select_fields_by_default is not specified, it should not call deselect_unselected_fields."""
        # Mocking the necessary functions and objects
        mock_singer.get_currently_syncing.return_value = None
        mock_get_streams_to_sync.return_value = []
        mock_get_selected_streams.return_value = []
        mock_generate_custom_streams.return_value = []

        # Mocking the catalog and state
        CONFIG.update({'select_fields_by_default': None})
        catalog = {'streams': []}
        state = {}

        # Call the function
        do_sync(state, catalog)

        # Assertions
        mock_deselect_unselected_fields.assert_not_called()

    # @patch('tap_hubspot.CONFIG', {'select_fields_by_default': True})
    @patch('tap_hubspot.deselect_unselected_fields')
    @patch('tap_hubspot.generate_custom_streams')
    @patch('tap_hubspot.clean_state')
    @patch('tap_hubspot.Context')
    @patch('tap_hubspot.validate_dependencies')
    @patch('tap_hubspot.get_streams_to_sync')
    @patch('tap_hubspot.get_selected_streams')
    @patch('tap_hubspot.singer')
    def test_do_sync_select_fields_by_default_true(self, mock_singer, mock_get_selected_streams, mock_get_streams_to_sync, mock_validate_dependencies, mock_Context, mock_clean_state, mock_generate_custom_streams, mock_deselect_unselected_fields):
        """Test the default behavior of the do_sync function. When select_fields_by_default is True, it should not call deselect_unselected_fields."""
        # Mocking the necessary functions and objects
        mock_singer.get_currently_syncing.return_value = None
        mock_get_streams_to_sync.return_value = []
        mock_get_selected_streams.return_value = []
        mock_generate_custom_streams.return_value = []

        # Mocking the catalog and state
        CONFIG.update({'select_fields_by_default': True})
        catalog = {'streams': []}
        state = {}

        # Call the function
        do_sync(state, catalog)

        # Assertions
        mock_deselect_unselected_fields.assert_not_called()

    # @patch('tap_hubspot.CONFIG', {'select_fields_by_default': False})
    @patch('tap_hubspot.deselect_unselected_fields')
    @patch('tap_hubspot.generate_custom_streams')
    @patch('tap_hubspot.clean_state')
    @patch('tap_hubspot.Context')
    @patch('tap_hubspot.validate_dependencies')
    @patch('tap_hubspot.get_streams_to_sync')
    @patch('tap_hubspot.get_selected_streams')
    @patch('tap_hubspot.singer')
    def test_do_sync_select_fields_by_default_false(self, mock_singer, mock_get_selected_streams, mock_get_streams_to_sync, mock_validate_dependencies, mock_Context, mock_clean_state, mock_generate_custom_streams, mock_deselect_unselected_fields):
        """Test the default behavior of the do_sync function. When select_fields_by_default is False, it should call deselect_unselected_fields."""
        # Mocking the necessary functions and objects
        mock_singer.get_currently_syncing.return_value = None
        mock_get_streams_to_sync.return_value = []
        mock_get_selected_streams.return_value = []
        mock_generate_custom_streams.return_value = []

        # Mocking the catalog and state
        CONFIG.update({'select_fields_by_default': False})
        catalog = {'streams': []}
        state = {}

        # Call the function
        do_sync(state, catalog)

        # Assertions
        mock_deselect_unselected_fields.assert_called_once_with(catalog)


class TestDeselectUnselectedFields(unittest.TestCase):

    def test_deselect_unselected_fields(self):
        catalog = {
            'streams': [
                {
                    "stream_id": "test_stream_1",
                    'metadata': [
                        {'breadcrumb': [], 'metadata': {'selected': True}},
                        {'breadcrumb': ['properties', 'field1'], 'metadata': {}},
                        {'breadcrumb': ['properties', 'field2'], 'metadata': {'selected': True}},
                        {'breadcrumb': ['properties', 'field3'], 'metadata': {'selected': False}}
                    ]
                },
                {
                    "stream_id": "test_stream_2",
                    'metadata': [
                        {'breadcrumb': [], 'metadata': {'selected': False}},
                        {'breadcrumb': ['properties', 'field1'], 'metadata': {}},
                        {'breadcrumb': ['properties', 'field2'], 'metadata': {}}
                    ]
                }
            ]
        }

        expected_catalog = {
            'streams': [
                {
                    "stream_id": "test_stream_1",
                    'metadata': [
                        {'breadcrumb': [], 'metadata': {'selected': True}},
                        {'breadcrumb': ['properties', 'field1'], 'metadata': {'selected': False}},
                        {'breadcrumb': ['properties', 'field2'], 'metadata': {'selected': True}},
                        {'breadcrumb': ['properties', 'field3'], 'metadata': {'selected': False}}
                    ]
                },
                {
                    "stream_id": "test_stream_2",
                    'metadata': [
                        {'breadcrumb': [], 'metadata': {'selected': False}},
                        {'breadcrumb': ['properties', 'field1'], 'metadata': {}},
                        {'breadcrumb': ['properties', 'field2'], 'metadata': {}}
                    ]
                }
            ]
        }

        deselect_unselected_fields(catalog)
        self.assertEqual(catalog, expected_catalog)
