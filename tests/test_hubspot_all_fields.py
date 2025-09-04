import datetime

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from tap_tester import LOGGER

from base import HubspotBaseTest

KNOWN_MISSING_FIELDS = {
    'owners': {'teams'}
}
class TestHubspotAllFields(HubspotBaseTest):
    """Test that with all fields selected for a stream we replicate data as expected"""
    @staticmethod
    def name():
        return "tt_hubspot_all_fields_dynamic"

    def get_properties(self):
        return {'start_date' : '2021-05-02T00:00:00Z'}

    def streams_under_test(self):
        """expected streams minus the streams not under test"""
        return self.expected_streams().difference({
            'owners',
            'subscription_changes', # BUG_TDL-14938 https://jira.talendforge.org/browse/TDL-14938
        })

    def test_run(self):
        streams_to_test = self.streams_under_test()

        conn_id = connections.ensure_connection(self)

        expected_automatic_fields = self.expected_automatic_fields()

        # Verify that there are catalogs found
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        
        # Table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # Grab metadata after performing table-and-field selection to set expectations
        # Used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(
                fields_from_field_level_md)

        self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)
    
        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields.get(
                    stream, set())

                # Verify that more than just the automatic fields are replicated for each stream.
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))

                expected_all_keys = expected_all_keys - KNOWN_MISSING_FIELDS.get(stream, set())
                messages = synced_records.get(stream)
                # Collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                # Verify all fields for each stream are replicated
                self.assertSetEqual(expected_all_keys, actual_all_keys)


class TestHubspotAllFieldsStatic(TestHubspotAllFields):
    @staticmethod
    def name():
        return "tt_hubspot_all_fields_static"

    def streams_under_test(self):
        """expected streams minus the streams not under test"""
        return {
            'owners',
            # 'subscription_changes', # BUG_TDL-14938 https://jira.talendforge.org/browse/TDL-14938
        }

    def get_properties(self):
        return {'start_date' : '2021-05-02T00:00:00Z'}
