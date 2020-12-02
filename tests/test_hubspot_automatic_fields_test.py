import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import re

from base import HubspotBaseTest

class TestHubspotAutomaticFields(HubspotBaseTest):
    def name(self):
        return "tap_tester_hubspot_combined_test"

    def get_properties(self):
        return {
            'start_date' : '2016-06-02T00:00:00Z',
        }

    def expected_streams(self):
        return self.expected_check_streams().difference({
            'subscription_changes',
            'email_events',
            'companies',
            'contacts',
            'deals',
            'engagements',
            'contacts_by_company',
        })


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries, select_all_fields=False)

        # Verify our selection worked as expected
        catalogs_selection = menagerie.get_catalogs(conn_id)
        for cat in catalogs_selection:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

            # Verify the expected stream tables are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            print("Validating selection on {}: {}".format(cat['stream_name'], selected))
            if cat['stream_name'] not in expected_streams:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            # Verify only automatic fields are selected
            expected_automatic_fields = self.expected_automatic_fields().get(cat['tap_stream_id'])
            selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])
            self.assertEqual(expected_automatic_fields, selected_fields, msg='for stream {}, expected: {} actual: {}'.format(cat['stream_name'], expected_automatic_fields, selected_fields))

        # Run a sync job using orchestrator
        sync_record_count = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Assert the records for each stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                data = synced_records.get(stream)

                if not data:
                    print('WARNING: Add data for {}'.format(stream))
                    continue

                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = self.expected_automatic_fields().get(stream)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertEqual(
                        actual_keys.symmetric_difference(expected_keys), set(),
                        msg="Expected automatic fields and nothing else.")

                # Verify the sync meets or exceeds the default record count
                record_count = sync_record_count.get(stream, 0)
                self.assertLessEqual(1, record_count)
