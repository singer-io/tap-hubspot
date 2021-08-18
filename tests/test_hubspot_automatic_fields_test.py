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
            'start_date' : '2021-05-02T00:00:00Z',
        }

    def expected_streams(self):
        """streams to test, excluding the list below"""
        return self.expected_check_streams().difference({
            'subscription_changes',  # TODO no data
            'email_events',  # TODO no data
        })


    def test_run(self):
        """
        Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        Verify that only the automatic fields are sent to the target.
        """
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries, select_all_fields=False)


        # TODO | Include the following step in this test if/when hubspot conforms to the standards of metadata
        # # Verify our selection worked as expected
        # catalogs_selection = menagerie.get_catalogs(conn_id)
        # for cat in catalogs_selection:
        #     with self.subTest(cat=cat):
        #         catalog_entry = menagerie.get_annotated_schema(conn_id, cat['stream_id'])

        #         # Verify the expected stream tables are selected
        #         selected = catalog_entry.get('annotated-schema').get('selected')
        #         print("Validating selection on {}: {}".format(cat['stream_name'], selected))
        #         if cat['stream_name'] not in expected_streams:
        #             self.assertFalse(selected, msg="Stream selected, but not testable.")
        #             continue # Skip remaining assertions if we aren't selecting this stream
        #         self.assertTrue(selected, msg="Stream not selected.")

        #         # Verify only automatic fields are selected
        #         expected_automatic_fields = self.expected_automatic_fields().get(cat['tap_stream_id'])
        #         selected_fields = self.get_selected_fields_from_metadata(catalog_entry['metadata'])

        #         # remove replication keys
        #         self.assertEqual(expected_automatic_fields, selected_fields)


        # Run a sync job using orchestrator
        sync_record_count = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Assert the records for each stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                # Verify that data is present
                record_count = sync_record_count.get(stream, 0)
                self.assertLessEqual(1, record_count, msg=f"record count: {record_count}")

                data = synced_records.get(stream)
                record_messages_keys = [set(row['data'].keys()) for row in data['messages']]
                expected_keys = self.expected_automatic_fields().get(stream)

                # BUG_TDL-9939 https://jira.talendforge.org/browse/TDL-9939
                if stream in {'companies', 'deals', 'contacts'}:
                    # replication keys not in the expected_keys
                    remove_keys = self.expected_metadata()[stream].get(self.REPLICATION_KEYS)
                    expected_keys = expected_keys.difference(remove_keys)
                elif stream in {'engagements'}:
                    # engagements has a nested object 'engagement' with the automatic fields
                    expected_keys = expected_keys.union({'engagement'})
                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(actual_keys, expected_keys,
                                        msg=f"Expected automatic fields: {expected_keys} and nothing else."
                    )

                # make sure there are no duplicate records by using the pks
                pk = self.expected_primary_keys()[stream]
                pks_values = [(message['data'][p] for p in pk) for message in data['messages']]
                self.assertEqual(len(pks_values), len(set(pks_values)))
