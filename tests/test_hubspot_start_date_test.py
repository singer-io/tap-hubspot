import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest

class TestHubspotStartDate(HubspotBaseTest):
    def name(self):
        return "tap_tester_hubspot_combined_test"

    def expected_streams(self):
        """All streams are under test"""
        return self.expected_check_streams() - {'subscription_changes', 'email_events'}


    def get_properties(self, original=True):
        if original:
            return {
                'start_date' : '2016-06-02T00:00:00Z',
            }
        else:
            return {
                'start_date' : '2020-11-01T00:00:00Z',
            }


    def test_run(self):
        # SYNC 1
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        first_record_count_by_stream = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()

        # SYNC 2
        conn_id = connections.ensure_connection(self, original_properties=False)

        found_catalogs = self.run_and_verify_check_mode(conn_id)
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        second_record_count_by_stream = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()

        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                # record counts
                first_sync_count = first_record_count_by_stream.get(stream, 0)
                second_sync_count = second_record_count_by_stream.get(stream, 0)

                # record messages
                first_sync_messages = first_sync_records.get(stream, {'messages': []}).get('messages')
                second_sync_messages = second_sync_records.get(stream, {'messages': []}).get('messages')

                # start dates
                start_date_1 = self.get_properties()['start_date']
                start_date_2 = self.get_properties(original=False)['start_date']

                if first_sync_messages and second_sync_messages:

                    sorted_pks = sorted(list(self.expected_metadata()[stream][self.PRIMARY_KEYS]))

                    # Get all primary keys for the first sync
                    first_sync_primary_keys = []
                    for message in first_sync_messages:
                        record = message['data']
                        primary_key = tuple([record[k] for k in sorted_pks])
                        first_sync_primary_keys.append(primary_key)

                    # Get all primary keys for the second sync
                    second_sync_primary_keys = []
                    for message in second_sync_messages:
                        record = message['data']
                        primary_key = tuple([record[k] for k in sorted_pks])
                        second_sync_primary_keys.append(primary_key)

                    # Verify everthing in sync 2 is in sync 1
                    for pk in sorted(second_sync_primary_keys):
                        self.assertIn(pk, first_sync_primary_keys)

                # Verify the second sync has less data
                self.assertGreaterEqual(first_sync_count, second_sync_count)
