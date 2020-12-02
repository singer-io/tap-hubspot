import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest


class TestHubspotPagination(HubspotBaseTest):
    def name(self):
        return "tap_tester_hubspot_pagination_test"

    def expected_page_size(self):
        return {
            # "subscription_changes": 10 - 1000,
            "subscription_changes": 1000,
            # "email_events": 10 - 1000,
            "email_events": 1000,
            # "forms": ??, #infinity
            # "workflows": ??,
            # "owners": ??,
            # "campaigns": ??,
            # "campaigns": 500, # Can't make test data
            # "contact_lists": 20 - 250,  #count,
            # "deal_pipelines": ?? , # deprecated
            # "contacts_by_company": 100,  #count # deprecated
            "contact_lists": 250,
            "contacts": 100, #count
            "companies": 250,
            "deals": 100,
            "engagements": 250,
        }

    def expected_streams(self):
        """
        All streams are under test
        """
        return set(self.expected_page_size().keys()).difference({
            'subscription_changes',
            'email_events',
        })

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z',
        }


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries, select_all_fields=True)

        sync_record_count = self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()


        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                record_count = sync_record_count.get(stream, 0)

                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                primary_key = self.expected_primary_keys().get(stream).pop()

                # Verify the sync meets or exceeds the default record count
                stream_page_size = self.expected_page_size()[stream]
                self.assertLess(stream_page_size, record_count)

                # Verify we did not duplicate any records across pages
                records_pks_set = {message.get('data').get(primary_key) for message in sync_messages}
                records_pks_list = [message.get('data').get(primary_key) for message in sync_messages]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg="We have duplicate records for {}".format(stream))
