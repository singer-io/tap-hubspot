import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest


class TestHubspotPagination(HubspotBaseTest):
    def name(self):
        return "tap_tester_hubspot_pagination_test"

    def expected_check_streams(self):
        return {
            "subscription_changes",
            "email_events",
            "forms",
            "workflows",
            "owners",
            "campaigns",
            "contact_lists",
            "contacts",
            "companies",
            "deals",
            "engagements",
            "deal_pipelines",
            "contacts_by_company"
        }

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
        conn_id = self.ensure_connection()

        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries, select_all_fields=True)

        for catalog_entry in catalog_entries:
            stream_schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog_entry,
                stream_schema
            )

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Examine target file
        sync_records = runner.get_records_from_target_output()
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())

        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                record_count = sync_record_count.get(stream, 0)

                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                primary_key = self.expected_primary_keys().get(stream).pop()

                # Verify the sync meets or exceeds the default record count
                stream_page_size = self.expected_page_size()[stream]
                self.assertLessEqual(stream_page_size, record_count)

                # Verify we did not duplicate any records across pages
                records_pks_set = {message.get('data').get(primary_key) for message in sync_messages}
                records_pks_list = [message.get('data').get(primary_key) for message in sync_messages]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg="We have duplicate records for {}".format(stream))
