import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest

class TestHubspotStartDate(HubspotBaseTest):
    def name(self):
        return "tap_tester_hubspot_combined_test"

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

    def expected_streams(self):
        """All streams are under test"""
        return self.expected_check_streams()

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
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # SYNC 2
        conn_id = self.ensure_connection(original=False)

        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        found_catalogs = menagerie.get_catalogs(conn_id)
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        # Run in Sync mode
        sync_job_name = runner.run_sync_mode(self, conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys())

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):
                # record counts
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # record messages
                first_sync_messages = first_sync_records.get(stream, {'messages': []}).get('messages')
                second_sync_messages = second_sync_records.get(stream, {'messages': []}).get('messages')

                # start dates
                start_date_1 = self.get_properties()['start_date']
                start_date_2 = self.get_properties(original=False)['start_date']

                # Verify everthing in sync 2 is in sync 1
                if first_sync_messages and second_sync_messages:
                    first_sync_primary_keys = []
                    sorted_pks = sorted(list(self.expected_metadata()[stream][self.PRIMARY_KEYS]))
                    for message in first_sync_messages:
                        record = message['data']
                        primary_key = tuple([record[k] for k in sorted_pks])
                        first_sync_primary_keys.append(primary_key)


                    second_sync_primary_keys = []
                    for message in second_sync_messages:
                        record = message['data']
                        primary_key = tuple([record[k] for k in sorted_pks])
                        second_sync_primary_keys.append(primary_key)

                    for pk in sorted(second_sync_primary_keys):
                        self.assertIn(pk, first_sync_primary_keys)

                # Verify the second sync has less data
                self.assertGreaterEqual(first_sync_count, second_sync_count)
