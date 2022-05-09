import datetime

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest
from client import TestClient


STATIC_DATA_STREAMS = {'owners', 'campaigns'}

class TestHubspotStartDate(HubspotBaseTest):

    def name(self):
        return "tt_hubspot_start_date"

    def setUp(self):
        """
        Create 1 record for every stream under test, because we must guarantee that
        over time there will always be more records in the sync 1 time bin
        (of start_date_1 -> now) than there are in the sync 2 time bin (of start_date_2 -> now).
        """

        print("running streams with creates")
        streams_under_test = self.expected_streams() - {'email_events'} # we get this for free with subscription_changes
        self.my_start_date = self.get_properties()['start_date']
        self.test_client = TestClient(self.my_start_date)
        for stream in streams_under_test:
            if stream == 'contacts_by_company':
                companies_records = self.test_client.read('companies', since=self.my_start_date)
                company_ids = [company['companyId'] for company in companies_records]
                self.test_client.create(stream, company_ids)
            else:
                self.test_client.create(stream)

    def expected_streams(self):

        # If any streams cannot have data generated programmatically,
        #      hardcode start_dates for these streams and run the test twice.
        # streams tested in TestHubspotStartDateStatic should be removed
        return self.expected_check_streams().difference({
            'deal_pipelines',
            'owners',
            'campaigns',
        })


    def get_properties(self, original=True):
        utc_today = datetime.datetime.strftime(
            datetime.datetime.utcnow(), self.START_DATE_FORMAT
        )

        if original:
            return {
                'start_date' : self.timedelta_formatted(utc_today, days=-7)
            }
        else:
            return {
                'start_date': utc_today
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
                self.assertGreater(first_sync_count, 0)
                if first_sync_messages and second_sync_messages:

                    primary_keys = self.expected_primary_keys()[stream]

                    # Get all primary keys for the first sync
                    first_sync_primary_keys = []
                    for message in first_sync_messages:
                        record = message['data']
                        primary_key = tuple([record[pk] for pk in primary_keys])
                        first_sync_primary_keys.append(primary_key)

                    # Get all primary keys for the second sync
                    second_sync_primary_keys = []
                    for message in second_sync_messages:
                        record = message['data']
                        primary_key = tuple([record[pk] for pk in primary_keys])
                        second_sync_primary_keys.append(primary_key)

                    # Verify everthing in sync 2 is in sync 1
                    for tupled_primary_key_value in (second_sync_primary_keys):
                        self.assertIn(tupled_primary_key_value, first_sync_primary_keys)
                if self.expected_metadata()[stream][self.OBEYS_START_DATE]:
                    # Log out WARNING if there is no data in sync 2 for a stream
                    if second_sync_count == 0:
                        print(f"WARNING | Sync 2 did not catch any data for {stream}-- check if the POSTs are working. "
                              "This test is likely to fail if sync 2 continues to have 0 records.")
                    # Verify the second sync has less data
                    self.assertGreater(first_sync_count, second_sync_count)
                else:
                    # If Start date is not obeyed then verify the syncs are equal
                    self.assertEqual(first_sync_count, second_sync_count)
                    self.assertEqual(first_sync_primary_keys, second_sync_primary_keys)
class TestHubspotStartDateStatic(TestHubspotStartDate):
    def name(self):
        return "tt_hubspot_start_date_static"

    def expected_streams(self):
        """expected streams minus the streams not under test"""
        return {
            'owners',
            'campaigns',
        }

    def get_properties(self, original=True):
        utc_today = datetime.datetime.strftime(
            datetime.datetime.utcnow(), self.START_DATE_FORMAT
        )

        if original:
            return {'start_date' : '2017-11-22T00:00:00Z'}

        else:
            return {
                'start_date' : '2022-02-25T00:00:00Z'
            }

    def setUp(self):
        print("running streams with no creates")
