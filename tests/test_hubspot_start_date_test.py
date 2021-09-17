import datetime

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest
from client import TestClient


class TestHubspotStartDate(HubspotBaseTest):

    def name(self):
        return "tap_tester_hubspot_start_date_test"
    # TODOs
    # make setUp that generates 1 record for each stream under test, to ensure 1 record exists for today
    # set the original (first sync) start date to today minus 5 days
    # set the second sync start date to today

    # although...

    # maybe we should base expected records off of the replication-key values in the test_client records
    # then we will always know excatly which records should be synced, and we will avoid stability issue
    # when devs/qa run tests locally....

    def generate_test_data(self):
        """
        Create 1 record for every stream under test, because we must guarantee that
        over time there will always be more records in the sync 1 time bin
        (of start_date_1 -> now) than there are in the sync 2 time bin (of start_date_2 -> now).
        """
        streams_under_test = self.expected_streams()
        self.test_client = TestClient()

        if 'campaigns' in streams_under_test:
            _ = self.test_client.create_campaigns()

        if 'forms' in streams_under_test:
            _ = self.test_client.create_forms()

        if 'owners' in streams_under_test:
            _ = self.test_client.create_owners()

        if 'engagements' in streams_under_test:
            _ = self.test_client.create_engagements()

        if 'workflows' in streams_under_test:
            _ = self.test_client.create_workflows()

        if 'contact_lists' in streams_under_test:
            _ = self.test_client.create_contact_lists()

        if 'contacts' in streams_under_test:
            _ = self.test_client.create_contacts()

        if 'companies' in streams_under_test:
            _ = self.test_client.create_companies()
        existing_companies = self.test_client.get_companies(self.get_properties()['start_date'])
        company_ids = [company['companyId'] for company in existing_companies]
        if 'contacts_by_company' in streams_under_test:
            _ = self.test_client.create_contacts_by_company(company_ids)

        if 'deal_pipelines' in streams_under_test:
            _ = self.test_client.create_deal_pipelines()

        if 'deals' in streams_under_test:
            _ = self.test_client.create_deals()

        if any([stream in streams_under_test
                for stream in {'email_events', 'subscription_changes'}]):
            _ = self.test_client.create_subscription_changes()

    def expected_streams(self):
        """returns the streams that are under test"""
        temporarliy_skipping_for_one_day = {'deal_pipelines','contacts_by_company'}

        # TODO If any streams cannot have data generated programmatically,
        #      hardcode start_dates for these streams and run the test twice.

        return self.expected_check_streams().difference({
            'deal_pipelines',
            'campaigns',  # Cannot create data dynamically
            'owners',  # Cannot create data dynamically
        })#.difference(temporarliy_skipping_for_one_day)


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
        # TEST SETUP
        self.generate_test_data()


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

                # Verify the second sync has less data
                self.assertGreater(first_sync_count, second_sync_count)

                # Log out WARNING if there is no data in sync 2 for a stream
                if second_sync_count == 0:
                    print("WARNING | Sync 2 did not catch any data -- check if the POSTs are working. "
                      "This test is likely to fail if sync 2 continues to have 0 records.")
