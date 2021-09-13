import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
from datetime import datetime
from datetime import timedelta
import time
from client import TestClient
from base import HubspotBaseTest


class TestHubspotPagination(HubspotBaseTest):

    # TODO | Optimization is possible by checking only the first page in our test client
    #        reads for 'has-more/hasMore'. That way if there are 1000 records and page size is 100,
    #        we only get 100 of those records.
    #        NOTE: This ^ approach makes assumptions about records returned are actually unique. May
    #              not be true of events streams.

    @staticmethod
    def name():
        return "tap_tester_hubspot_pagination_test"

    def get_properties(self):
        return {
            'start_date' : datetime.strftime(datetime.today()-timedelta(days=7), self.START_DATE_FORMAT)  # TODO make this 1 week ago
        }


    @staticmethod
    def expected_page_size():
        # TODO verify which  streams paginate and what are the limits
        # TODO abstract this expectation into base metadata expectations
        return {
            #"subscription_changes": 10 - 1000, # TODO
            "subscription_changes": 1000,
            #  "email_events": 10 - 1000, # TODO
            "email_events": 1000, # TODO
            # "forms": ??, # TODO #infinity
            # "workflows": ??, # TODO
            # "owners": ??, # TODO
            # "campaigns": ??, # TODO
            # "campaigns": 500, # TODO # Can't make test data
            # "deal_pipelines": ?? , # TODO # deprecated
            #"contacts_by_company": 100,
            "contact_lists": 250,
            "contacts": 100,
            "companies": 250,
            "deals": 100,
            # "engagements": 250, # TODO
        }

    def setUp(self):
        self.maxDiff = None  # see all output in failure
        set_up_start = time.perf_counter()

        self.my_timestamp = self.get_properties()['start_date']
        test_client = TestClient(self.my_timestamp)
        existing_records = dict()
        streams = self.expected_streams() - {'email_events'} # we get this for free with subscription_changes
        limits = self.expected_page_limits() # TODO This should be set off of the base expectations
         # 'contacts_by_company' stream needs to get companyIds first so putting the stream last in the list
        stream_to_run_last = 'contacts_by_company'
        if stream_to_run_last in streams:
            streams.remove(stream_to_run_last)
            streams = list(streams)
            streams.append(stream_to_run_last)

        for stream in streams:
            # Get all records
            if stream == 'contacts_by_company':
                company_ids = [company['companyId'] for company in existing_records['companies']]
                existing_records[stream] = test_client.read(stream, parent_ids=company_ids)
            elif stream in {'companies', 'contact_lists', 'subscription_changes'}:
                existing_records[stream] = test_client.read(stream, since=self.my_timestamp)
            else:
                existing_records[stream] = test_client.read(stream)
                # existing_records['subscription_changes'] = test_client.get_subscription_changes()  # see BUG_TDL-14938
            # check if we exceed the limit
            under_target = limits[stream] + 1 - len(existing_records[stream])
            print(f'under_target = {under_target} for {stream}')

            if under_target >= 0 :
                print(f"need to make {under_target} records for {stream} stream")
                #subscription changes will pass in records and under_target to get to the large limit quicker
                if stream == "subscription_changes":
                    test_client.create(stream, subscriptions=existing_records[stream], times=under_target)
                else:
                    for i in range(under_target):
                        # create records to exceed limit
                        if stream == 'contacts_by_company':
                            # company_ids = [record["company-id"] for record in existing_records[stream]]
                            test_client.create(stream, company_ids)
                        else:
                            test_client.create(stream)

        set_up_end = time.perf_counter()
        print(f"Test Client took about {str(set_up_end-set_up_start).split('.')[0]} seconds")

    def expected_streams(self): # TODO this should run off of base expectations
        """
        All streams are under test
        """
        streams_to_test =  set(stream for stream, limit in self.expected_page_limits().items() if limit != set())

        return streams_to_test

    # TODO card out boundary testing for future tap-tester upgrades
    #

    def test_run(self):
        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        #self.select_all_streams_and_fields(conn_id, catalog_entries, select_all_fields=True)
        for catalog_entry in catalog_entries:
            stream_schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog_entry,
                stream_schema
            )

        sync_record_count = self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()


        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                record_count = sync_record_count.get(stream, 0)

                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                primary_keys = self.expected_primary_keys().get(stream)

                # Verify the sync meets or exceeds the default record count
                stream_page_size = self.expected_page_limits()[stream]
                self.assertLess(stream_page_size, record_count)

                # Verify we did not duplicate any records across pages
                records_pks_set = {tuple([message.get('data').get(primary_key)
                                          for primary_key in primary_keys])
                                   for message in sync_messages}
                records_pks_list = [tuple([message.get('data').get(primary_key)
                                           for primary_key in primary_keys])
                                    for message in sync_messages]
                # records_pks_list = [message.get('data').get(primary_key) for message in sync_messages]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg=f"We have duplicate records for {stream}")
