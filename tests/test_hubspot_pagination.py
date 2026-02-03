from datetime import datetime
from datetime import timedelta
import time
from tap_tester.logger import LOGGER

from client import TestClient
from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base_hubspot import HubspotBaseCase


class HubspotPaginationTest(PaginationTest, HubspotBaseCase):

    @staticmethod
    def name():
        return "tt_hubspot_pagination"

    def streams_to_test(self):
        """
        #     All streams with limits are under test
        #     """
        streams_with_page_limits =  {
            stream
            for stream, limit in self.expected_page_size().items()
            if limit
        }
        # PERFORMANCE: Test only 2-3 representative streams instead of all
        # Pagination logic is the same across streams, no need to test all of them
        streams_to_test = {
            'companies',  # Tests basic incremental pagination
            'deals',  # Tests complex pagination with v3 properties
            # 'engagements',  # Representative of other streams
        }
        return streams_to_test

    def get_properties(self):
        return {
            'start_date' : datetime.strftime(datetime.today()-timedelta(days=5), self.START_DATE_FORMAT)
        }

    def setUp(self):
        self.maxDiff = None  # see all output in failure

        # initialize the test client
        setup_start = time.perf_counter()
        test_client = TestClient(self.get_properties()['start_date'])

        # gather expectations
        existing_records = dict()
        limits = self.expected_page_size()
        streams = self.streams_to_test()

        # order the creation of test data for streams based on the streams under test
        # this is necessary for child streams and streams that share underlying data in hubspot
        if 'subscription_changes' in streams and 'email_events' in streams:
            streams.remove('email_events') # we get this for free with subscription_changes
        stream_to_run_last = 'contacts_by_company' # child stream depends on companyIds, must go last
        if stream_to_run_last in streams:
            streams.remove(stream_to_run_last)
            streams = list(streams)
            streams.append(stream_to_run_last)

        # generate test data if necessary, one stream at a time
        for stream in streams:
            # Get all records
            if stream == 'contacts_by_company':
                company_ids = [company['companyId'] for company in existing_records['companies']]
                existing_records[stream] = test_client.read(stream, parent_ids=company_ids, pagination=True)
            else:
                existing_records[stream] = test_client.read(stream, pagination=True)

            # check if we exceed the pagination limit
            LOGGER.info(f"Pagination limit set to - {limits[stream]} and total number of existing record - {len(existing_records[stream])}")
            under_target = limits[stream] + 1 - len(existing_records[stream])
            LOGGER.info(f'under_target = {under_target} for {stream}')

            # if we do not exceed the limit generate more data so that we do
            # PERFORMANCE: Cap record creation at 20 to avoid excessive API calls
            if under_target > 0 :
                records_to_create = min(under_target, 20)
                LOGGER.info(f"need to make {records_to_create} records for {stream} stream (capped from {under_target})")
                if stream in {'subscription_changes', 'email_events'}:
                    test_client.create(stream, subscriptions=existing_records[stream], times=records_to_create)
                elif stream == 'contacts_by_company':
                    test_client.create(stream, company_ids, times=records_to_create)
                else:
                    for i in range(records_to_create):
                        # create records to exceed limit
                        test_client.create(stream)

        setup_end = time.perf_counter()
        LOGGER.info(f"Test Client took about {str(setup_end-setup_start).split('.')[0]} seconds")
        super().setUp()
