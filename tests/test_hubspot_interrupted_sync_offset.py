from datetime import datetime, timedelta
from time import sleep
import copy

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest
from client import TestClient


class TestHubspotInterruptedSyncOffsetContactLists(HubspotBaseTest):
    """Testing interrupted syncs for streams that implement unique bookmarking logic."""
    synced_records = None

    @staticmethod
    def name():
        return "tt_hubspot_interrupt_contact_lists"

    def streams_to_test(self):
        """expected streams minus the streams not under test"""
        untested = {
            # Streams tested elsewhere
            'engagements', # covered in TestHubspotInterruptedSync1
            # Feature Request | TDL-16095: [tap-hubspot] All incremental
            #                   streams should implement the interruptible sync feature
            'forms', # TDL-16095
            'owners', # TDL-16095
            'workflows', # TDL-16095
            # Streams that do not apply
            'deal_pipelines', # interruptible does not apply, child of deals
            'campaigns', # unable to manually find a partial state with our test data
            'email_events', # unable to manually find a partial state with our test data
            'subscription_changes', # BUG_TDL-14938
            'tickets' # covered in TestHubspotInterruptedSync1
        }

        return self.expected_streams() - untested

    def stream_to_interrupt(self):
        return 'contact_lists'

    def state_to_inject(self, new_state):
        new_state['bookmarks']['contact_lists'] = {'offset': {'offset': 250}}
        return new_state

    def get_properties(self):
        return {
            'start_date' : datetime.strftime(
                datetime.today()-timedelta(days=3), self.START_DATE_FORMAT
            ),
        }

    def setUp(self):
        self.maxDiff = None  # see all output in failure

    def test_run(self):

        # BUG TDL-16094 [tap-hubspot] `contacts` streams fails to recover from sync interruption
        if self.stream_to_interrupt() == 'contacts':
            self.skipTest("Skipping contacts TEST! See BUG[TDL-16094]")


        expected_streams = self.streams_to_test()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        for catalog_entry in catalog_entries:
            stream_schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog_entry,
                stream_schema
            )

        # Run sync 1
        first_record_count_by_stream = self.run_and_verify_sync(conn_id)
        self.synced_records = runner.get_records_from_target_output()
        state_1 = menagerie.get_state(conn_id)

        # Update state to simulate a bookmark
        stream = self.stream_to_interrupt()
        new_state = copy.deepcopy(state_1)
        new_state = self.state_to_inject(new_state)
        new_state['currently_syncing'] = stream

        menagerie.set_state(conn_id, new_state)

        # run second sync
        second_record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records_2 = runner.get_records_from_target_output()
        state_2 = menagerie.get_state(conn_id)

        # Verify post-iterrupted sync bookmark should be greater than or equal to interrupted sync bookmark
        # since newly created test records may get updated while stream is syncing
        replication_keys = self.expected_replication_keys()
        for stream in state_1.get('bookmarks'):

            if self.stream_to_interrupt() == 'companies' and stream == 'companies':
                    replication_key = list(replication_keys[stream])[0]
                    self.assertLessEqual(new_state.get('bookmarks')[stream].get('current_sync_start'),
                                        state_2["bookmarks"][stream].get(replication_key),
                                        msg="First sync bookmark should not be greater than the second bookmark.")
            elif stream == 'contacts_by_company':
                self.assertEqual(state_1["bookmarks"][stream], {"offset": {}})
                self.assertEqual(state_2["bookmarks"][stream], {"offset": {}})

            else:
                replication_key = list(replication_keys[stream])[0]
                self.assertLessEqual(state_1["bookmarks"][stream].get(replication_key),
                                    state_2["bookmarks"][stream].get(replication_key),
                                    msg="First sync bookmark should not be greater than the second bookmark.")


class TestHubspotInterruptedSyncOffsetDeals(TestHubspotInterruptedSyncOffsetContactLists):
    """Testing interrupted syncs for streams that implement unique bookmarking logic."""
    @staticmethod
    def name():
        return "tt_hubspot_interrupt_deals"

    def get_properties(self):
        return  {
            'start_date' : datetime.strftime(
                datetime.today()-timedelta(days=3), self.START_DATE_FORMAT
            ),
        }

    def stream_to_interrupt(self):
        return 'deals'

    def state_to_inject(self, new_state):
        new_state['bookmarks']['deals'] = {'property_hs_lastmodifieddate': '2021-10-13T08:32:08.383000Z',
                                           'offset': {'offset': 3442973342}}
        return new_state


class TestHubspotInterruptedSyncOffsetCompanies(TestHubspotInterruptedSyncOffsetContactLists):
    """Testing interrupted syncs for streams that implement unique bookmarking logic."""
    @staticmethod
    def name():
        return "tt_hubspot_interrupt_companies"

    def get_properties(self):
        return {
            'start_date' : datetime.strftime(
                datetime.today()-timedelta(days=5), self.START_DATE_FORMAT
            ),
        }

    def stream_to_interrupt(self):
        return 'companies'

    def state_to_inject(self, new_state):
        companies_records = self.synced_records['companies']['messages']
        contacts_by_company_records = self.synced_records['contacts_by_company']['messages']

        company_record_index = int(len(companies_records)/2)
        contact_record_index = int(3*len(contacts_by_company_records)/4)

        last_modified_value = companies_records[-1]['data'][list(self.expected_replication_keys()['companies'])[0]]['value']
        current_sync_start = companies_records[company_record_index]['data'][list(self.expected_replication_keys()['companies'])[0]]['value']
        offset_1 = companies_records[company_record_index]['data']['companyId']
        offset_2 = contacts_by_company_records[contact_record_index]['data']['company-id']

        new_state['bookmarks']['companies'] = {'property_hs_lastmodifieddate': last_modified_value,
                                               'current_sync_start': current_sync_start,
                                               'offset': {'offset': offset_1}}
        new_state['bookmarks']['contacts_by_company'] = {'offset': {'offset': offset_2}}

        return new_state
