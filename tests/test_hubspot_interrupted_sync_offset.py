from datetime import datetime, timedelta
from time import sleep
import copy

import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest
from client import TestClient


class TestHubspotInterruptedSyncOffset1(HubspotBaseTest):
    """Testing interrupted syncs for streams that implement unique bookmarking logic."""

    def name(self):
        return "tt_hubspot_sync_interrupt_offset_1"

    def streams_to_test(self):
        """expected streams minus the streams not under test"""
        covered_elsewhere = {  # covered in TestHubspotInterruptedSync1
            'companies',
            'engagements'
        }
        streams_with_bugs = {
            'subscription_changes'
        }
        todo_streams = {'contacts_by_company'}

        return self.expected_streams() - covered_elsewhere - streams_with_bugs - todo_streams

    """
    'contacts': {'offset': {'vidOffset': 3502}
    'deals': {'hs_lastmodifieddate': '2021-10-13T08:32:08.383000Z',
    'offset': {'offset': 3442973342}}
    """
    def stream_to_interrupt(self):
        #return 'contact_lists'
        return 'contacts'

    def state_to_inject(self):
        # return {'offset': {'offset': 250}}
        return

    def get_properties(self):
        return {
            'start_date' : datetime.strftime(
                datetime.today()-timedelta(days=3), self.START_DATE_FORMAT
            ),
        }

    def setUp(self):
        self.maxDiff = None  # see all output in failure

    def test_run(self):

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
        synced_records = runner.get_records_from_target_output()
        state_1 = menagerie.get_state(conn_id)

        # Update state to simulate a bookmark
        stream = self.stream_to_interrupt()
        new_state = copy.deepcopy(state_1)
        new_state['bookmarks'][stream] = self.state_to_inject()
        new_state['currently_syncing'] = stream
        menagerie.set_state(conn_id, new_state)

        # run second sync
        second_record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records_2 = runner.get_records_from_target_output()
        state_2 = menagerie.get_state(conn_id)

        # # Test the interrupted stream
        # stream = self.stream_to_interrupt()
        # primary_keys = self.expected_primary_keys()[stream]

        # actual_record_count_2 = second_record_count_by_stream[stream]
        # actual_records_2 = [message['data']
        #                     for message in synced_records_2[stream]['messages']
        #                     if message['action'] == 'upsert']

        # actual_record_count_1 = first_record_count_by_stream[stream]
        # actual_records_1 = [message['data']
        #                     for message in synced_records[stream]['messages']
        #                     if message['action'] == 'upsert']

        # # verify the record count 
        # self.assertEqual(actual_record_count_1, actual_record_count_2)


        # verify the uninterrupted sync and the simulated resuming sync end with the same bookmark values
        self.assertEqual(state_1, state_2)
        """
        {'bookmarks': {'campaigns': {'offset': {}},
                       'contact_lists': {'offset': {},
                                         'updatedAt': '2021-10-12T12:30:37.352000Z'},
                       'contacts': {'offset': {},
                                    'versionTimestamp': '2021-10-12T12:30:54.819000Z'},
                       'deals': {'hs_lastmodifieddate': '2021-10-12T12:30:37.916000Z',
                                 'offset': {}},
                       'email_events': {'offset': {},
                                        'startTimestamp': '2021-10-12T00:00:00.000000Z'},
                       'forms': {'updatedAt': '2021-10-12T12:30:38.501000Z'},
                       'owners': {'updatedAt': '2021-10-09T00:00:00Z'},
                       'subscription_changes': {'offset': {},
                                                'startTimestamp': '2021-10-12T00:00:00.000000Z'},
                       'workflows': {'updatedAt': '2021-10-12T12:29:26.537000Z'}},
         'currently_syncing': None}
        """
# class TestHubspotInterruptedSyncOffset2(TestHubspotInterruptedSyncOffset1):
#     """Testing interrupted syncs for streams that implement unique bookmarking logic."""

#     def name(self):
#         return "tt_hubspot_sync_interrupt_offset_2"

#     def stream_to_interrupt(self):
#         return 'contacts'

#     def state_to_inject(self):
#         return {'offset': {'offset': 250}}

