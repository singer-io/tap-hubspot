from datetime import datetime
from time import sleep


import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest
from client import TestClient


class TestHubspotBookmarks(HubspotBaseTest):
    """Ensure tap replicates new and upated records based on the replication method of a given stream.


    Create records for each stream. Run check mode, perform table and field selection, and run a sync. Create 1 record for each stream and update 1 record for each stream prior to running a  2nd sync.
    Also delete records between syncs if possible.
    ### ASSERTIONS - will vary based on bookmarks implementation
    #### Bookmarking on field   (updatedAt, dateLastEdited etc.)
     - Verify for each incremental stream you can do a sync which records bookmarks, and that the format matches expectations.
     - Verify that a bookmark doesn't exist for full table streams.
     - Verify the bookmark is the max value sent to the target for the a given replication key.
     - Verify 2nd sync respects the bookmark.
    All data of the 2nd sync is >= the bookmark from the first sync
    The number of records in the 2nd sync is less then the first
    #### Date windowing
    Same as 'Bookmarking on field' BUT must consider the following and add/adjust assertions accordingly:
     - Lookback Windows (How does it affect already replicated data in future syncs?)
     - Inclusive/Exclusive bookmarks (Comparison of bookmarked values vary from tap to tap, i.e. tap-trello bookmarks on date values and will subtract a millisecond to include events at midnight.)
     - Effect of clock drift (ie. Is the bookmark set in a way that does not break if there is clock drift between the tap and the integration’s API?)
     - Timezone info, adding extra logging in the tests around bookmarks when an API includes timezone information can help with debugging.

    """
    BOOKMARK_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def name(self):
        return "tap_tester_bookmarks_test"

    def testable_streams(self):
        """expected streams minus the streams not under test"""
        return self.expected_streams().difference({
            'subscription_changes', # BUG_TDL-14938 https://jira.talendforge.org/browse/TDL-14938
            'campaigns',  # no create
            'owners',  # no create
            # 'email_events', # TODO
            # TODO This did not capture expected records on syncs 1 or 2 and capture less records on sync 2 than 1.
            'contacts_by_companies',
        })


    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None  # see all output in failure

        cls.my_timestamp = '2021-08-05T00:00:00.000000Z'
        cls.test_client = TestClient()

    def create_test_data(self, expected_streams):
        self.expected_records = {stream: []
                                 for stream in expected_streams}

        for stream in expected_streams:

            self.expected_records[stream] = []

            # create two records, one for updating later
            for _ in range(3):
                if stream in {'subscription_changes', 'email_events'}:
                    # TODO account for possibility of making too many records here? 
                    email_record, subscription_record = self.test_client.create(stream)
                    self.expected_records['email_events'] += email_record
                    # self.expected_records['subscription_changes'] += subscription_record # BUG_TDL-14938

                record = self.test_client.create(stream)
                self.expected_records[stream] += record

    def test_run(self):
        expected_streams = self.testable_streams()

        self.create_test_data(expected_streams)

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # TODO start date bug?
        # moving the state up so the sync will be shorter and the test takes less time
        state = {'bookmarks': {'companies': {'current_sync_start': None,
                                             'hs_lastmodifieddate': self.my_timestamp,
                                             'offset': {}}},
                 'currently_syncing': None}
        menagerie.set_state(conn_id, state)

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

        # Create 1 record for each stream between syncs
        for stream in expected_streams:
            record = self.test_client.create(stream)
            self.expected_records[stream] += record

        # Update records TODO

        #run second sync
        second_record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records_2 = runner.get_records_from_target_output()
        state_2 = menagerie.get_state(conn_id)

        # Test by Stream
        for stream in expected_streams:

            with self.subTest(stream=stream):

                # gather expected values
                replication_method = self.expected_replication_method()[stream]
                primary_keys = self.expected_primary_keys()[stream]

                # setting expected records for sync 1 based on the unsorted list of record
                # which does not inclue the created record between syncs 1 and 2
                expected_records_1 = self.expected_records[stream][:3]

                # gather replicated records
                actual_record_count_2 = second_record_count_by_stream[stream]
                actual_records_2 = [message['data']
                                    for message in synced_records_2[stream]['messages']
                                    if message['action'] == 'upsert']
                actual_record_count_1 = first_record_count_by_stream[stream]
                actual_records_1 = [message['data']
                                    for message in synced_records[stream]['messages']
                                    if message['action'] == 'upsert']


                if replication_method == self.INCREMENTAL:
                    # NB: FOR INCREMENTAL STREAMS he tap does not replicate the replication-key for any records.
                    #     It does functionaly replicate as a standard incremental sync would but does not order
                    #     records by replication-key value (since it does not exist on the record). To get around
                    #     this we are putting the replication-keys on our expected records via test_client. We will
                    #     verify the records we expect (via primary-key) are replicated prior to checking the
                    #     replication-key values.

                    # get saved states
                    stream_replication_key = list(self.expected_replication_keys()[stream])[0]
                    bookmark_1 = state_1['bookmarks'][stream][stream_replication_key]
                    bookmark_2 = state_2['bookmarks'][stream][stream_replication_key]

                    # TODO NB THIS THIGN
                    # # grab the replication key values for sync 1 and
                    # # grab expected number of records based on those replication-key values
                    # replication_key_values_1 = []
                    # for record in expected_records_1:
                    #     replication_key_values_1.append(record[stream_replication_key])

                    # for value in replication_key_values_1:
                    #     # ensure bookmarked state is the max rep key value from sync 1
                    #     self.assertLessEqual(value, bookmark_1)

                    # setting expected records  knowing they are ordered by replication-key value
                    expected_records_2 = self.expected_records[stream][-1:]

                    # TODO NB THIS THIGN
                    # # grab the replication key values for sync 2 and
                    # # grab expected number of records based on those replication-key values
                    # bookmarked_records_2 = []
                    # replication_key_values_2 = []
                    # for record in expected_records_2:
                    #     replication_key_values_2.append(record[stream_replication_key])
                    #     if record[stream_replication_key] == bookmark_2:
                    #         bookmarked_records_2.append(record)

                    # for value in replication_key_values_2:
                    #     # ensure bookmarked state is the max rep key value from sync 2
                    #     self.assertLessEqual(value, bookmark_2)

                    #     # ensure record replication key values are greater or equal to bookmarked value from sync 1
                    #     self.assertGreaterEqual(value, bookmark_1)
                    self.assertGreater(actual_record_count_1, actual_record_count_2)


                elif replication_method == self.FULL:
                    expected_records_2 = self.expected_records[stream]
                    if stream != 'contacts_by_company': # TODO BUG
                        self.assertEqual(actual_record_count_1 + 1, actual_record_count_2)

                else:
                    raise AssertionError(f"Replication method is {replication_method} for stream: {stream}")

                # verify by primary key that all expected records are replicated in sync 1
                sync_1_pks = [tuple([record[pk] for pk in primary_keys]) for record in actual_records_1]
                expected_sync_1_pks = [tuple([record[pk] for pk in primary_keys])
                                       for record in expected_records_1]
                for expected_pk in expected_sync_1_pks:
                    self.assertIn(expected_pk, sync_1_pks)

                # verify by primary key that all expected records are replicated in sync 2
                sync_2_pks = sorted([tuple([record[pk] for pk in primary_keys]) for record in actual_records_2])
                expected_sync_2_pks = sorted([tuple([record[pk] for pk in primary_keys])
                                              for record in expected_records_2])
                for expected_pk in expected_sync_2_pks:
                    self.assertIn(expected_pk, sync_2_pks)

                # verify that at least 1 record from the first sync is replicated in the 2nd sync
                # to prove that the bookmarking is inclusive
                self.assertTrue(any([expected_pk in sync_2_pks for expected_pk in expected_sync_1_pks]))
