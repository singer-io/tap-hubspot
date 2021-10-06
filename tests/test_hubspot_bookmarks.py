from datetime import datetime, timedelta
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
        return "tt_hubspot_bookmarks"

    def streams_to_test(self):
        """expected streams minus the streams not under test"""
        return {
            "companies",
            "contact_lists",
            # "contacts",
            # "contacts_by_company",
            "deal_pipelines",
            "deals",
            # "email_events",
            # "engagements",
            # "forms",
            # "subscription_changes",
            # "workflows",
        }

    def asdf_streams_to_test(self):
        """expected streams minus the streams not under test"""
        return self.expected_streams().difference({
            'campaigns',  # no create
            'owners',  # no create
            'subscription_changes', # BUG_TDL-14938 https://jira.talendforge.org/browse/TDL-14938
        })

    def get_properties(self):
        return {
            'start_date' : datetime.strftime(datetime.today()-timedelta(days=3), self.START_DATE_FORMAT),
        }

    def setUp(self):
        self.maxDiff = None  # see all output in failure

        self.test_client = TestClient(self.get_properties()['start_date'])

    def create_test_data(self, expected_streams):
        self.expected_records = {stream: []
                                 for stream in expected_streams}

        for stream in expected_streams:
            if stream == 'email_events':
                email_records = self.test_client.create(stream, times=3)
                self.expected_records['email_events'] += email_records
                #     # # self.expected_records['subscription_changes'] += subscription_record # BUG_TDL-14938
                # else:
            else:
                # create records, one will be updated between syncs
                for _ in range(3):
                    record = self.test_client.create(stream)
                    self.expected_records[stream] += record

    def test_run(self):
        expected_streams = self.streams_to_test()

        self.create_test_data(expected_streams)

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

        # Create 1 record for each stream between syncs
        for stream in expected_streams:
            record = self.test_client.create(stream)
            self.expected_records[stream] += record

        # Update records TODO
        for stream in {'companies', 'contact_lists', 'deals', 'deal_pipelines'}:
            if stream == 'workflows':
                workflow_id = self.expected_records[stream][0]['id']
                contact_email = self.expected_records['contacts'][0]['properties']['email']['value']
                record = self.test_client.update_workflows(workflow_id, contact_email)
            elif stream == 'companies':
                company_id = self.expected_records[stream][0]['companyId']
                record = self.test_client.update_companies(company_id)
            elif stream == 'contacts':
                contact_id = self.expected_records[stream][0]['vid']
                record = self.test_client.update_contacts(contact_id)
            elif stream == 'contact_lists':
                contact_list_id = self.expected_records[stream][0]['listId']
                record = self.test_client.update_contact_lists(contact_list_id)
            elif stream == 'deal_pipelines':
                deal_pipeline_id = self.expected_records[stream][0]['pipelineId']
                record = self.test_client.update_deal_pipelines(deal_pipeline_id)
            elif stream == 'deals':
                deal_id = self.expected_records[stream][0]['dealId']
                record = self.test_client.update_deals(deal_id)
            elif stream == 'forms':
                form_id = self.expected_records[stream][0]['guid']
                record = self.test_client.update_forms(form_id)
            elif stream == 'engagements':
                engagement_id = self.expected_records[stream][0]['engagement_id']
                record = self.test_client.update_engagements(engagement_id)

            self.expected_records[stream].append(record)

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

                    # TODO NB THIS THING
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

                    # TODO NB THIS THING
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
                    if stream != 'contacts_by_company': # TODO BUG_1
                        self.assertEqual(actual_record_count_1 + 1, actual_record_count_2)

                else:
                    raise AssertionError(f"Replication method is {replication_method} for stream: {stream}")

                # verify by primary key that all expected records are replicated in sync 1
                if stream in {'contacts_by_company'}:  # BUG_1 |TODO
                    continue
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
                if stream in {'contacts', # BUG | https://jira.talendforge.org/browse/TDL-15502
                              'companies'}: # BUG https://jira.talendforge.org/browse/TDL-15503
                    continue  # skipping failures
                self.assertTrue(any([expected_pk in sync_2_pks for expected_pk in expected_sync_1_pks]))
