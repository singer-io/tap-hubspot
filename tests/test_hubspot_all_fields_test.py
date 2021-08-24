import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest
from client import TestClient

KNOWN_MISSING_FIELDS = { # TODO need to write up the following discrepancy
    'contact_lists': {
        'authorId',
        'teamIds'
    },
    'email_events': {
        'portalSubscriptionStatus',
        'attempt',
        'source',
        'subscriptions',
        'sourceId',
        'replyTo',
        'suppressedMessage',
        'bcc',
        'suppressedReason',
        'cc',
     },
    'workflows': {
        'migrationStatus',
        'updateSource',
        'description',
        'originalAuthorUserId',
        'lastUpdatedByUserId',
        'creationSource',
        'portalId',
        'contactCounts',
    },
    'owners': {
        'activeSalesforceId'
    },
    'forms': {
        'alwaysCreateNewCompany',
        'themeColor',
        'publishAt',
        'editVersion',
        'themeName',
        'style',
        'thankYouMessageJson',
        'createMarketableContact',
        'kickbackEmailWorkflowId',
        'businessUnitId',
        'portableKey',
        'parentId',
        'kickbackEmailsJson',
        'unpublishAt',
        'internalUpdatedAt',
        'multivariateTest',
        'publishedAt',
        'customUid',
        'isPublished',
    },
    'companies': {
        'mergeAudits',
        'stateChanges',
        'isDeleted',
        'additionalDomains',
    },
    'campaigns': {
        'lastProcessingStateChangeAt',
        'lastProcessingFinishedAt',
        'processingState',
        'lastProcessingStartedAt',
    },
    # 'deals': {
    #     # This field requires attaching conferencing software to
    #     # Hubspot and booking a meeting as part of a deal
    #     'property_engagements_last_meeting_booked',
    #     # These 3 fields are derived from UTM codes attached to the above
    #     # meetings
    #     'property_engagements_last_meeting_booked_campaign',
    #     'property_engagements_last_meeting_booked_medium',
    #     'property_engagements_last_meeting_booked_source',
    #     # There's a way to associate a deal with a marketing campaign
    #     'property_hs_campaign',
    #     'property_hs_deal_amount_calculation_preference',
    #     # These are calculated properties
    #     'property_hs_likelihood_to_close',
    #     'property_hs_merged_object_ids',
    #     'property_hs_predicted_amount',
    #     'property_hs_predicted_amount_in_home_currency',
    #     'property_hs_sales_email_last_replied',
    #     # These we have no data for
    #     'property_hs_date_entered_appointmentscheduled',
    #     'property_hs_date_entered_decisionmakerboughtin',
    #     'property_hs_date_exited_qualifiedtobuy',
    #     'property_hs_time_in_closedwon',
    #     'property_hs_date_exited_appointmentscheduled',
    #     'property_hs_time_in_decisionmakerboughtin',
    #     'property_hs_date_exited_closedlost',
    #     'property_hs_time_in_closedlost',
    #     'property_hs_date_entered_closedlost',
    #     'property_hs_date_entered_closedwon',
    #     'property_hs_date_exited_contractsent',
    #     'property_hs_time_in_presentationscheduled',
    #     'property_hs_date_exited_presentationscheduled',
    #     'property_hs_time_in_qualifiedtobuy',
    #     'property_hs_date_exited_decisionmakerboughtin',
    #     'property_hs_time_in_contractsent',
    #     'property_hs_time_in_appointmentscheduled',
    #     'property_hs_date_entered_presentationscheduled',
    #     'property_hs_date_entered_qualifiedtobuy',
    #     'property_hs_date_entered_contractsent',
    #     'property_hs_date_exited_closedwon',
    #     # BUG https://jira.talendforge.org/browse/TDL-9886
    #     #     The following streams have been added since tests were written
    #     'property_hs_all_assigned_business_unit_ids',
    #     'property_hs_unique_creation_key',
    #     'property_hs_num_target_accounts',
    #     'property_hs_priority',
    #     'property_hs_user_ids_of_all_notification_unfollowers',
    #     'property_hs_deal_stage_probability_shadow',
    #     'property_hs_user_ids_of_all_notification_followers',
    # },
}

class TestHubspotAllFields(HubspotBaseTest):
    """Test that with all fields selected for a stream we replicate data as expected"""

    def name(self):
        return "tap_tester_all_fields_all_fields_test"

    def testable_streams(self):
        return {
            # 'deals',
            'deal_pipelines',
            'contacts',  # pass
            'companies', # pass
            'campaigns', # pass
            'contact_lists', # pass
            'contacts_by_company', # 
            'email_events', # pass
            'engagements', # pass
            'forms', # pass
            'owners', # pass
            'workflows', # pass
            # 'subscription_changes', # TODO cannot be tested easily without a valid pk
        }

    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None  # see all output in failure

        cls.my_timestamp = '2021-08-05T00:00:00.000000Z'

        test_client = TestClient()
        cls.expected_records = dict()

        # pass
        cls.expected_records['campaigns'] = test_client.get_campaigns()
        cls.expected_records['forms'] = test_client.get_forms()
        cls.expected_records['owners'] = test_client.get_owners()
        cls.expected_records['engagements'] = test_client.get_engagements()
        cls.expected_records['workflows'] = test_client.get_workflows()
        cls.expected_records['email_events'] = test_client.get_email_events()
        cls.expected_records['contact_lists'] = test_client.get_contact_lists()
        cls.expected_records['contacts'] = test_client.get_contacts()
        cls.expected_records['companies'] = test_client.get_companies(since=cls.my_timestamp)
        company_ids = [company['companyId'] for company in cls.expected_records['companies']]
        cls.expected_records['contacts_by_company'] = test_client.get_contacts_by_company(parent_ids=company_ids)
        cls.expected_records['deal_pipelines'] = test_client.get_deal_pipelines()
        # fail
        # cls.expected_records['subscription_changes'] = test_client.get_subscription_changes()

        # TODO
        # cls.expected_records['deals'] = test_client.get_deals() # TODO work with dev to get passing

        for stream, records in cls.expected_records.items():
            print(f"The test client found {len(records)} {stream} records.")

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # moving the state up so the sync will be shorter and the test takes less time
        state = {'bookmarks': {'companies': {'current_sync_start': None,
                                             'hs_lastmodifieddate': self.my_timestamp,
                                             'offset': {}}},
                 'currently_syncing': None}
        menagerie.set_state(conn_id, state)

        # Select only the expected streams tables
        expected_streams = self.testable_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        for catalog_entry in catalog_entries:
            stream_schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog_entry,
                stream_schema
            )

        # Run sync
        first_record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Test by Stream
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # gather expected values
                replication_method = self.expected_replication_method()[stream]
                primary_keys = self.expected_primary_keys()[stream]
                expected_primary_key_values = [[record[primary_key] for primary_key in primary_keys]
                                               for record in self.expected_records[stream]]

                # gather replicated records
                actual_records = [message['data']
                                  for message in synced_records[stream]['messages']
                                  if message['action'] == 'upsert']

                for expected_record in self.expected_records[stream]:

                    primary_key_dict = {primary_key: expected_record[primary_key] for primary_key in primary_keys}
                    primary_key_values = list(primary_key_dict.values())

                    with self.subTest(expected_record=primary_key_dict):

                        # grab the replicated record that corresponds to expected_record by checking primary keys
                        matching_actual_records_by_pk = [record for record in actual_records
                                                         if primary_key_values == [record[primary_key]
                                                                                   for primary_key in primary_keys]]
                        self.assertEqual(1, len(matching_actual_records_by_pk))
                        actual_record = matching_actual_records_by_pk[0]


                        # TODO BUG
                        #      KNOWN_MISSING_FIELDS is a dictionary of streams to aggregated missing fields.
                        #      We will check each expected_record to see which of the known keys is present in expectations
                        #      and then will add them to the known_missing_keys set.
                        known_missing_keys = set()
                        for missing_key in KNOWN_MISSING_FIELDS.get(stream, set()):
                            if missing_key in expected_record.keys():
                                known_missing_keys.add(missing_key)

                        # Verify the fields in our expected record match the fields in the corresponding replicated record
                        self.assertSetEqual(
                            set(expected_record.keys()), set(actual_record.keys()).union(known_missing_keys)
                        )

                # TODO PUT BACK

                # BUG ? TEST ISSUE ? TODO
                # if stream not in {'companies'}:
                #     # Verify by primary key values that only the expected records were replicated
                #     actual_records_primary_key_values = [[record[primary_key]
                #                                           for primary_key in primary_keys]
                #                                          for record in actual_records]
                #     self.assertEqual(expected_primary_key_values, actual_records_primary_key_values)
