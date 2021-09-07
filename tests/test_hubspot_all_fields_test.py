import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest
from client import TestClient

KNOWN_EXTRA_FIELDS = {
    'deals': {
        # BUG_TDL-14993 | https://jira.talendforge.org/browse/TDL-14993
        #                 Has an value of object with key 'value' and value 'Null'
        'property_hs_date_entered_1258834',
    },
}

KNOWN_MISSING_FIELDS = {
    'contact_lists': {  # BUG https://jira.talendforge.org/browse/TDL-14996
        'authorId',
        'teamIds'
    },
    'email_events': {  # BUG https://jira.talendforge.org/browse/TDL-14997
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
    'workflows': {  # BUG https://jira.talendforge.org/browse/TDL-14998
        'migrationStatus',
        'updateSource',
        'description',
        'originalAuthorUserId',
        'lastUpdatedByUserId',
        'creationSource',
        'portalId',
        'contactCounts',
    },
    'owners': {  # BUG https://jira.talendforge.org/browse/TDL-15000
        'activeSalesforceId'
    },
    'forms': {  # BUG https://jira.talendforge.org/browse/TDL-15001
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
    'companies': {  # BUG https://jira.talendforge.org/browse/TDL-15003
        'mergeAudits',
        'stateChanges',
        'isDeleted',
        'additionalDomains',
    },
    'campaigns': {  # BUG https://jira.talendforge.org/browse/TDL-15003
        'lastProcessingStateChangeAt',
        'lastProcessingFinishedAt',
        'processingState',
        'lastProcessingStartedAt',
    },
    'deals': {  # BUG https://jira.talendforge.org/browse/TDL-14999
        'imports',
        'property_hs_num_associated_deal_splits',
        'property_hs_is_deal_split',
        'stateChanges',
        'property_hs_num_associated_active_deal_registrations',
        'property_hs_num_associated_deal_registrations'
    },
}

class TestHubspotAllFields(HubspotBaseTest):
    """Test that with all fields selected for a stream we replicate data as expected"""

    def name(self):
        return "tap_tester_all_fields_all_fields_test"

    def testable_streams(self):
        """expected streams minus the streams not under test"""
        return self.expected_streams().difference({
            'subscription_changes', # BUG_TDL-14938 https://jira.talendforge.org/browse/TDL-14938
        })

    def get_properties(self):
        return {'start_date' : '2021-08-05T00:00:00Z'}

    # TODO move the overriden start date up as much as possible to minimize the run time
    #      it can probably be dynamic like today minus 7 days

    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None  # see all output in failure

        # TODO my_timestamp needs to be driven off of start_date
        # do a strptim on get_prop[start_date]
        # Then do a strftime using this format with fractional seconds
        
        cls.my_timestamp = '2021-08-05T00:00:00.000000Z'

        test_client = TestClient()
        cls.expected_records = dict()

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
        cls.expected_records['deals'] = test_client.get_deals()
        # cls.expected_records['subscription_changes'] = test_client.get_subscription_changes()  # see BUG_TDL-14938

        # cls.expected_records = get_expected_records_from_test_client() # TODO?

        for stream, records in cls.expected_records.items():
            print(f"The test client found {len(records)} {stream} records.")

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # # moving the state up so the sync will be shorter and the test takes less time
        # state = {'bookmarks': {'companies': {'current_sync_start': None,
        #                                      'hs_lastmodifieddate': self.my_timestamp,
        #                                      'offset': {}}},
        #          'currently_syncing': None}
        # menagerie.set_state(conn_id, state)

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


                        # NB: KNOWN_MISSING_FIELDS is a dictionary of streams to aggregated missing fields.
                        #     We will check each expected_record to see which of the known keys is present in expectations
                        #     and then will add them to the known_missing_keys set.
                        known_missing_keys = set()
                        for missing_key in KNOWN_MISSING_FIELDS.get(stream, set()):
                            if missing_key in expected_record.keys():
                                known_missing_keys.add(missing_key)

                        # NB : KNOWN_EXTRA_FIELDS is a dictionary of streams to fields that should not
                        #      be replicated but are. See the variable declaration at top of file for linked BUGs.
                        known_extra_keys = set()
                        for extra_key in KNOWN_EXTRA_FIELDS.get(stream, set()):
                            known_extra_keys.add(extra_key)


                        # Verify the fields in our expected record match the fields in the corresponding replicated record
                        expected_keys_adjusted = set(expected_record.keys()).union(known_extra_keys)
                        actual_keys_adjusted = set(actual_record.keys()).union(known_missing_keys)
                        # TODO There are dynamic fields on here that we just can't track.
                        #      But shouldn't we be doing dynamic field discovery on these things? BUG?
                        # deals workaround for 'property_hs_date_entered_<property>' fields
                        bad_key_prefix = 'property_hs_date_entered_'
                        bad_keys = set()
                        for key in expected_keys_adjusted:
                            if key.startswith(bad_key_prefix) and key not in actual_keys_adjusted:
                                bad_keys.add(key)
                        for key in actual_keys_adjusted:
                            if key.startswith(bad_key_prefix) and key not in expected_keys_adjusted:
                                bad_keys.add(key)
                        for key in bad_keys:
                            if key in expected_keys_adjusted:
                                expected_keys_adjusted.remove(key)
                            elif key in actual_keys_adjusted:
                                actual_keys_adjusted.remove(key)

                        self.assertSetEqual(expected_keys_adjusted, actual_keys_adjusted)

                # Verify by primary key values that only the expected records were replicated
                expected_primary_key_values = {tuple([record[primary_key]
                                                      for primary_key in primary_keys])
                                               for record in self.expected_records[stream]}
                actual_records_primary_key_values = {tuple([record[primary_key]
                                                            for primary_key in primary_keys])
                                                     for record in actual_records}
                self.assertSetEqual(expected_primary_key_values, actual_records_primary_key_values)
