import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest


class TestHubspotPagination(HubspotBaseTest):
    def name(self):
        return "tap_tester_hubspot_pagination_test"

    def expected_page_size(self):
        # TODO verify which  streams paginate and what are the limits
        # TODO abstract this expectation into base metadata expectations
        return {
            # "subscription_changes": 10 - 1000, # TODO
            "subscription_changes": 1000, # TODO
            # "email_events": 10 - 1000, # TODO
            "email_events": 1000, # TODO
            # "forms": ??, # TODO #infinity
            "workflows": ??, # TODO
            # "owners": ??, # TODO
            # "campaigns": ??, # TODO
            # "campaigns": 500, # TODO # Can't make test data
            # "contact_lists": 20 - 250, # TODO  #count, # TODO
            # "deal_pipelines": ?? , # TODO # deprecated
            # "contacts_by_company": 100, # TODO  #count # deprecated
            "contact_lists": 250, # TODO
            "contacts": 100, # DONE
            "companies": 250, # TODO
            "deals": 100, # TODO
            "engagements": 250, # TODO
        }

    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None  # see all output in failure

        cls.my_timestamp = cls.get_properties(cls)['start_date']

        test_client = TestClient()
        existing_records = dict()

        # TODO there is no need to get records for streams not under test
        existing_records['campaigns'] = test_client.get_campaigns()
        existing_records['forms'] = test_client.get_forms()
        existing_records['owners'] = test_client.get_owners()
        existing_records['engagements'] = test_client.get_engagements()
        existing_records['workflows'] = test_client.get_workflows()
        existing_records['email_events'] = test_client.get_email_events()
        existing_records['contact_lists'] = test_client.get_contact_lists()
        existing_records['contacts'] = test_client.get_contacts()
        existing_records['companies'] = test_client.get_companies(since=cls.my_timestamp)
        company_ids = [company['companyId'] for company in existing_records['companies']]
        existing_records['contacts_by_company'] = test_client.get_contacts_by_company(parent_ids=company_ids)
        existing_records['deal_pipelines'] = test_client.get_deal_pipelines()
        existing_records['deals'] = test_client.get_deals()
        # existing_records['subscription_changes'] = test_client.get_subscription_changes()  # see BUG_TDL-14938
        #check if additional records are needed and create records if so
        streams = self.expected_streams()
        limits = self.expected_page_size() # TODO This should be set off of the base expectations
        for stream in streams:
            # Get all records
            # check if we meet the limit
            if len(self.expected_records[stream]) < limits[stream]:
                if stream = "contacts":
                    # create records to exceed limit
                    test_client.create(stream)
                    # TODO get the create dispatch function
                    # TODO create n records for each stream


                
    def expected_streams(self): # TODO this should run off of base expectations
        """
        All streams are under test
        """
        return set(self.expected_page_size().keys()).difference({
            'subscription_changes',
            'email_events',
        })

    def get_properties(self):
        return {
            'start_date' : '2017-01-01T00:00:00Z',  # TODO make this 1 week ago
        }

    # TODO card out boundary testing for future tap-tester upgrades
    #      

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        expected_streams = self.expected_streams()
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries, select_all_fields=True)

        sync_record_count = self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()


        # Test by stream
        for stream in self.expected_streams():
            with self.subTest(stream=stream):

                record_count = sync_record_count.get(stream, 0)

                sync_messages = sync_records.get(stream, {'messages': []}).get('messages')

                primary_key = self.expected_primary_keys().get(stream).pop()

                # Verify the sync meets or exceeds the default record count
                stream_page_size = self.expected_page_size()[stream]
                self.assertLess(stream_page_size, record_count)

                # Verify we did not duplicate any records across pages
                records_pks_set = {message.get('data').get(primary_key) for message in sync_messages}
                records_pks_list = [message.get('data').get(primary_key) for message in sync_messages]
                self.assertCountEqual(records_pks_set, records_pks_list,
                                      msg="We have duplicate records for {}".format(stream))
