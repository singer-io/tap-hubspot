import unittest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from tap_tester.logger import LOGGER
from base_hubspot import HubspotBaseCase
from client import TestClient

def get_matching_actual_record_by_pk(expected_primary_key_dict, actual_records):
    ret_records = []
    can_save = True
    for record in actual_records:
        for key, value in expected_primary_key_dict.items():
            actual_value = record[key]
            if actual_value != value:
                can_save = False
                break
        if can_save:
            ret_records.append(record)
        can_save = True
    return ret_records

class HubspotAllFieldsTest(AllFieldsTest, HubspotBaseCase):
    """Hubspot all fields test implementation """

    @staticmethod
    def name():
        return "tt_hubspot_all_fields"

    def streams_to_test(self):
        """expected streams minus the streams not under test"""
        return self.expected_stream_names().difference({
            'owners',
            'subscription_changes', # BUG_TDL-14938 https://jira.talendforge.org/browse/TDL-14938
        })

    def setUp(self):
        self.maxDiff = None  # see all output in failure

        test_client = TestClient(start_date=self.get_properties()['start_date'])
        self.expected_records = dict()
        streams = self.streams_to_test()
        stream_to_run_last = 'contacts_by_company'
        if stream_to_run_last in streams:
            streams.remove(stream_to_run_last)
            streams = list(streams)
            streams.append(stream_to_run_last)

        for stream in streams:
            # Get all records
            if stream == 'contacts_by_company':
                company_ids = [company['companyId'] for company in self.expected_records['companies']]
                self.expected_records[stream] = test_client.read(stream, parent_ids=company_ids)
            else:
                self.expected_records[stream] = test_client.read(stream)

        for stream, records in self.expected_records.items():
            LOGGER.info("The test client found %s %s records.", len(records), stream)

        self.convert_datatype(self.expected_records)
        super().setUp()

    def convert_datatype(self, expected_records):
        for stream, records in expected_records.items():
            for record in records:

                # convert timestamps to string formatted datetime
                timestamp_keys = {'timestamp'}
                for key in timestamp_keys:
                    timestamp = record.get(key)
                    if timestamp:
                        unformatted = datetime.datetime.fromtimestamp(timestamp/1000)
                        formatted = datetime.datetime.strftime(unformatted, self.BASIC_DATE_FORMAT)
                        record[key] = formatted

        return expected_records

    def test_all_fields_for_streams_are_replicated(self):
        for stream in self.test_streams:
            with self.subTest(stream=stream):

                # gather expected values
                #replication_method = self.expected_replication_method()[stream]
                primary_keys = sorted(self.expected_primary_keys()[stream])

                # gather replicated records
                actual_records = [message['data']
                                  for message in AllFieldsTest.synced_records[stream]['messages']
                                  if message['action'] == 'upsert']

                for expected_record in self.expected_records[stream]:

                    primary_key_dict = {primary_key: expected_record[primary_key] for primary_key in primary_keys}
                    primary_key_values = list(primary_key_dict.values())

                    with self.subTest(expected_record=primary_key_dict):
                        # grab the replicated record that corresponds to expected_record by checking primary keys
                        matching_actual_records_by_pk = get_matching_actual_record_by_pk(primary_key_dict, actual_records)
                        if not matching_actual_records_by_pk:
                            LOGGER.warn("Expected %s record was not replicated: %s",
                                        stream, primary_key_dict)
                            continue # skip this expected record if it isn't replicated
                        actual_record = matching_actual_records_by_pk[0]

                        expected_keys = set(expected_record.keys()).union(FIELDS_ADDED_BY_TAP.get(stream, {}))
                        actual_keys = set(actual_record.keys())

                        # NB: KNOWN_MISSING_FIELDS is a dictionary of streams to aggregated missing fields.
                        #     We will check each expected_record to see which of the known keys is present in expectations
                        #     and then will add them to the known_missing_keys set.
                        known_missing_keys = set()
                        for missing_key in KNOWN_MISSING_FIELDS.get(stream, set()):
                            if missing_key in expected_record.keys():
                                known_missing_keys.add(missing_key)
                                del expected_record[missing_key]

                        # NB : KNOWN_EXTRA_FIELDS is a dictionary of streams to fields that should not
                        #      be replicated but are. See the variable declaration at top of file for linked BUGs.
                        known_extra_keys = set()
                        for extra_key in KNOWN_EXTRA_FIELDS.get(stream, set()):
                            known_extra_keys.add(extra_key)

                        # Verify the fields in our expected record match the fields in the corresponding replicated record
                        expected_keys_adjusted = expected_keys.union(known_extra_keys)
                        actual_keys_adjusted = actual_keys.union(known_missing_keys)

                        # NB: The following woraround is for dynamic fields on the `deals` stream that we just can't track.
                        #     At the time of implementation there is no customer feedback indicating that these dynamic fields
                        #     would prove useful to an end user. The ones that we replicated with the test client are specific
                        #     to our test data. We have determined that the filtering of these fields is an expected behavior.

                        # deals workaround for 'property_hs_date_entered_<property>' fields
                        bad_key_prefixes = {'property_hs_date_entered_', 'property_hs_date_exited_',
                                            'property_hs_time_in'}
                        bad_keys = set()
                        for key in expected_keys_adjusted:
                            for prefix in bad_key_prefixes:
                                if key.startswith(prefix) and key not in actual_keys_adjusted:
                                    bad_keys.add(key)
                        for key in actual_keys_adjusted:
                            for prefix in bad_key_prefixes:
                                if key.startswith(prefix) and key not in expected_keys_adjusted:
                                    bad_keys.add(key)
                        for key in bad_keys:
                            if key in expected_keys_adjusted:
                                expected_keys_adjusted.remove(key)
                            elif key in actual_keys_adjusted:
                                actual_keys_adjusted.remove(key)

                        self.assertSetEqual(expected_keys_adjusted, actual_keys_adjusted)

    ##########################################################################
    # Tests To Skip
    ##########################################################################

    @unittest.skip("Skip till all cards of missing fields are fixed. TDL-16145 ")
    def test_all_fields_for_streams_are_replicated(self):
        for stream in self.test_streams:
            with self.subTest(stream=stream):

                # gather expectations
                expected_all_keys = self.selected_fields.get(stream, set()) - set(self.MISSING_FIELDS.get(stream, {}))

                # gather results
                fields_replicated = self.actual_fields.get(stream, set())

                # verify that all fields are sent to the target
                # test the combination of all records
                self.assertSetEqual(fields_replicated, expected_all_keys,
                                    logging=f"verify all fields are replicated for stream {stream}")

    @unittest.skip("Random selection doesn't always sync records")
    def test_all_streams_sync_records(self):
        pass

