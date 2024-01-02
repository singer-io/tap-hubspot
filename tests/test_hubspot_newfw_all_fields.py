import unittest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest
from tap_tester.logger import LOGGER
from base_hubspot import HubspotBaseCase
from client import TestClient

class HubspotAllFieldsTest(AllFieldsTest, HubspotBaseCase):
    """Hubspot all fields test implementation """
    EXTRA_FIELDS = HubspotBaseCase.EXTRA_FIELDS

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

        super().setUp()
        self.convert_datatype(self.expected_records)

    def convert_datatype(self, expected_records):
        # Convert the time stamp data type, Get keys with data and with no data 
        for stream, records in expected_records.items():
            expected_keys = set()
            for record in records:

                expected_keys = expected_keys.union(record.keys())
                # convert timestamps to string formatted datetime
                timestamp_keys = {'timestamp'}
                for key in timestamp_keys:
                    timestamp = record.get(key)
                    if timestamp:
                        record[key]=self.datetime_from_timestamp(timestamp/1000, str_format=self.BASIC_DATE_FORMAT)

            self.KEYS_WITH_NO_DATA[stream] = self.selected_fields.get(stream).difference(expected_keys)

        return expected_records

    def remove_bad_keys(self, stream):
        # NB: The following woraround is for dynamic fields on the `deals` stream that we just can't track.
        #     At the time of implementation there is no customer feedback indicating that these dynamic fields
        #     would prove useful to an end user. The ones that we replicated with the test client are specific
        #     to our test data. We have determined that the filtering of these fields is an expected behavior.
        # deals workaround for 'property_hs_date_entered_<property>' fields
        # BUG_TDL-14993 | https://jira.talendforge.org/browse/TDL-14993
        #                 Has an value of object with key 'value' and value 'Null'
        if stream == 'deals':
            bad_key_prefixes = {'property_hs_date_entered_', 'property_hs_date_exited_', 'property_hs_time_in'}
            bad_keys = set()
            for key in self.expected_all_keys:
                for bad_prefix in bad_key_prefixes:
                   if key.startswith(bad_prefix) and key not in self.fields_replicated:
                        bad_keys.add(key)
            for key in self.fields_replicated:
                for bad_prefix in bad_key_prefixes:
                   if key.startswith(bad_prefix) and key not in self.expected_all_keys:
                        bad_keys.add(key)

            for key in bad_keys:
                if key in self.expected_all_keys:
                    self.expected_all_keys.remove(key)
                elif key in self.fields_replicated:
                    self.fields_replicated.remove(key)

    ##########################################################################
    # Tests To Skip
    ##########################################################################

    @unittest.skip("Skip till all cards of missing fields are fixed. TDL-16145 ")
    def test_values_of_all_fields(self):
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

