import os
import unittest

from functools import reduce

from singer import metadata
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest


KNOWN_MISSING_FIELDS = {
    'deals': {
        # This field requires attaching conferencing software to
        # Hubspot and booking a meeting as part of a deal
        'property_engagements_last_meeting_booked',
        # These 3 fields are derived from UTM codes attached to the above
        # meetings
        'property_engagements_last_meeting_booked_campaign',
        'property_engagements_last_meeting_booked_medium',
        'property_engagements_last_meeting_booked_source',
        # There's a way to associate a deal with a marketing campaign
        'property_hs_campaign',
        'property_hs_deal_amount_calculation_preference',
        # These are calculated properties
        'property_hs_likelihood_to_close',
        'property_hs_merged_object_ids',
        'property_hs_predicted_amount',
        'property_hs_predicted_amount_in_home_currency',
        'property_hs_sales_email_last_replied'
    },
}

class TestHubspotAllFields(HubspotBaseTest):
    """Test that with all fields selected for a stream we replicate data as expected"""

    def name(self):
        return "tap_tester_all_fields_all_fields_test"

    def testable_streams(self):
        return {
            'deals',
        }

    def test_run(self):
        conn_id = self.ensure_connection()

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

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

        # run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # read target output
        first_record_count_by_stream = runner.examine_target_output_file(self, conn_id,
                                                                         self.expected_streams(),
                                                                         self.expected_primary_keys())
        replicated_row_count =  reduce(lambda accum,c : accum + c, first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Test by Stream
        for stream in self.testable_streams():
            with self.subTest(stream=stream):

                expected_fields = set(synced_records.get(stream)['schema']['properties'].keys())
                print('Number of expected keys ', len(expected_fields))
                actual_fields = set(runner.examine_target_output_for_fields()[stream])
                print('Number of actual keys ', len(actual_fields))

                unexpected_fields = actual_fields & KNOWN_MISSING_FIELDS[stream]
                if unexpected_fields:
                    print('WARNING: Found new fields: {}'.format(unexpected_fields))
                self.assertSetEqual(expected_fields, actual_fields | KNOWN_MISSING_FIELDS[stream])
