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
        'property_hs_sales_email_last_replied',
        # These we have no data for
        'property_hs_date_entered_appointmentscheduled',
        'property_hs_date_entered_decisionmakerboughtin',
        'property_hs_date_exited_qualifiedtobuy',
        'property_hs_time_in_closedwon',
        'property_hs_date_exited_appointmentscheduled',
        'property_hs_time_in_decisionmakerboughtin',
        'property_hs_date_exited_closedlost',
        'property_hs_time_in_closedlost',
        'property_hs_date_entered_closedlost',
        'property_hs_date_entered_closedwon',
        'property_hs_date_exited_contractsent',
        'property_hs_time_in_presentationscheduled',
        'property_hs_date_exited_presentationscheduled',
        'property_hs_time_in_qualifiedtobuy',
        'property_hs_date_exited_decisionmakerboughtin',
        'property_hs_time_in_contractsent',
        'property_hs_time_in_appointmentscheduled',
        'property_hs_date_entered_presentationscheduled',
        'property_hs_date_entered_qualifiedtobuy',
        'property_hs_date_entered_contractsent',
        'property_hs_date_exited_closedwon',
        # BUG https://jira.talendforge.org/browse/TDL-9886
        #     The following streams have been added since tests were written
        'property_hs_all_assigned_business_unit_ids',
        'property_hs_unique_creation_key',
        'property_hs_num_target_accounts',
        'property_hs_priority',
        'property_hs_user_ids_of_all_notification_unfollowers',
        'property_hs_deal_stage_probability_shadow',
        'property_hs_user_ids_of_all_notification_followers',
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
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

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

        replicated_row_count = sum(first_record_count_by_stream.values())
        synced_records = runner.get_records_from_target_output()

        # Test by Stream
        for stream in self.testable_streams():
            with self.subTest(stream=stream):

                expected_fields = set(synced_records.get(stream)['schema']['properties'].keys())
                print('Number of expected keys ', len(expected_fields))
                actual_fields = set(runner.examine_target_output_for_fields()[stream])
                print('Number of actual keys ', len(actual_fields))
                print('Number of known missing keys ', len(KNOWN_MISSING_FIELDS[stream]))

                unexpected_fields = actual_fields & KNOWN_MISSING_FIELDS[stream]
                if unexpected_fields:
                    print('WARNING: Found new fields: {}'.format(unexpected_fields))
                self.assertSetEqual(expected_fields, actual_fields | KNOWN_MISSING_FIELDS[stream])
