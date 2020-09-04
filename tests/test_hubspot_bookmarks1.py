from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import datetime
import unittest
from functools import reduce
from singer import utils
import datetime
import pdb

class HubSpotBookmarks1(unittest.TestCase):
    def setUp(self):
        missing_envs = [x for x in [os.getenv('TAP_HUBSPOT_REDIRECT_URI'),
                                    os.getenv('TAP_HUBSPOT_CLIENT_ID'),
                                    os.getenv('TAP_HUBSPOT_CLIENT_SECRET'),
                                    os.getenv('TAP_HUBSPOT_REFRESH_TOKEN')] if x == None]
        if len(missing_envs) != 0:
            #pylint: disable=line-too-long
            raise Exception("set TAP_HUBSPOT_REDIRECT_URI, TAP_HUBSPOT_CLIENT_ID, TAP_HUBSPOT_CLIENT_SECRET, TAP_HUBSPOT_REFRESH_TOKEN")

    def name(self):
        return "tap_tester_hub_bookmarks_1"

    def tap_name(self):
        return "tap-hubspot"

    def get_type(self):
        return "platform.hubspot"

    def get_credentials(self):
        return {'refresh_token': os.getenv('TAP_HUBSPOT_REFRESH_TOKEN'),
                'client_secret': os.getenv('TAP_HUBSPOT_CLIENT_SECRET'),
                'redirect_uri':  os.getenv('TAP_HUBSPOT_REDIRECT_URI'),
                'client_id':     os.getenv('TAP_HUBSPOT_CLIENT_ID')}

    def expected_pks(self):
        return {
            "subscription_changes" : {"timestamp", "portalId", "recipient"},
            "email_events" :         {'id'},
            "forms" :                {"guid"},
            "workflows" :            {"id"},
            "owners" :               {"ownerId"},
            "campaigns" :            {"id"},
            "contact_lists":         {"listId"},
            "contacts" :             {'vid'},
            "companies":             {"companyId"},
            "deals":                 {"dealId"},
            "engagements":           {"engagement_id"},
            "contacts_by_company" : {"company-id", "contact-id"},
            "deal_pipelines" : {"pipelineId"},
        }

    def acceptable_bookmarks(self):
        return {
            "subscription_changes",
            "email_events",
            "forms",
            "workflows",
            "owners",
            "campaigns",
            "contact_lists",
            "contacts",
            "companies",
            "deals",
            "engagements",
            "contacts_by_company",
        }

    def expected_check_streams(self):
        return {
            "subscription_changes",
            "email_events",
            "forms",
            "workflows",
            "owners",
            "campaigns",
            "contact_lists",
            "contacts",
            "companies",
            "deals",
            "engagements",
            "deal_pipelines",
            "contacts_by_company"
        }

    def expected_sync_streams(self):
        return {
            "subscription_changes",
            "email_events",
            "forms",
            "workflows",
            "owners",
            "campaigns",
            "contact_lists",
            "contacts",
            "companies",
            "deals",
            "engagements",
            "contacts_by_company",
            "deal_pipelines",
        }

    def expected_offsets(self):
        return {'deals': {},
                'contact_lists': {},
                'email_events' : {},
                'contacts' : {},
                'campaigns' : {},
                'subscription_changes' : {},
                'engagements': {},
                'companies' : {}}


    def record_to_bk_value(self, stream, record):
        # Deals and Companies records have been transformed so the bookmark
        # is prefixed by "property_". There is a nest map structure beneath the value.

        if stream == 'companies':
            bk_value = record.get('property_hs_lastmodifieddate') or record.get('createdate')
            if bk_value is None:
                return None
            return bk_value.get('value')

        if stream == 'deals':
            bk_value = record.get('property_hs_lastmodifieddate')
            if bk_value is None:
                return None
            return bk_value.get('value')
        else:
            bk_columns = self.expected_bookmarks().get(stream, [])
            if len(bk_columns) == 0:
                return None

            bk_column = bk_columns[0] #only consider first bookmark

            bk_value = record.get(bk_column)
            if not bk_value:
                raise Exception("Record received without bookmark value for stream {}: {}".format(stream, record))
            return utils.strftime(utils.strptime_with_tz(bk_value))

    def expected_bookmarks(self):
        return {'deals' :         ['hs_lastmodifieddate'],
                'contact_lists' : ['updatedAt'],
                'email_events' :  ['startTimestamp'],
                # 'contacts':       ['versionTimestamp'],
                'workflows':      ['updatedAt'],
                'campaigns':      [],
                'contacts_by_company' : [],
                'owners' :        ['updatedAt'],
                'subscription_changes':  ['startTimestamp'],
                'engagements' :          ['lastUpdated'],
                'companies'  :           ['hs_lastmodifieddate'],
                'forms'      :           ['updatedAt'] }


    def get_properties(self):
        return {'start_date' : '2017-05-01 00:00:00'}

    def perform_field_selection(self, conn_id, catalog):
        schema = menagerie.select_catalog(conn_id, catalog)

        return {'key_properties' :     catalog.get('key_properties'),
                'schema' :             schema,
                'tap_stream_id':       catalog.get('tap_stream_id'),
                'replication_method' : catalog.get('replication_method'),
                'replication_key'    : catalog.get('replication_key')}

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        #run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        #verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))

        diff = self.expected_check_streams().symmetric_difference( found_catalog_names )
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))
        print("discovered schemas are kosher")

        # Select all Catalogs
        for catalog in found_catalogs:
            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, menagerie.get_annotated_schema(conn_id, catalog['stream_id']))

        #clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_pks())
        replicated_row_count =  reduce(lambda accum,c : accum + c, record_count_by_stream.values())
        self.assertGreater(replicated_row_count, 0, msg="failed to replicate any data: {}".format(record_count_by_stream))
        print("total replicated row count: {}".format(replicated_row_count))

        max_bookmarks_from_records = runner.get_most_recent_records_from_target(self, self.expected_bookmarks(), self.get_properties()['start_date'])

        start_of_today =  utils.strftime(datetime.datetime(datetime.datetime.utcnow().year, datetime.datetime.utcnow().month, datetime.datetime.utcnow().day, 0, 0, 0, 0, datetime.timezone.utc))
        max_bookmarks_from_records['subscription_changes'] = start_of_today
        max_bookmarks_from_records['email_events'] = start_of_today


        #if we didn't replicate data, the bookmark should be the start_date
        for k in self.expected_bookmarks().keys():
            if max_bookmarks_from_records.get(k) is None:
                max_bookmarks_from_records[k] = utils.strftime(datetime.datetime(2017, 5, 1, 0, 0, 0, 0, datetime.timezone.utc))

        state = menagerie.get_state(conn_id)
        bookmarks = state.get('bookmarks')
        bookmark_streams = set(state.get('bookmarks').keys())

        #verify bookmarks and offsets
        for k,v in sorted(list(self.expected_bookmarks().items())):
            for w in v:
                bk_value = bookmarks.get(k,{}).get(w)
                self.assertEqual(utils.strptime_with_tz(bk_value), utils.strptime_with_tz(max_bookmarks_from_records[k]), "Bookmark {} ({}) for stream {} should have been updated to {}".format(bk_value, w, k, max_bookmarks_from_records[k]))
                print("bookmark {}({}) updated to {} from max record value {}".format(k, w, bk_value, max_bookmarks_from_records[k]))

        for k,v in self.expected_offsets().items():
            self.assertEqual(bookmarks.get(k,{}).get('offset', {}), v, msg="unexpected offset found for stream {} {}. state: {}".format(k, v, state))
            print("offsets {} cleared".format(k))

        diff = bookmark_streams.difference(self.acceptable_bookmarks())
        self.assertEqual(len(diff), 0, msg="Unexpected bookmarks: {} Expected: {} Actual: {}".format(diff, self.acceptable_bookmarks(), bookmarks))

        self.assertEqual(state.get('currently_syncing'), None,"Unexpected `currently_syncing` bookmark value: {} Expected: None".format(state.get('currently_syncing')))


SCENARIOS.add(HubSpotBookmarks1)
