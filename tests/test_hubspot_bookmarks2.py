from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest

class HubSpotBookmarks2(unittest.TestCase):

    def name(self):
        return "tap_tester_hub_bookmarks_2"

    def expected_sync_streams(self):
        return {
            # "subscription_changes",
            # "email_events",
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
            "deal_pipelines"}

    def expected_offsets(self):
        return {'deals': {},
                'contact_lists': {},
                'email_events' : {},
                'contacts' : {},
                'campaigns' : {},
                'subscription_changes' : {},
                'engagements': {},
                'companies' : {}}

    def expected_bookmarks(self):
        return {'deals' :         ['hs_lastmodifieddate'],
                'contact_lists' : ['updatedAt'],
                'email_events' :  ['startTimestamp'],
                # 'contacts':       ['lastmodifieddate'],
                'workflows':      ['updatedAt'],
                'campaigns':      [],
                'owners' :        ['updatedAt'],
                'subscription_changes':  ['startTimestamp'],
                'engagements' :          ['lastUpdated'],
                'companies'  :           ['hs_lastmodifieddate'],
                'forms'      :           ['updatedAt'] }


    def get_properties(self):
        return {'start_date' : '2017-05-01T00:00:00Z'}

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

        #select all catalogs
        for catalog in found_catalogs:
            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, menagerie.get_annotated_schema(conn_id, catalog['stream_id']))

        future_time = "2050-01-01T00:00:00.000000Z"

        #clear state
        future_bookmarks = {"currently_syncing" : None,
                            "bookmarks":  {"contacts" : {"offset" : {},
                                                         "versionTimestamp" :  future_time},
                                           "subscription_changes" : {"startTimestamp" : future_time,
                                                                     "offset" :  {}},
                                           "campaigns" :  {"offset" : {}},
                                           "forms" : {"updatedAt" :  future_time},
                                           "deals" :  {"offset" :  {},
                                                       "hs_lastmodifieddate" :  future_time},
                                           "workflows" :  {"updatedAt" : future_time},
                                           "owners" :  {"updatedAt" :  future_time},
                                           "contact_lists" :  {"updatedAt" :  future_time,
                                                               "offset" :  {}},
                                           "email_events" :  {"startTimestamp" : future_time,
                                                              "offset" : {}},
                                           "companies" :  {"offset" : {},
                                                           "hs_lastmodifieddate" :  future_time},
                                           "engagements" :  {"lastUpdated" :  future_time,
                                                             "offset" : {}}}}

        menagerie.set_state(conn_id, future_bookmarks)

        sync_job_name = runner.run_sync_mode(self, conn_id)

        #verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self, conn_id, self.expected_sync_streams(), self.expected_primary_keys())

        #because the bookmarks were set into the future, we should NOT actually replicate any data.
        #minus campaigns, and deal_pipelines because those endpoints do NOT suppport bookmarks
        streams_with_bookmarks = self.expected_sync_streams()
        streams_with_bookmarks.remove('campaigns')
        streams_with_bookmarks.remove('deal_pipelines')
        bad_streams = streams_with_bookmarks.intersection(record_count_by_stream.keys())
        self.assertEqual(len(bad_streams), 0, msg="still pulled down records from {} despite future bookmarks".format(bad_streams))


        state = menagerie.get_state(conn_id)

        # NB: Companies and engagements won't set a bookmark in the future.
        state["bookmarks"].pop("companies")
        state["bookmarks"].pop("engagements")
        future_bookmarks["bookmarks"].pop("companies")
        future_bookmarks["bookmarks"].pop("engagements")

        self.assertEqual(state, future_bookmarks, msg="state should not have been modified because we didn't replicate any data")
        bookmarks = state.get('bookmarks')
        bookmark_streams = set(state.get('bookmarks').keys())




SCENARIOS.add(HubSpotBookmarks2)
