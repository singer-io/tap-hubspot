import os
import datetime
import unittest
from functools import reduce
import datetime

from singer import utils
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

from base import HubspotBaseTest

class HubSpotBookmarks1(HubspotBaseTest):
    def name(self):
        return "tap_tester_hub_bookmarks_1"

    def get_properties(self):  # TODO Determine if we can move this forward so it syncs quicker
        return {'start_date' : '2017-05-01T00:00:00Z'}

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

    def expected_sync_streams(self):
        return {
            #"subscription_changes",
            #"email_events",
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
                # 'email_events' :  ['startTimestamp'],
                # 'contacts':       ['versionTimestamp'],
                'workflows':      ['updatedAt'],
                'campaigns':      [],
                'contacts_by_company' : [],
                'owners' :        ['updatedAt'],
                # 'subscription_changes':  ['startTimestamp'],
                'engagements' :          ['lastUpdated'],
                'companies'  :           ['hs_lastmodifieddate'],
                'forms'      :           ['updatedAt'] }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select all Catalogs
        for catalog in found_catalogs:
            if catalog['tap_stream_id'] in self.expected_sync_streams():
                connections.select_catalog_and_fields_via_metadata(conn_id, catalog, menagerie.get_annotated_schema(conn_id, catalog['stream_id']))

        #clear state
        menagerie.set_state(conn_id, {})

        record_count_by_stream = self.run_and_verify_sync(conn_id)

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
                self.assertEqual(utils.strptime_with_tz(bk_value),
                                 utils.strptime_with_tz(max_bookmarks_from_records[k]),
                                 "Bookmark {} ({}) for stream {} should have been updated to {}".format(bk_value, w, k, max_bookmarks_from_records[k]))
                print("bookmark {}({}) updated to {} from max record value {}".format(k, w, bk_value, max_bookmarks_from_records[k]))

        for k,v in self.expected_offsets().items():
            self.assertEqual(bookmarks.get(k,{}).get('offset', {}), v, msg="unexpected offset found for stream {} {}. state: {}".format(k, v, state))
            print("offsets {} cleared".format(k))

        diff = bookmark_streams.difference(self.acceptable_bookmarks())
        self.assertEqual(len(diff), 0, msg="Unexpected bookmarks: {} Expected: {} Actual: {}".format(diff, self.acceptable_bookmarks(), bookmarks))

        self.assertEqual(state.get('currently_syncing'), None,"Unexpected `currently_syncing` bookmark value: {} Expected: None".format(state.get('currently_syncing')))


SCENARIOS.add(HubSpotBookmarks1)
