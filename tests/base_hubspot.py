import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import tap_tester.menagerie   as menagerie
import tap_tester.connections as connections
import tap_tester.runner      as runner
from tap_tester.base_suite_tests.base_case import BaseCase
from tap_tester import LOGGER


class HubspotBaseCase(BaseCase):

    # set the default start date which can be overridden in the tests.
    start_date = BaseCase.timedelta_formatted(dt.utcnow(), delta=timedelta(days=-1))

    EXTRA_FIELDS = {
        "contacts": { "versionTimestamp" }
    }

    def setUp(self):
        missing_envs = [x for x in [
            'TAP_HUBSPOT_REDIRECT_URI',
            'TAP_HUBSPOT_CLIENT_ID',
            'TAP_HUBSPOT_CLIENT_SECRET',
            'TAP_HUBSPOT_REFRESH_TOKEN'
        ] if os.getenv(x) is None]
        if missing_envs:
            raise Exception("Missing environment variables: {}".format(missing_envs))

    @staticmethod
    def get_type():
        return "platform.hubspot"

    @staticmethod
    def tap_name():
        return "tap-hubspot"

    def get_properties(self):
        return {'start_date': self.start_date}

    def get_credentials(self):
        return {'refresh_token': os.getenv('TAP_HUBSPOT_REFRESH_TOKEN'),
                'client_secret': os.getenv('TAP_HUBSPOT_CLIENT_SECRET'),
                'redirect_uri':  os.getenv('TAP_HUBSPOT_REDIRECT_URI'),
                'client_id':     os.getenv('TAP_HUBSPOT_CLIENT_ID')}

    @classmethod
    def expected_metadata(cls):  # DOCS_BUG https://stitchdata.atlassian.net/browse/DOC-1523)
        """The expected streams and metadata about the streams"""

        return  {
            "campaigns": {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.FULL_TABLE,
                BaseCase.OBEYS_START_DATE: False
            },
            "companies": {
                BaseCase.PRIMARY_KEYS: {"companyId"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"property_hs_lastmodifieddate"},
                BaseCase.API_LIMIT: 250,
                BaseCase.OBEYS_START_DATE: True
            },
            "contact_lists": {
                BaseCase.PRIMARY_KEYS: {"listId"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updatedAt"},
                BaseCase.API_LIMIT: 250,
                BaseCase.OBEYS_START_DATE: True
            },
            "contacts": {
                BaseCase.PRIMARY_KEYS: {"vid"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"versionTimestamp"},
                BaseCase.API_LIMIT: 100,
                BaseCase.OBEYS_START_DATE: True
            },
            "contacts_by_company": {
                BaseCase.PRIMARY_KEYS: {"company-id", "contact-id"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.API_LIMIT: 100,
                BaseCase.OBEYS_START_DATE: True,
                BaseCase.PARENT_STREAM: 'companies'
            },
            "deal_pipelines": {
                BaseCase.PRIMARY_KEYS: {"pipelineId"},
                BaseCase.REPLICATION_METHOD: BaseCase.FULL_TABLE,
                BaseCase.OBEYS_START_DATE: False,
            },
            "deals": {
                BaseCase.PRIMARY_KEYS: {"dealId"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"property_hs_lastmodifieddate"},
                BaseCase.OBEYS_START_DATE: True
            },
            "email_events": {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"startTimestamp"},
                BaseCase.API_LIMIT: 1000,
                BaseCase.OBEYS_START_DATE: True
            },
            "engagements": {
                BaseCase.PRIMARY_KEYS: {"engagement_id"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"lastUpdated"},
                BaseCase.API_LIMIT: 250,
                BaseCase.OBEYS_START_DATE: True
            },
            "forms": {
                BaseCase.PRIMARY_KEYS: {"guid"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updatedAt"},
                BaseCase.OBEYS_START_DATE: True
            },
            "owners": {
                BaseCase.PRIMARY_KEYS: {"ownerId"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updatedAt"},
                BaseCase.OBEYS_START_DATE: True  # TODO is this a BUG?
            },
            "subscription_changes": {
                BaseCase.PRIMARY_KEYS: {"timestamp", "portalId", "recipient"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"startTimestamp"},
                BaseCase.API_LIMIT: 1000,
                BaseCase.OBEYS_START_DATE: True
            },
            "workflows": {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updatedAt"},
                BaseCase.OBEYS_START_DATE: True
            },
            "tickets": {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updatedAt"},
                BaseCase.API_LIMIT: 100,
                BaseCase.OBEYS_START_DATE: True
            },
            # below are the custom_objects stream
            "cars": {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updatedAt"},
                BaseCase.API_LIMIT: 100,
                BaseCase.EXPECTED_PAGE_SIZE: 100,
                BaseCase.OBEYS_START_DATE: True
            },
            "co_firsts": {
                BaseCase.PRIMARY_KEYS: {"id"},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {"updatedAt"},
                BaseCase.API_LIMIT: 100,
                BaseCase.EXPECTED_PAGE_SIZE: 100,
                BaseCase.OBEYS_START_DATE: True
            }

        }
