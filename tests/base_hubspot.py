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

    
    FIELDS_ADDED_BY_TAP = {
        # In 'contacts' streams 'versionTimeStamp' is not available in response of the second call.
        # In the 1st call, Tap retrieves records of all contacts and from those records, it collects vids(id of contact).
        # These records contain the versionTimestamp field.
        # In the 2nd call, vids collected from the 1st call will be used to retrieve the whole contact record.
        # Here, the records collected for detailed contact information do not contain the versionTimestamp field.
        # So, we add the versionTimestamp field(fetched from 1st call records) explicitly in the record of 2nd call.
        "contacts": { "versionTimestamp" }
    }
    
    KNOWN_EXTRA_FIELDS = {
        'deals': {
            # BUG_TDL-14993 | https://jira.talendforge.org/browse/TDL-14993
            #                 Has an value of object with key 'value' and value 'Null'
            'property_hs_date_entered_1258834',
            'property_hs_time_in_example_stage1660743867503491_315775040'
        },
    }
    
    KNOWN_MISSING_FIELDS = {
        'contacts':{ # BUG https://jira.talendforge.org/browse/TDL-16016
            'property_hs_latest_source',
            'property_hs_latest_source_data_1',
            'property_hs_latest_source_data_2',
            'property_hs_latest_source_timestamp',
            'property_hs_timezone',
            'property_hs_v2_cumulative_time_in_lead',
            'property_hs_v2_cumulative_time_in_opportunity',
            'property_hs_v2_cumulative_time_in_subscriber',
            'property_hs_v2_date_entered_customer',
            'property_hs_v2_date_entered_lead',
            'property_hs_v2_date_entered_opportunity',
            'property_hs_v2_date_entered_subscriber',
            'property_hs_v2_date_exited_lead',
            'property_hs_v2_date_exited_opportunity',
            'property_hs_v2_date_exited_subscriber',
            'property_hs_v2_latest_time_in_lead',
            'property_hs_v2_latest_time_in_opportunity',
            'property_hs_v2_latest_time_in_subscriber',
        },
        'contact_lists': {  # BUG https://jira.talendforge.org/browse/TDL-14996
            'authorId',
            'teamIds',
            'internal',
            'ilsFilterBranch',
            'limitExempt',
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
        'engagements': {  # BUG https://jira.talendforge.org/browse/TDL-14997
            'scheduledTasks',
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
            'embedVersion',
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
            'paymentSessionTemplateIds',
            'selectedExternalOptions',
        },
        'companies': {  # BUG https://jira.talendforge.org/browse/TDL-15003
            'mergeAudits',
            'stateChanges',
            'isDeleted',
            'additionalDomains',
            'property_hs_analytics_latest_source',
            'property_hs_analytics_latest_source_data_2',
            'property_hs_analytics_latest_source_data_1',
            'property_hs_analytics_latest_source_timestamp',
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
            'property_hs_num_associated_deal_registrations',
            'property_hs_analytics_latest_source',
            'property_hs_analytics_latest_source_timestamp_contact',
            'property_hs_analytics_latest_source_data_1_contact',
            'property_hs_analytics_latest_source_timestamp',
            'property_hs_analytics_latest_source_data_1',
            'property_hs_analytics_latest_source_contact',
            'property_hs_analytics_latest_source_company',
            'property_hs_analytics_latest_source_data_1_company',
            'property_hs_analytics_latest_source_data_2_company',
            'property_hs_analytics_latest_source_data_2',
            'property_hs_analytics_latest_source_data_2_contact',
        },
        'subscription_changes':{
            'normalizedEmailId'
        }
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
        return {'start_date': self.start_date }

    def get_credentials(self):
        return {'refresh_token': os.getenv('TAP_HUBSPOT_REFRESH_TOKEN'),
                'client_secret': os.getenv('TAP_HUBSPOT_CLIENT_SECRET'),
                'redirect_uri':  os.getenv('TAP_HUBSPOT_REDIRECT_URI'),
                'client_id':     os.getenv('TAP_HUBSPOT_CLIENT_ID')}

    def expected_check_streams(self):
        return set(self.expected_metadata().keys())

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
                HubspotBaseCase.PARENT_STREAM: 'companies'
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
            }
        }
