import datetime
import requests
import backoff
import json
import uuid
import random
from  tap_tester import menagerie
from base import HubspotBaseTest


BASE_URL = "https://api.hubapi.com"


class TestClient():
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    V3_DEALS_PROPERTY_PREFIXES = {'hs_date_entered', 'hs_date_exited', 'hs_time_in'}

    ##########################################################################
    ### CORE METHODS
    ##########################################################################

    def giveup(exc):
        """Checks a response status code, returns True if unsuccessful unless rate limited."""
        return exc.response is not None \
            and 400 <= exc.response.status_code < 500 \
            and exc.response.status_code != 429

    @backoff.on_exception(backoff.constant,
                          (requests.exceptions.RequestException,
                           requests.exceptions.HTTPError),
                          max_tries=5,
                          jitter=None,
                          giveup=giveup,
                          interval=10)
    def get(self, url, params=dict()):
        """Perform a GET using the standard requests method and logs the action"""
        response = requests.get(url, params=params, headers=self.HEADERS)
        print(f"TEST CLIENT | GET {url} params={params}  STATUS: {response.status_code}")
        response.raise_for_status()
        json_response = response.json()

        return json_response

    @backoff.on_exception(backoff.constant,
                          (requests.exceptions.RequestException,
                           requests.exceptions.HTTPError),
                          max_tries=5,
                          jitter=None,
                          giveup=giveup,
                          interval=10)
    def post(self, url, data, params=dict(), debug=False):
        """Perfroma a POST using the standard requests method and log the action"""

        headers = dict(self.HEADERS)
        headers['content-type'] = "application/json"
        response = requests.post(url, json=data, params=params, headers=headers)
        print(f"TEST CLIENT | POST {url} data={data} params={params}  STATUS: {response.status_code}")
        if debug:
            print(response.text)
        response.raise_for_status()
        json_response = response.json()

        return json_response

    @backoff.on_exception(backoff.constant,
                          (requests.exceptions.RequestException,
                           requests.exceptions.HTTPError),
                          max_tries=5,
                          jitter=None,
                          giveup=giveup,
                          interval=10)
    def put(self, url, data, params=dict()):
        """Perfroma a PUT using the standard requests method and log the action"""
        headers = dict(self.HEADERS)
        headers['content-type'] = "application/json"
        response = requests.put(url, json=data, params=params, headers=headers)
        print(f"TEST CLIENT | PUT {url} data={data} params={params}  STATUS: {response.status_code}")
        response.raise_for_status()

    def denest_properties(self, stream, records):
        """
        Takes a list of records and checks each for a 'properties' key to denest.
        Returns the list of denested records.
        """
        for record in records:
            if record.get('properties'):
                for property_key, property_value in record['properties'].items():

                    # if any property has a versions object track it by the top level key 'properties_versions'
                    if property_value.get('versions'):
                        if not record.get('properties_versions'):
                            record['properties_versions'] = []
                        record['properties_versions'] += property_value['versions']

                    # denest each property to be a top level key
                    record[f'property_{property_key}'] = property_value

        print(f"TEST CLIENT | Transforming {len(records)} {stream} records")
        return records

    ##########################################################################
    ### GET
    ##########################################################################
    def read(self, stream, parent_ids=[], since=''):
        if stream == 'forms':
            return self.get_forms()
        elif stream == 'owners':
            return self.get_owners()
        elif stream == 'companies':
            return self.get_companies(since)
        elif stream == 'contact_lists':
            return self.get_contact_lists(since)
        elif stream == 'contacts_by_company':
            return self.get_contacts_by_company(parent_ids)
        elif stream == 'engagements':
            return self.get_engagements()
        elif stream == 'campaigns':
            return self.get_campaigns()
        elif stream == 'deals':
            return self.get_deals()
        elif stream == 'workflows':
            return self.get_workflows()
        elif stream == 'contacts':
            return self.get_contacts()
        elif stream == 'deal_pipelines':
            return self.get_deal_pipelines()
        elif stream == 'email_events':
            return self.get_email_events()
        elif stream == 'subscription_changes':
            return self.get_subscription_changes(since)
        else:
            raise NotImplementedError
    def get_campaigns(self):
        """
        Get all campaigns by id, then grab the details of each campaign.
        """
        campaign_by_id_url = f"{BASE_URL}/email/public/v1/campaigns/by-id"
        campaign_url = f"{BASE_URL}/email/public/v1/campaigns/"

        # get all campaigns by-id
        response = self.get(campaign_by_id_url)
        campaign_ids = [campaign['id'] for campaign in response['campaigns']]

        # get the detailed record corresponding to each campagin-id
        records = []
        for campaign_id in campaign_ids:
            url = f"{campaign_url}{campaign_id}"
            response = self.get(url)
            records.append(response)

        return records

    def get_companies(self, since):
        """
        Get all companies by paginating using 'hasMore' and 'offset'.
        """
        url = f"{BASE_URL}/companies/v2/companies/paged"
        if not since:
            since = self.start_date_strf

        if not isinstance(since, datetime.datetime):
            since = datetime.datetime.strptime(since, self.START_DATE_FORMAT)
        params = {'properties': ["createdate", "hs_lastmodifieddate"]}
        records = []

        # paginating through all the companies
        companies = []
        has_more = True
        while has_more:

            response = self.get(url, params=params)
            # TODO refactor this? so that we get start date for free?
            for company in response['companies']:
                if company['properties']['hs_lastmodifieddate']:
                    company_timestamp = datetime.datetime.fromtimestamp(
                        company['properties']['hs_lastmodifieddate']['timestamp']/1000
                    )
                else:
                    company_timestamp = datetime.datetime.fromtimestamp(
                        company['properties']['createdate']['timestamp']/1000
                    )

                if company_timestamp >= since:
                    companies.append(company)

            has_more = response['has-more']
            params['offset'] = response['offset']

        # get the details of each company
        for company in companies:
            url = f"{BASE_URL}/companies/v2/companies/{company['companyId']}"
            response = self.get(url)
            records.append(response)

        records = self.denest_properties('companies', records)

        return records

    def get_contact_lists(self, since):
        """
        Get all contact_lists by paginating using 'has-more' and 'offset'.
        """
        url = f"{BASE_URL}/contacts/v1/lists"
        if not since:
            since = self.start_date_strf

        if not isinstance(since, datetime.datetime):
            since = datetime.datetime.strptime(since, self.START_DATE_FORMAT)
        since = str(since.timestamp() * 1000).split(".")[0]
        params = {'since': since}

        records = []
        replication_key = list(self.replication_keys['contact_lists'])[0]
        # paginating through all the contact_lists
        has_more = True
        while has_more:

            response = self.get(url, params=params)
            for record in response['lists']:
                if int(since) <= record[replication_key]:
                    records.append(record)

            has_more = response['has-more']
            params['offset'] = response['offset']

        return records

    def get_contacts(self):
        """
        Get all contact vids by paginating using 'has-more' and 'vid-offset/vidOffset'.
        Then use the vids to grab the detailed contacts records.
        """
        url_1 = f"{BASE_URL}/contacts/v1/lists/all/contacts/all"
        params_1 = {
            'showListMemberships': True,
            'includeVersion': True,
            'count': 100,
        }
        vids = []
        url_2 = f"{BASE_URL}/contacts/v1/contact/vids/batch/"
        params_2 = {
            'showListMemberships': True,
            'formSubmissionMode': "all",
        }
        records = []

        has_more = True
        while has_more:
            # get a page worth of contacts and pull the vids
            response_1 = self.get(url_1, params=params_1)
            vids = [record['vid'] for record in response_1['contacts']
                    if record['versionTimestamp'] >= self.start_date]

            has_more = response_1['has-more']
            params_1['vidOffset'] = response_1['vid-offset']

            # get the detailed contacts records by vids
            params_2['vid'] = vids
            response_2 = self.get(url_2, params=params_2)
            records.extend([record for record in response_2.values()])

        records = self.denest_properties('contacts', records)
        return records


    def get_contacts_by_company(self, parent_ids):
        """
        Get all contacts_by_company iterating over compnayId's and
        paginating using 'hasMore' and 'vidOffset'. This stream is essentially
        a join on contacts and companies.
        NB: This stream is a CHILD of 'companies'. If any test needs to pull expected
            data from this endpoint, it requires getting all 'companies' data and then
            pulling the 'companyId' from each record to perform the corresponding get here.
        """

        url = f"{BASE_URL}/companies/v2/companies/{{}}/vids"
        params = dict()
        records = []

        for parent_id in parent_ids:
            child_url = url.format(parent_id)
            has_more = True
            while has_more:

                response = self.get(child_url, params=params)
                for vid in response.get('vids', {}):
                    records.extend([{'company-id': parent_id,
                                     'contact-id': vid}])

                has_more = response['hasMore']
                params['vidOffset'] = response['vidOffset']

            params = dict()

        return records

    def get_deal_pipelines(self):
        """
        Get all deal_pipelines.
        """
        url = f"{BASE_URL}/deals/v1/pipelines"
        records = []

        response = self.get(url)
        records.extend(response)

        records = self.denest_properties('deal_pipelines', records)
        return records

    def get_deals(self):
        """
        Get all deals from the v1 endpoiint by paginating using 'hasMore' and 'offset'.
        For each deals record denest 'properties' so that they are prefxed with 'property_'
        and located at the top level.
        """
        v1_url = f"{BASE_URL}/deals/v1/deal/paged"

        v1_params = {'includeAllProperties': True,
                     'allPropertiesFetchMode': 'latest_version',
                     'properties' : []}
        replication_key = list(self.replication_keys['deals'])[0]
        records = []

        # hit the v1 endpoint to get the record
        has_more = True
        while has_more:

            response = self.get(v1_url, params=v1_params)
            records.extend([record for record in response['deals']
                            if record['properties'][replication_key]['timestamp'] >= self.start_date])
            has_more = response['hasMore']
            v1_params['offset'] = response['offset']

        # batch the v1 response ids into groups of 100
        v1_ids = [{'id': str(record['dealId'])} for record in records]
        batches = []
        batch_size = 100
        for i in range(0, len(v1_ids), batch_size):
            batches.append(v1_ids[i:i + batch_size])

        # hit the v3 endpoint to get the special hs_<whatever> fields from v3 'properties'
        v3_url = f"{BASE_URL}/crm/v3/objects/deals/batch/read"
        v3_property = ['hs_date_entered_appointmentscheduled']
        v3_records = []
        for batch in batches:
            data = {'inputs': batch,
                    'properties': v3_property}
            v3_response = self.post(v3_url, data)
            v3_records += v3_response['results']

        # pull the desired properties from the v3 records and add them to correspond  v1 records
        for v3_record in v3_records:
            for record in records:
                if v3_record['id'] == str(record['dealId']):

                    # don't inclue the v3 property if the value is None
                    non_null_v3_properties = {v3_property_key: v3_property_value
                                              for v3_property_key, v3_property_value in v3_record['properties'].items()
                                              if v3_property_value is not None}

                    # only grab v3 properties with a specific prefix
                    trimmed_v3_properties = {v3_property_key: v3_property_value
                                             for v3_property_key, v3_property_value in non_null_v3_properties.items()
                                             if any([v3_property_key.startswith(prefix)
                                                     for prefix in self.V3_DEALS_PROPERTY_PREFIXES])}

                    # the v3 properties must be restructured into objects to match v1
                    v3_properties = {v3_property_key: {'value': v3_property_value}
                                     for v3_property_key, v3_property_value in trimmed_v3_properties.items()}

                    # add the v3 record properties to the v1 record
                    record['properties'].update(v3_properties)

        records = self.denest_properties('deals', records)
        return records

    def get_email_events(self):
        """
        Get all email_events by paginating using 'hasMore' and 'offset'.
        """
        url = f"{BASE_URL}/email/public/v1/events"
        replication_key = list(self.replication_keys['email_events'])[0]
        params = dict()
        records = []

        has_more = True
        while has_more:

            response = self.get(url, params=params)
            records.extend([record for record in response['events']
                            if record['created'] >= self.start_date])

            has_more = response['hasMore']
            params['offset'] = response['offset']

        return records

    def get_engagements(self):
        """
        Get all engagements by paginating using 'hasMore' and 'offset'.
        """
        url = f"{BASE_URL}/engagements/v1/engagements/paged"
        replication_key = list(self.replication_keys['engagements'])[0]
        params = dict()
        records = []

        has_more = True
        while has_more:

            response = self.get(url, params=params)
            for result in response['results']:
                if result['engagement'][replication_key] >= self.start_date:
                    result['engagement_id'] = result['engagement']['id']
                    result['lastUpdated'] = result['engagement']['lastUpdated']
                    records.append(result)


            has_more = response['hasMore']
            params['offset'] = response['offset']

        return records

    def get_forms(self):
        """
        Get all forms.
        """
        url = f"{BASE_URL}/forms/v2/forms"
        replication_key = list(self.replication_keys['forms'])[0]
        records = []

        response = self.get(url)
        records.extend([record for record in response
                        if record[replication_key] >= self.start_date])

        return records
    def get_owners(self):
        """
        Get all owners.
        """
        url = f"{BASE_URL}/owners/v2/owners"
        records = self.get(url)

        return records

    def get_subscription_changes(self, since=''):
        """
        Get all subscription_changes from 'since' date by paginating using 'hasMore' and 'offset'.
        Default since date is one week ago
        """
        url = f"{BASE_URL}/email/public/v1/subscriptions/timeline"
        params = dict()
        records = []
        replication_key = list(self.replication_keys['subscription_changes'])[0]
        if not since:
            since = self.start_date_strf

        if not isinstance(since, datetime.datetime):
            since = datetime.datetime.strptime(since, self.START_DATE_FORMAT)
        since = str(since.timestamp() * 1000).split(".")[0]
        # copied overparams = {'properties': ["createdate", "hs_lastmodifieddate"]}
        has_more = True
        while has_more:
            response = self.get(url, params=params)
            has_more = response['hasMore']
            params['offset'] = response['offset']
            for record in response['timeline']:
                if int(since) <= record['timestamp']: # TODO bug timestamp is the replication method rather than replication_method which is startTimestamp
                    records.append(record)

        return records

    def get_workflows(self):
        """
        Get all workflows.
        """
        url = f"{BASE_URL}/automation/v3/workflows"
        replication_key = list(self.replication_keys['workflows'])[0]
        records = []

        response = self.get(url)
        records.extend([record for record in response['workflows']
                        if record[replication_key] >= self.start_date])

        return records

    ##########################################################################
    ### CREATE
    ##########################################################################

    def create(self, stream, company_ids=[], subscriptions=[], times=1):
        """Dispatch create to make tests clean."""
        if stream == 'forms':
            return self.create_forms()
        elif stream == 'owners':
            return self.create_owners()
        elif stream == 'companies':
            return self.create_companies()
        elif stream == 'contact_lists':
            return self.create_contact_lists()
        elif stream == 'contacts_by_company':
            return self.create_contacts_by_company(company_ids)
        elif stream == 'engagements':
            return self.create_engagements()
        elif stream == 'campaigns':
            return self.create_campaigns()
        elif stream == 'deals':
            return self.create_deals()
        elif stream == 'workflows':
            return self.create_workflows()
        elif stream == 'contacts':
            return self.create_contacts()
        elif stream == 'deal_pipelines':
            return self.create_deal_pipelines()
        elif stream == 'email_events':
            print(
                f"TEST CLIENT | WARNING Calling the create_subscription_changes method to generate {stream} records"
            )
            return self.create_subscription_changes()
        elif stream == 'subscription_changes':
            return self.create_subscription_changes(subscriptions, times)
        else:
            raise NotImplementedError(f"There is no create_{stream} method in this dipatch!")

    def create_contacts(self):
        """
        Generate a single contacts record.
        Hubspot API https://legacydocs.hubspot.com/docs/methods/contacts/create_contact
        """
        record_uuid = str(uuid.uuid4()).replace('-', '')

        url = f"{BASE_URL}/contacts/v1/contact"
        data = {
            "properties": [
                {
                   "property": "email",
                   "value": f"{record_uuid}@stitchdata.com"
                 },
                 {
                   "property": "firstname",
                   "value": "Yusaku"
                 },
                 {
                   "property": "lastname",
                   "value": "Kasahara"
                 },
                 {
                   "property": "website",
                   "value": "http://app.stitchdata.com"
                 },
                 {
                   "property": "company",
                   "value": "Talend"
                 },
                 {
                   "property": "phone",
                   "value": "555-122-2323"
                 },
                 {
                   "property": "address",
                   "value": "25 First Street"
                 },
                 {
                   "property": "city",
                   "value": "Cambridge"
                 },
                 {
                   "property": "state",
                   "value": "MA"
                 },
                 {
                   "property": "zip",
                   "value": "02139"
                 }
               ]
             }

        # generate a contacts record
        response = self.post(url, data)
        records = [response]
        return records

    def create_campaigns(self):
        """
        TODO couldn't find endpoint...
        """
        # record_uuid = str(uuid.uuid4()).replace('-', '')

        # url = f"{BASE_URL}"
        # data = {}
        raise NotImplementedError("TODO SPIKE needed on create campaign since there was no endpoint")
        # generate a record
        # response = self.post(url, data)
        # records = [response]
        # return records

    def create_companies(self):
        """
        It takes about 6 seconds after the POST for the created record to be caught by the next GET.
        This is intended for generating one record for companies.
        HubSpot API https://legacydocs.hubspot.com/docs/methods/companies/create_company
        """
        record_uuid = str(uuid.uuid4()).replace('-', '')

        url = f"{BASE_URL}/companies/v2/companies/"
        data = {"properties": [{"name": "name", "value": f"Company Name {record_uuid}"},
                               {"name": "description", "value": "company description"}]}

        # generate a record
        response = self.post(url, data)
        records = [response]
        return records

    def create_contact_lists(self):
        """
        HubSpot API https://legacydocs.hubspot.com/docs/methods/lists/create_list
        """
        record_uuid = str(uuid.uuid4()).replace('-', '')

        url = f"{BASE_URL}/contacts/v1/lists/"
        data = {
            "name": f"tweeters{record_uuid}",
            "dynamic": True,
            "filters":[
                [{
                    "operator": "EQ",
                    "value": f"@hubspot{record_uuid}",
                    "property": "twitterhandle",
                    "type": "string"
                }]
            ]
        }
        #TODO generate different filters
        # generate a record
        response = self.post(url, data)
        records = [response]
        return records

    def create_contacts_by_company(self, company_ids):
        """
        https://legacydocs.hubspot.com/docs/methods/companies/add_contact_to_company
        https://legacydocs.hubspot.com/docs/methods/crm-associations/associate-objects
        """
        url = f"{BASE_URL}/crm-associations/v1/associations"
        # only use contacts-company combinations that do not exist yet
        contact_records = self.get_contacts()
        contacts_by_company_records = self.get_contacts_by_company(set(company_ids))
        for company_id in set(company_ids):
            for contact in contact_records:
                # look for a contact that is not already in the contacts_by_company list
                if contact['vid'] not in [record['contact-id'] for record in contacts_by_company_records]:
                    contact_id = contact['vid']
                    data = {
                        "fromObjectId": company_id,
                        "toObjectId": contact_id,
                        "category": "HUBSPOT_DEFINED",
                        "definitionId": 2
                    }
                    # generate a record
                    self.put(url, data)
                    records = [{'company-id': company_id, 'contact-id': contact_id}]
                    return records

        raise NotImplementedError("All contacts already have an associated company")

    def create_deal_pipelines(self):
        """
        HubSpot API
        https://legacydocs.hubspot.com/docs/methods/pipelines/create_new_pipeline
        """
        record_uuid = str(uuid.uuid4()).replace('-', '')
        record_uuid2 = str(uuid.uuid4()).replace('-', '')

        url = f"{BASE_URL}/crm-pipelines/v1/pipelines/deals"
        data = {
            "pipelineId": record_uuid,
            "label": f"API test ticket pipeline {record_uuid}",
            "displayOrder": 2,
            "active": True,
            "stages": [
                {
                    "stageId": f"example_stage {record_uuid}",
                    "label": f"Example stage{record_uuid}",
                    "displayOrder": 1,
                    "metadata": {
                        "probability": 0.5
                    }
                },
                {
                    "stageId": f"another_example_stage{record_uuid2}",
                    "label": f"Another example stage{record_uuid2}",
                    "displayOrder": 2,
                    "metadata": {
                        "probability": 1.0
                    }
                }
            ]
        }

        # generate a record
        response = self.post(url, data)
        records = [response]
        return records

    def create_deals(self):
        """
        HubSpot API https://legacydocs.hubspot.com/docs/methods/deals/create_deal
        """
        record_uuid = str(uuid.uuid4()).replace('-', '')

        url = f"{BASE_URL}/deals/v1/deal/"
        #TODO need to use various pipelines and stages
        data = {
            "associations": {
                "associatedCompanyIds": [
                    6804176293
                ],
                "associatedVids": [
                    2304
                ]
            },
            "properties": [
                {
                    "value": "Tim's Newer Deal",
                    "name": "dealname"
                },
                {
                    "value": "appointmentscheduled",
                    "name": "dealstage"
                },
                {
                    "value": "default",
                    "name": "pipeline"
                },
                {
                    "value": "98621200",
                    "name": "hubspot_owner_id"
                },
                {
                    "value": 1409443200000,
                    "name": "closedate"
                },
                {
                    "value": "60000",
                    "name": "amount"
                },
            {
                "value": "newbusiness",
                "name": "dealtype"
            }
            ]
        }

        # generate a record
        response = self.post(url, data)
        records = [response]
        return records

    def create_email_events(self):
        """
        HubSpot API  https://legacydocs.hubspot.com/docs/methods/email/email_events_overview
        TODO We are able to create email_events by updating email subscription status with a PUT (create_subscription_changes()). If trying to expand data for other email_events, browser automation with an email application may be required
        """

        raise NotImplementedError("Use create_subscription_changes instead to create records for email_events stream")

    def create_engagements(self):
        """
        HubSpot API https://legacydocs.hubspot.com/docs/methods/engagements/create_engagement
        TODO - dependent on valid (currently hardcoded) contactId, companyId, and ownerId
        """
        record_uuid = str(uuid.uuid4()).replace('-', '')

        url = f"{BASE_URL}/engagements/v1/engagements"
        data = {
            "engagement": {
                "active": True,
                "ownerId": 98621200,
                "type": "NOTE",
                "timestamp": 1409172644778
            },
            "associations": {
                "contactIds": [2304],
                "companyIds": [6804176293],
                "dealIds": [ ],
                "ownerIds": [ ],
		"ticketIds":[ ]
            },
            "attachments": [
                {
                    "id": 4241968539
                }
            ],
            "metadata": {
                "body": "note body"
            }
        }

        # generate a record
        response = self.post(url, data)
        records = [response]
        return records

    def create_forms(self):
        """
        HubSpot API https://legacydocs.hubspot.com/docs/methods/forms/v2/create_form
        """
        record_uuid = str(uuid.uuid4()).replace('-', '')

        url = f"{BASE_URL}/forms/v2/forms"
        data = {
            "name": f"DemoForm{record_uuid}",
            "action": "",
            "method": "",
            "cssClass": "",
            "redirect": "",
            "submitText": "Submit",
            "followUpId": "",
            "notifyRecipients": "",
            "leadNurturingCampaignId": "",
            "formFieldGroups": [
                {
                    "fields": [
                        {
                            "name": "firstname",
                            "label": "First Name",
                            "type": "string",
                            "fieldType": "text",
                            "description": "",
                            "groupName": "",
                            "displayOrder": 0,
                            "required": False,
                            "selectedOptions": [],
                            "options": [],
                            "validation": {
                                "name": "",
                                "message": "",
                                "data": "",
                                "useDefaultBlockList": False
                            },
                            "enabled": True,
                            "hidden": False,
                            "defaultValue": "",
                            "isSmartField": False,
                            "unselectedLabel": "",
                            "placeholder": ""
                        }
                    ],
                    "default": True,
                    "isSmartGroup": False
                },
                {
                    "fields": [
                        {
                            "name": "lastname",
                            "label": "Last Name",
                            "type": "string",
                            "fieldType": "text",
                            "description": "",
                            "groupName": "",
                            "displayOrder": 1,
                            "required": False,
                            "selectedOptions": [],
                            "options": [],
                            "validation": {
                                "name": "",
                                "message": "",
                                "data": "",
                                "useDefaultBlockList": False
                            },
                            "enabled": True,
                            "hidden": False,
                            "defaultValue": "",
                            "isSmartField": False,
                            "unselectedLabel": "",
                            "placeholder": ""
                        }
                    ],
                    "default": True,
                    "isSmartGroup": False
                },
                {
                    "fields": [
                        {
                            "name": "adress_1",
                            "label": "Adress 1",
                            "type": "string",
                            "fieldType": "text",
                            "description": "",
                            "groupName": "",
                            "displayOrder": 2,
                            "required": False,
                            "selectedOptions": [],
                            "options": [],
                            "validation": {
                                "name": "",
                                "message": "",
                                "data": "",
                                "useDefaultBlockList": False
                            },
                            "enabled": True,
                            "hidden": False,
                            "defaultValue": "",
                            "isSmartField": False,
                            "unselectedLabel": "",
                            "placeholder": ""
                        }
                    ],
                    "default": True,
                    "isSmartGroup": False
                }
            ],
            "createdAt": 1318534279910,
            "updatedAt": 1413919291011,
            "performableHtml": "",
            "migratedFrom": "ld",
            "ignoreCurrentValues": False,
            "metaData": [],
            "deletable": True
        }

        # generate a record
        response = self.post(url, data)
        records = [response]
        return records

    def create_owners(self):
        """
        HubSpot API The Owners API is read-only. Owners can only be created in HubSpot.
        TODO - use selenium
        """
        raise NotImplementedError("Only able to create owners from web app")

    def create_subscription_changes(self, subscriptions=[] , times=1):
        """
        HubSpot API https://legacydocs.hubspot.com/docs/methods/email/update_status
        This will update email_events as well.
        TODO For updating sub_changes, utilize sub_id as an arg and make a passthrough method
        """
        # by default, a new subscription change will be created from a previous subscription change from one week ago as defined in the get
        if subscriptions == []:
            subscriptions = self.get_subscription_changes()
        subscription_id_list = [[change.get('subscriptionId') for change in subscription['changes']] for subscription in subscriptions]

        count = 0
        records = []
        print(f"creating {times} records")

        for item in subscription_id_list:
            if count < times:
                #if item[0]
                record_uuid = str(uuid.uuid4()).replace('-', '')
                url = f"{BASE_URL}/email/public/v1/subscriptions/{{}}".format(record_uuid+"@stitchdata.com")
                data = {
                    "subscriptionStatuses": [
                        {
                            "id": item[0], #a_sub_id,
                            "subscribed": True,
                            "optState": "OPT_IN",
                            "legalBasis": "PERFORMANCE_OF_CONTRACT",
                            "legalBasisExplanation": "We need to send them these emails as part of our agreement with them."
                        }
                    ]
                }
                # generate a record
                response = self.put(url, data)
                records.append([response])
                count += 1
        return records

    def create_workflows(self):
        """
        HubSpot API https://legacydocs.hubspot.com/docs/methods/workflows/v3/create_workflow
        """
        record_uuid = str(uuid.uuid4()).replace('-', '')

        url = f"{BASE_URL}/automation/v3/workflows"
        data = {
            "name": "Test Workflow",
            "type": "DRIP_DELAY",
            "onlyEnrollsManually": True,
            "actions": [
                {
                    "type": "DELAY",
                    "delayMillis": 3600000
                },
                {
                    "newValue": "HubSpot",
                    "propertyName": "company",
                    "type": "SET_CONTACT_PROPERTY"
                },
                {
                    "type": "WEBHOOK",
                    "url": "https://www.myintegration.com/webhook.php",
                    "method": "POST",
                    "authCreds": {
                        "user": "user",
                        "password": "password"
                    }
                }
            ]
        }

        # generate a record
        response = self.post(url, data)
        records = [response]
        return records

    ##########################################################################
    ### Updates
    ##########################################################################

    def updated_subscription_changes(self, subscription_id):
        return self.create_subscription_changes(subscription_id)

    ##########################################################################
    ### OAUTH
    ##########################################################################

    def acquire_access_token_from_refresh_token(self):
        # TODO does this limit test parallelization to n=1??
        # TODO just import this from the tap to lessen the maintenance burden
        payload = {
            "grant_type": "refresh_token",
            "redirect_uri": self.CONFIG['redirect_uri'],
            "refresh_token": self.CONFIG['refresh_token'],
            "client_id": self.CONFIG['client_id'],
            "client_secret": self.CONFIG['client_secret'],
        }

        response = requests.post(BASE_URL + "/oauth/v1/token", data=payload)
        response.raise_for_status()
        auth = response.json()
        self.CONFIG['access_token'] = auth['access_token']
        self.CONFIG['refresh_token'] = auth['refresh_token']
        self.CONFIG['token_expires'] = (
            datetime.datetime.utcnow() +
            datetime.timedelta(seconds=auth['expires_in'] - 600))
        print(f"TEST CLIENT | Token refreshed. Expires at {self.CONFIG['token_expires']}")

    def __init__(self, start_date=''):
        self.BaseTest = HubspotBaseTest()
        self.replication_keys = self.BaseTest.expected_replication_keys()
        self.CONFIG = self.BaseTest.get_credentials()
        self.CONFIG.update(self.BaseTest.get_properties())

        self.start_date_strf = start_date if start_date else self.CONFIG['start_date']
        self.start_date = datetime.datetime.strptime(
                self.start_date_strf, self.BaseTest.START_DATE_FORMAT
        ).timestamp() * 1000

        self.acquire_access_token_from_refresh_token()

        self.HEADERS = {'Authorization': f"Bearer {self.CONFIG['access_token']}"}
