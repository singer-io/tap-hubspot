import datetime
import requests
import backoff

from  tap_tester import menagerie
from base import HubspotBaseTest


BASE_URL = "https://api.hubapi.com"


class TestClient():

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
    def post(self, url, data, params=dict()):
        """Perfroma a POST using the standard requests method and log the action"""
        headers = self.HEADERS
        headers['content-type'] = "application/json"

        response = requests.post(url, json=data, params=params, headers=self.HEADERS)
        print(f"TEST CLIENT | POST {url} data={data} params={params}  STATUS: {response.status_code}")
        response.raise_for_status()
        json_response = response.json()

        return json_response

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
        url = f"{BASE_URL}/companies/v2/companies/recent/modified"
        since = str(datetime.datetime.strptime(since, "%Y-%m-%dT00:00:00.000000Z").timestamp() * 1000)[:-2]
        params = {'since': since}
        records = []

        # paginating through all the companies
        companies = []
        has_more = True
        while has_more:

            response = self.get(url, params=params)
            companies.extend(response['results'])

            has_more = response['hasMore']
            params['offset'] = response['offset']

        # get the details of each company
        for company in companies:
            url = f"{BASE_URL}/companies/v2/companies/{company['companyId']}"
            response = self.get(url)
            records.append(response)

        records = self.denest_properties('companies', records)

        return records

    def get_contact_lists(self):
        """
        Get all contact_lists by paginating using 'has-more' and 'offset'.
        """
        url = f"{BASE_URL}/contacts/v1/lists"
        params = dict()
        records = []
        replication_key = list(self.replication_keys['contact_lists'])[0]
        # paginating through all the contact_lists
        has_more = True
        while has_more:

            response = self.get(url, params=params)
            for record in response['lists']:
                if self.start_date < record[replication_key]:
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
            vids = [record['vid'] for record in response_1['contacts']]

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
        url = f"{BASE_URL}/companies/v2/companies/<company_id>/vids"
        params = dict()
        records = []

        for parent_id in parent_ids:
            child_url = url.replace('<company_id>', str(parent_id))
            response = self.get(child_url, params=params)

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

        v1_ids = [{'id': str(record['dealId'])} for record in records]

        # hit the v3 endpoint to get the special hs_<whatever> fields from v3 'properties'
        v3_url = f"{BASE_URL}/crm/v3/objects/deals/batch/read"
        v3_property = {'hs_date_entered_appointmentscheduled'}
        data = {'inputs': v1_ids,
                'properties': list(v3_property)}
        v3_response = self.post(v3_url, data)
        v3_records = v3_response['results']

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
                    result_dict = result
                    result_dict['engagement'] = result['engagement']
                    result_dict['engagement_id'] = result['engagement']['id']
                    result_dict['lastUpdated'] = result['engagement']['lastUpdated']

                    records.append(result_dict)

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
        records = []

        response = self.get(url)
        records.extend(response)

        return records

    def get_subscription_changes(self):
        """
        Get all subscription_changes by paginating using 'hasMore' and 'offset'.
        """
        url = f"{BASE_URL}/email/public/v1/subscriptions/timeline"
        params = dict()
        records = []

        has_more = True
        while has_more:

            response = self.get(url, params=params)
            records.extend(response['timeline'])

            has_more = response['hasMore']
            params['offset'] = response['offset']


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

    def __init__(self):
        self.BaseTest = HubspotBaseTest()
        self.replication_keys = self.BaseTest.expected_replication_keys()

        self.CONFIG = self.BaseTest.get_credentials()
        self.CONFIG.update(self.BaseTest.get_properties())

        self.start_date = datetime.datetime.strptime(
            self.CONFIG['start_date'], self.BaseTest.START_DATE_FORMAT).timestamp() * 1000

        self.acquire_access_token_from_refresh_token()

        self.HEADERS = {'Authorization': f"Bearer {self.CONFIG['access_token']}"}
