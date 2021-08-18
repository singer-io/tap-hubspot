import datetime
import requests

from base import HubspotBaseTest

#url = https://api.hubapi.com/email/public/v1/campaigns/13054799?hapikey=demo
BASE_URL = "https://api.hubapi.com"
class TestClient():

    def get_campaigns(self):
        """Get all campaigns by id, then grab the details of each campaign"""
        campaign_by_id_url = "https://api.hubapi.com/email/public/v1/campaigns/by-id"
        campaign_url = "https://api.hubapi.com/email/public/v1/campaigns/"

        headers = {'Authorization': f"Bearer {self.CONFIG['access_token']}"}

        resp = requests.get(campaign_by_id_url, headers=headers)
        resp.raise_for_status()
        json_resp = resp.json()

        campaign_ids = [campaign['id'] for campaign in json_resp['campaigns']]

        campaigns = []
        for campaign_id in campaign_ids:
            url = f"{campaign_url}{campaign_id}"
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            json_resp = resp.json()
            campaigns.append(json_resp)

        return campaigns

    def acquire_access_token_from_refresh_token(self):
        # TODO just import this from the tap to lessen the maintenance burden
        payload = {
            "grant_type": "refresh_token",
            "redirect_uri": self.CONFIG['redirect_uri'],
            "refresh_token": self.CONFIG['refresh_token'],
            "client_id": self.CONFIG['client_id'],
            "client_secret": self.CONFIG['client_secret'],
        }

        resp = requests.post(BASE_URL + "/oauth/v1/token", data=payload)
        resp.raise_for_status()
        auth = resp.json()
        self.CONFIG['access_token'] = auth['access_token']
        self.CONFIG['refresh_token'] = auth['refresh_token']
        self.CONFIG['token_expires'] = (
            datetime.datetime.utcnow() +
            datetime.timedelta(seconds=auth['expires_in'] - 600))
        print(f"Token refreshed. Expires at {self.CONFIG['token_expires']}")

    def __init__(self):
        self.BaseTest = HubspotBaseTest()

        self.CONFIG = self.BaseTest.get_credentials()
        self.CONFIG.update(self.BaseTest.get_properties())

        self.acquire_access_token_from_refresh_token()
