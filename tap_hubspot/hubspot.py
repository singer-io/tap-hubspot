import requests
from dateutil import parser
import time
from ratelimit import limits
import ratelimit
import singer
import backoff
import sys
import datetime
from singer import utils

LOGGER = singer.get_logger()


class Hubspot:
    BASE_URL = "https://api.hubapi.com"
    ENDPOINTS = {
        "companies": "/companies/v2/companies/paged",
        "contacts": "/contacts/v1/lists/all/contacts/all",
        "deal_pipelines": "/crm-pipelines/v1/pipelines/deals",
        "deals": "/deals/v1/deal/paged",
        "email_events": "/email/public/v1/events",
        "engagements": "/engagements/v1/engagements/paged",
        "forms": "/forms/v2/forms",
    }
    DATA_PATH = {
        "companies": "companies",
        "contacts": "contacts",
        "deal_pipelines": "results",
        "deals": "deals",
        "email_events": "events",
        "engagements": "results",
    }
    REPLICATION_PATH = {
        "companies": ["properties", "hs_lastmodifieddate", "timestamp",],
        "contacts": ["properties", "lastmodifieddate", "value"],
        "deal_pipelines": ["updatedAt"],
        "deals": ["properties", "hs_lastmodifieddate", "timestamp"],
        "email_events": ["created"],
        "engagements": ["engagement", "lastUpdated"],
        "forms": ["updatedAt"],
    }

    def __init__(self, config, tap_stream_id, properties, limit=250):
        self.SESSION = requests.Session()
        self.limit = limit
        self.access_token = None
        self.tap_stream_id = tap_stream_id
        self.config = config
        self.refresh_access_token()
        self.endpoint = self.ENDPOINTS[tap_stream_id]
        self.offset_value = None
        self.offset_key = None
        self.hasmore = True
        self.PARAMS = {
            "companies": {"limit": self.limit, "properties": properties,},
            "contacts": {
                "showListMemberships": True,
                "includeVersion": True,
                "count": self.limit,
            },
            "engagements": {"limit": self.limit},
            "deals": {
                "count": self.limit,
                "includeAssociations": False,
                "properties": properties,
                "limit": self.limit,
            },
        }

    def get_url_params(self, start_date, end_date):
        url = f"{self.BASE_URL}{self.endpoint}"
        params = self.PARAMS.get(self.tap_stream_id, {})
        if self.tap_stream_id == "email_events":
            params = {"startTimestamp": start_date, "endTimestamp": end_date}
        if self.offset_value:
            params[self.offset_key] = self.offset_value
        return url, params

    def get_replication_value(
        self, obj: dict, path_to_replication_key=None, default=None
    ):
        if not path_to_replication_key:
            return default
        for path_element in path_to_replication_key:
            obj = obj.get(path_element)
            if not obj:
                return default
        return self.milliseconds_to_datetime(obj)

    def milliseconds_to_datetime(self, ms: str):
        return (
            datetime.datetime.fromtimestamp((int(ms) / 1000), datetime.timezone.utc)
            if ms
            else None
        )

    def datetime_to_milliseconds(self, d: datetime.datetime):
        return int(d.timestamp() * 1000) if d else None

    def get_records(self, start_date, end_date):
        while self.hasmore:
            url, params = self.get_url_params(start_date, end_date)
            records = self.call_api(url, params=params)
            if records:
                replication_value = map(
                    lambda record: self.get_replication_value(
                        obj=record,
                        path_to_replication_key=self.REPLICATION_PATH.get(
                            self.tap_stream_id
                        ),
                    ),
                    records,
                )
                yield from zip(records, replication_value)
            else:
                break

    def streams(self, start_date, end_date):
        start_date = self.datetime_to_milliseconds(start_date)
        end_date = self.datetime_to_milliseconds(end_date)
        yield from self.get_records(start_date, end_date)

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
        ),
    )
    @limits(calls=100, period=10)
    def call_api(self, url, params={}):
        response = self.SESSION.get(
            url, headers={"Authorization": f"Bearer {self.access_token}"}, params=params
        )
        LOGGER.info(response.url)
        response.raise_for_status()
        data = self.get_offset(response.json())

        return data

    def get_offset(self, data):
        data_path = self.DATA_PATH.get(self.tap_stream_id)
        if isinstance(data, list):
            self.hasmore = False
            return data

        if self.tap_stream_id == "deal_pipelines":
            self.hasmore = False

        offset = [k for k in data.keys() if k.endswith("offset")]
        if offset:
            offset = offset[0]
            self.offset_value = data.get(offset)
            self.offset_key = "vidOffset" if offset == "vid-offset" else "offset"
        data = data[data_path] if data_path else data

        return data

    def refresh_access_token(self):
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }

        resp = requests.post(self.BASE_URL + "/oauth/v1/token", data=payload)
        resp.raise_for_status()
        if not resp:
            raise Exception(resp.text)
        self.access_token = resp.json()["access_token"]
