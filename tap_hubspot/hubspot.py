import requests
from ratelimit import limits
import ratelimit
import singer
import backoff
import datetime
from typing import Dict
from tap_hubspot.util import record_nodash

LOGGER = singer.get_logger()


class Hubspot:
    BASE_URL = "https://api.hubapi.com"

    def __init__(self, config, limit=250):
        self.SESSION = requests.Session()
        self.limit = limit
        self.access_token = None
        self.config = config
        self.refresh_access_token()

    def streams(self, tap_stream_id, start_date, end_date, properties):
        if tap_stream_id == "companies":
            yield from self.get_companies(properties)
        elif tap_stream_id == "contacts":
            yield from self.get_contacts()
        elif tap_stream_id == "engagements":
            yield from self.get_engagements()
        elif tap_stream_id == "deal_pipelines":
            yield from self.get_deal_pipelines()
        elif tap_stream_id == "deals":
            yield from self.get_deals(properties)
        elif tap_stream_id == "email_events":
            start_date = self.datetime_to_milliseconds(start_date)
            end_date = self.datetime_to_milliseconds(end_date)
            yield from self.get_email_events(start_date, end_date)
        elif tap_stream_id == "forms":
            yield from self.get_forms()
        elif tap_stream_id == "submissions":
            yield from self.get_submissions()
        else:
            raise NotImplementedError(f"unknown stream_id: {tap_stream_id}")

    def get_companies(self, properties):
        path = "/companies/v2/companies/paged"
        data_field = "companies"
        replication_path = ["properties", "hs_lastmodifieddate", "timestamp"]
        params = {
            "limit": self.limit,
            "properties": properties,
        }
        offset_key = "offset"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_contacts(self):
        path = "/contacts/v1/lists/all/contacts/all"
        data_field = "contacts"
        replication_path = ["properties", "lastmodifieddate", "value"]
        params = {
            "showListMemberships": True,
            "includeVersion": True,
            "count": self.limit,
        }
        offset_key = "vid-offset"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_engagements(self):
        path = "/engagements/v1/engagements/paged"
        data_field = "results"
        replication_path = ["engagement", "lastUpdated"]
        params = {"limit": self.limit}
        offset_key = "offset"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_deal_pipelines(self):
        path = "/crm-pipelines/v1/pipelines/deals"
        data_field = "results"
        replication_path = ["updatedAt"]
        yield from self.get_records(path, replication_path, data_field=data_field)

    def get_deals(self, properties):
        path = "/deals/v1/deal/paged"
        data_field = "deals"
        replication_path = ["properties", "hs_lastmodifieddate", "timestamp"]
        params = {
            "count": self.limit,
            "includeAssociations": True,
            "properties": properties,
            "limit": self.limit,
        }
        offset_key = "offset"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_email_events(self, start_date, end_date):
        path = "/email/public/v1/events"
        data_field = "events"
        replication_path = ["created"]
        params = {"startTimestamp": start_date, "endTimestamp": end_date}
        offset_key = "offset"

        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_forms(self):
        path = "/forms/v2/forms"
        replication_path = ["updatedAt"]
        yield from self.get_records(path, replication_path)

    def get_submissions(self):
        # submission data is retrieved according to guid from forms
        replication_path = ["submittedAt"]
        data_field = "results"
        offset_key = "after"
        params = {"limit": 50}  # maxmimum limit is 50
        forms = self.get_forms()
        for form, _ in forms:
            guid = form["guid"]
            path = f"/form-integrations/v1/submissions/forms/{guid}"
            yield from self.get_records(
                path,
                replication_path,
                params=params,
                data_field=data_field,
                offset_key=offset_key,
            )

    def get_records(
        self, path, replication_path, params={}, data_field=None, offset_key=None
    ):
        for record in self.paginate(
            path, params=params, data_field=data_field, offset_key=offset_key,
        ):
                record = record_nodash(record)
            replication_value = self.milliseconds_to_datetime(
                self.get_value(record, replication_path)
            )
            yield record, replication_value

    def get_value(self, obj: dict, path_to_replication_key=None, default=None):
        if not path_to_replication_key:
            return default
        for path_element in path_to_replication_key:
            obj = obj.get(path_element)
            if not obj:
                return default
        return obj

    def milliseconds_to_datetime(self, ms: str):
        return (
            datetime.datetime.fromtimestamp((int(ms) / 1000), datetime.timezone.utc)
            if ms
            else None
        )

    def datetime_to_milliseconds(self, d: datetime.datetime):
        return int(d.timestamp() * 1000) if d else None

    def paginate(
        self, path: str, params: Dict = None, data_field: str = None, offset_key=None
    ):
        offset_value = None
        while True:
            if offset_value:
                if offset_key == "vid-offset":
                    params["vidOffset"] = offset_value
                else:
                    params[offset_key] = offset_value

            data = self.call_api(path, params=params)

            if not data_field:
                # non paginated list
                yield from data
                return
            else:
                d = data.get(data_field, [])
                yield from d
                if not d:
                    return

            if offset_key:
                if "paging" in data:
                    offset_value = self.get_value(data, ["paging", "next", "after"])
                else:
                    offset_value = data.get(offset_key)
            if not offset_value:
                break

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
            f"{self.BASE_URL}{url}",
            headers={"Authorization": f"Bearer {self.access_token}"},
            params=params,
        )
        LOGGER.info(response.url)
        response.raise_for_status()
        return response.json()

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
