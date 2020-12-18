import sys
import requests
from ratelimit import limits
import ratelimit
import singer
import backoff
from datetime import datetime, timezone
from typing import Dict, List, Optional, DefaultDict, Set
from dateutil import parser
import urllib


class RetryAfterReauth(Exception):
    pass


LOGGER = singer.get_logger()
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
MANDATORY_PROPERTIES = {
    "companies": [
        "name",
        "country",
        "domain",
        "website",
        "numberofemployees",
        "industry",
        "hs_user_ids_of_all_owners",
        "owneremail",
        "ownername",
        "hubspot_owner_id",
        "hs_all_owner_ids",
        "industrynaics",
        "industrysic",
        "what_industry_company_",
        "industry",
        "number_of_employees_company",
        "numberofemployees",
        "employeesinalllocations",
        "employeesinalllocationsnum",
        "annualrevenue",
        "currency",
        "salesannual",
        "salesannualnum",
        "total_revenue",
        "type",
        "hs_merged_object_ids",
        "lifecyclestage",
    ],
    "contacts": [
        "email",
        "emailadresse",
        "hs_email_domain",
        "domain",
        "utm_campaign_original",
        "utm_medium_original",
        "utm_source_original",
        "utm_term_original",
        "hs_analytics_source",
        "hs_analytics_source_data_1",
        "hs_analytics_source_data_2",
        "hs_analytics_first_referrer",
        "hs_analytics_first_url",
        "hs_analytics_last_url",
        "hs_analytics_num_page_views",
        "hs_analytics_num_visits",
        "hs_analytics_num_event_completions",
        "hs_analytics_first_touch_converting_campaign",
        "hs_analytics_last_touch_converting_campaign",
        "associatedcompanyid",
        "hs_analytics_last_timestamp",
        "recent_conversion_date",
        "hs_calculated_form_submissions",
        "hs_all_contact_vids",
        "hs_facebook_click_id",
        "hs_google_click_id",
        "jobtitle",
        "firstname",
        "lastname",
        "date_of_birth",
        "first_conversion_date",
        "first_conversion_event_name",
        "form_submission_url",
        "numemployees",
        "employees_all_sites_",
        "jobseniority",
        "seniority",
        "hs_buying_role",
        "hs_calculated_merged_vids",
        "hs_merged_object_ids",
        "job_function",
        "hs_persona",
        "salutation",
        "website_source",
        "hs_lifecyclestage_customer_date",
        "hs_lifecyclestage_lead_date",
        "hs_lifecyclestage_marketingqualifiedlead_date",
        "hs_lifecyclestage_salesqualifiedlead_date",
        "hs_lifecyclestage_subscriber_date",
        "hs_lifecyclestage_evangelist_date",
        "hs_lifecyclestage_opportunity_date",
        "hs_lifecyclestage_other_date",
    ],
    "deals": [
        "amount_in_home_currency",
        "closedate",
        "deal_currency_code",
        "dealname",
        "dealstage",
        "dealtype",
        "hs_is_closed",
        "pipeline",
        "hs_merged_object_ids",
        "hs_analytics_source",
        "hs_analytics_source_data_1",
        "hs_analytics_source_data_2",
        "hs_campaign",
        "engagements_last_meeting_booked_campaign",
        "engagements_last_meeting_booked_medium",
        "engagements_last_meeting_booked_source",
        "closed_lost_reason",
        "closed_won_reason",
        "lifecyclestage",
    ],
}


class Hubspot:
    BASE_URL = "https://api.hubapi.com"

    def __init__(
        self,
        config: Dict,
        tap_stream_id: str,
        event_state: DefaultDict[Set, str],
        limit=250,
        timeout=10,  # seconds before first byte should have been received
    ):
        self.SESSION = requests.Session()
        self.limit = limit
        self.access_token = None
        self.config = config
        self.tap_stream_id = tap_stream_id
        self.event_state = event_state
        self.timeout = timeout

    def streams(
        self,
        start_date: datetime,
        end_date: datetime,
    ):
        self.refresh_access_token()
        if self.tap_stream_id == "owners":
            yield from self.get_owners()
        elif self.tap_stream_id == "companies":
            yield from self.get_companies()
        elif self.tap_stream_id == "contacts":
            yield from self.get_contacts(start_date=start_date, end_date=end_date)
        elif self.tap_stream_id == "engagements":
            yield from self.get_engagements()
        elif self.tap_stream_id == "deal_pipelines":
            yield from self.get_deal_pipelines()
        elif self.tap_stream_id == "deals":
            yield from self.get_deals()
        elif self.tap_stream_id == "email_events":
            yield from self.get_email_events(start_date=start_date, end_date=end_date)
        elif self.tap_stream_id == "forms":
            yield from self.get_forms()
        elif self.tap_stream_id == "submissions":
            yield from self.get_submissions()
        elif self.tap_stream_id == "contacts_events":
            yield from self.get_contacts_events()
        else:
            raise NotImplementedError(f"unknown stream_id: {self.tap_stream_id}")

    def get_owners(self):
        path = "/crm/v3/owners"
        data_field = "results"
        replication_path = ["updatedAt"]
        params = {"limit": 100}
        offset_key = "after"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_companies(self):
        path = "/crm/v3/objects/companies"
        data_field = "results"
        replication_path = ["updatedAt"]
        params = {"limit": 100, "properties": MANDATORY_PROPERTIES["companies"]}
        offset_key = "after"
        yield from self.get_records(
            path,
            replication_path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_contacts(self, start_date: datetime, end_date: datetime):
        self.event_state["contacts_start_date"] = start_date
        self.event_state["contacts_end_date"] = end_date
        path = "/crm/v3/objects/contacts"
        data_field = "results"
        offset_key = "after"
        replication_path = ["updatedAt"]
        params = {"limit": 100, "properties": MANDATORY_PROPERTIES["contacts"]}
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
        path = "/crm/v3/pipelines/deals"
        data_field = "results"
        offset_key = "after"
        replication_path = ["updatedAt"]
        yield from self.get_records(
            path,
            replication_path,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_deals(self):
        path = "/crm/v3/objects/deals"
        data_field = "results"
        params = {
            "limit": 100,
            "associations": "company",
            "properties": MANDATORY_PROPERTIES["deals"],
        }
        offset_key = "after"
        yield from self.get_records(
            path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        )

    def get_email_events(
        self,
        start_date: datetime,
        end_date: datetime,
    ):
        start_date: int = self.datetime_to_milliseconds(start_date)
        end_date: int = self.datetime_to_milliseconds(end_date)
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

    def get_guids_from_endpoint(self) -> set:
        forms = set()
        forms_from_endpoint = self.get_forms()
        if not forms_from_endpoint:
            return forms
        for form, _ in forms_from_endpoint:
            guid = form["guid"]
            forms.add(guid)
        return forms

    def get_submissions(self):
        # submission data is retrieved according to guid from forms
        # and hs_calculated_form_submissions field in contacts endpoint
        data_field = "results"
        offset_key = "after"
        params = {"limit": 50}  # maxmimum limit is 50
        guids_from_contacts = self.event_state["hs_calculated_form_submissions_guids"]
        guids_from_endpoint = self.get_guids_from_endpoint()

        def merge_guids():
            for guid in guids_from_contacts:
                guids_from_endpoint.discard(
                    guid
                )  # does not raise keyerror if not exists
                yield guid
            for guid in guids_from_endpoint:
                yield guid

        for guid in merge_guids():
            path = f"/form-integrations/v1/submissions/forms/{guid}"
            try:
                # some of the guids don't work
                self.test_endpoint(path)
            except:
                continue
            yield from self.get_records(
                path,
                params=params,
                data_field=data_field,
                offset_key=offset_key,
            )

    def is_enterprise(self):
        path = "/events/v3/events"
        try:
            self.test_endpoint(url=path)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 403:
                LOGGER.info(
                    "The company's account does not belong to Marketing Hub Enterprise. No event data can be retrieved"
                )
                return False
        return True

    def get_contacts_events(self):
        # contacts_events data is retrieved according to contact id
        start_date: str = self.event_state["contacts_start_date"].strftime(DATE_FORMAT)
        end_date: str = self.event_state["contacts_end_date"].strftime(DATE_FORMAT)
        data_field = "results"
        offset_key = "after"
        path = "/events/v3/events"
        if not self.is_enterprise():
            return None, None
        for contact_id in self.event_state["contacts_events_ids"]:

            params = {
                "limit": self.limit,
                "objectType": "contact",
                "objectId": contact_id,
                "occurredBefore": end_date,
                "occurredAfter": start_date,
            }
            yield from self.get_records(
                path,
                params=params,
                data_field=data_field,
                offset_key=offset_key,
            )

    def check_contact_id(
        self,
        record: Dict,
        visited_page_date: Optional[str],
        submitted_form_date: Optional[str],
    ):
        contacts_start_date = self.event_state["contacts_start_date"]
        contacts_end_date = self.event_state["contacts_end_date"]
        contact_id = record["id"]
        if visited_page_date:
            visited_page_date: datetime = parser.isoparse(visited_page_date)
            if (
                visited_page_date > contacts_start_date
                and visited_page_date <= contacts_end_date
            ):
                return contact_id
        if submitted_form_date:
            submitted_form_date: datetime = parser.isoparse(submitted_form_date)
            if (
                submitted_form_date > contacts_start_date
                and submitted_form_date <= contacts_end_date
            ):
                return contact_id
        return None

    def store_ids_submissions(self, record: Dict):

        # get form guids from contacts to sync submissions data
        hs_calculated_form_submissions = self.get_value(
            record, ["properties", "hs_calculated_form_submissions"]
        )
        if hs_calculated_form_submissions:
            # contacts_events_ids is a persistent dictionary (shelve) backed by a file
            # we use it to deduplicate and later to iterate
            # we dont care about the value only the key
            forms_times = hs_calculated_form_submissions.split(";")
            for form_time in forms_times:
                guid = form_time.split(":", 1)[0]
                self.event_state["hs_calculated_form_submissions_guids"][guid] = None
                self.event_state["hs_calculated_form_submissions_guids"].sync()

        # get contacts ids to sync events_contacts data
        # check if certain contact_id needs to be synced according to hs_analytics_last_timestamp and recent_conversion_date fields in contact record
        visited_page_date: Optional[str] = self.get_value(
            record, ["properties", "hs_analytics_last_timestamp"]
        )
        submitted_form_date: Optional[str] = self.get_value(
            record, ["properties", "recent_conversion_date"]
        )
        contact_id = self.check_contact_id(
            record=record,
            visited_page_date=visited_page_date,
            submitted_form_date=submitted_form_date,
        )
        if contact_id:
            # contacts_events_ids is a persistent dictionary (shelve) backed by a file
            # we use it to deduplicate and later to iterate
            # we dont care about the value only the key
            self.event_state["contacts_events_ids"][contact_id] = None
            self.event_state["contacts_events_ids"].sync()

    def get_records(
        self, path, replication_path=None, params=None, data_field=None, offset_key=None
    ):
        for record in self.paginate(
            path,
            params=params,
            data_field=data_field,
            offset_key=offset_key,
        ):
            if self.tap_stream_id in [
                "owners",
                "contacts",
                "companies",
                "deal_pipelines",
            ]:
                replication_value = parser.isoparse(
                    self.get_value(record, replication_path)
                )
                self.store_ids_submissions(record)

            else:
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
        return datetime.fromtimestamp((int(ms) / 1000), timezone.utc) if ms else None

    def datetime_to_milliseconds(self, d: datetime):
        return int(d.timestamp() * 1000) if d else None

    def paginate(
        self, path: str, params: Dict = None, data_field: str = None, offset_key=None
    ):
        params = params or {}
        offset_value = None
        while True:
            if offset_value:
                params[offset_key] = offset_value

            data = self.call_api(path, params=params)
            params[offset_key] = None

            if not data_field:
                # non paginated list
                yield from data
                return
            else:
                d = data.get(data_field, [])
                if not d:
                    return
                yield from d

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
            requests.exceptions.ReadTimeout,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
            ratelimit.exception.RateLimitException,
            RetryAfterReauth,
        ),
        max_tries=10,
    )
    @limits(calls=100, period=10)
    def call_api(self, url, params=None):
        params = params or {}
        url = f"{self.BASE_URL}{url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        with self.SESSION.get(
            url, headers=headers, params=params, timeout=self.timeout
        ) as response:
            if response.status_code == 401:
                # attempt to refresh access token
                self.refresh_access_token()
                raise RetryAfterReauth
            LOGGER.debug(response.url)
            response.raise_for_status()
            return response.json()

    def test_endpoint(self, url, params={}):
        url = f"{self.BASE_URL}{url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        with self.SESSION.get(
            url, headers=headers, params=params, timeout=self.timeout
        ) as response:
            response.raise_for_status()

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
