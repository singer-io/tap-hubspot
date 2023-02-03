import requests
from ratelimit import limits
import ratelimit
import singer
import backoff
from datetime import datetime, timezone, timedelta
from typing import Dict, Iterable, Optional, DefaultDict, Set, List, Any, Tuple, TypeVar

from dateutil import parser
import simplejson


class RetryAfterReauth(Exception):
    pass


class InvalidCredentials(Exception):
    pass


class MissingScope(Exception):
    pass


def giveup_http_codes(e: Exception):
    if not isinstance(e, requests.RequestException):
        return False

    if isinstance(e, requests.HTTPError):
        # raised by response.raise_for_status()
        status_code = e.response.status_code
        if status_code in {404, 400}:
            return True

    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        # retry on connection and timeout errors
        return False

    if isinstance(e, (ValueError, requests.URLRequired)):
        # catch invalid/missing requests due to schema, invalid url etc.
        return True

    # backoff on all remaining requests.RequestException subclasses
    return False


LOGGER = singer.get_logger()
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
MANDATORY_PROPERTIES = {
    "companies": [
        "hs_object_id",
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
        "hs_date_entered_salesqualifiedlead",  # trengo custom field
        "became_a_lead_date",  # trengo custom field
        "became_a_mql_date",  # trengo custom field
        "became_a_sql_date",  # trengo custom field
        "became_a_opportunity_date",  # trengo custom field
        "class",  # trengo custom field
        "recent_deal_amount",  # trengo
        "initial_deal_size",  # trengo
        "became_a_sal",  # trengo
        "became_a_customer_date",  # trengo
        "hs_additional_domains",
        "marketing_pipeline_value_in__",  # capmo
        "recent_conversion_date",  # capmo
        "recent_conversion_event_name",  # capmo
        "first_conversion_date",  # capmo
        "first_conversion_event_name",  # capmo
        "company__target_market__tiers_",  # capmo
        "plan_price_ex_tax_",  # ably_com
        "plan",  # ably_com
        "first_paid_invoice",  # ably_com
        "monthly_recurring_revenue",  # sleekflow_io
        "mrr",  # cloudtalk_io
        "mrr_software_only",  # cloudtalk_io
        "industry_ivalua_",  # ivalua
        "profitwell_created_on",  # logmycare_co_uk
        "profitwell_activated_on",  # logmycare_co_uk
        "hs_date_entered_marketingqualifiedlead",  # agencyanalytics_com
        "hs_date_entered_26057217",  # agencyanalytics_com
        "product_qualified_lead_date",  # agencyanalytics_com
        "hs_date_entered_salesqualifiedlead",  # agencyanalytics_com
        "sales_qualified_lead_date",  # agencyanalytics_com
        "hs_date_entered_opportunity",  # agencyanalytics_com
        "opportunity_date",  # agencyanalytics_com
        "hs_date_entered_customer",  # agencyanalytics_com
        "customer",  # agencyanalytics_com
        "plan_mrr",  # agencyanalytics_com
    ],
    "contacts": [
        "hs_object_id",
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
        "hs_additional_emails",
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
        "went_mql",
        "went_mql_date",
        "original_mql_date_before_reset",
        "converting_touch",
        "mql_date",
        "most_recent_source",  # ably_com
        "ably_id",  # ably_com
        "sign_up_date",  # ably_com
        "become_a_customer___phadmin",  # cloudtalk_io
        "customer_canceled_an_account___phadmin",  # cloudtalk_io
        "lead_source",  # cloudtalk_io
        "approved_trial",  # cloudtalk_io
        "mrr",  # cloudtalk_io,
        "new_seat_range_strategy_calculation",  # cloudtalk_io
        "calculated_users_range_conservative",  # cloudtalk_io
        "trial_login_date",  # cloudtalk_io
        "monthly_recurring_revenue_only_software",  # cloudtalk_io
        "qual_out_date_stamp",  # cloudtalk_io
        "unqual_date_stamp",  # cloudtalk_io
        "ft_1_2_seats_date_stamp",  # cloudtalk_io
        "ft_3_10_seats_date_stamp",  # cloudtalk_io
        "ft_11_20_seats_date_stamp",  # cloudtalk_io
        "ft_21_50",  # cloudtalk_io
        "ft_51_100_seats_date_stamp",  # cloudtalk_io
        "ft_101_250_seats_date_stamp",  # cloudtalk_io
        "ft_251_more_seats_date_stamp",  # cloudtalk_io
        "segment__contact_",  # cloudtalk_io
        "closedate",  # getmagic_com
        "lifecyclestage",  # getmagic_com
        "contact_type",  # getmagic_com
        "discovery_call_attended",  # getmagic_com
        "stage",  # getmagic_com
        "dw_client_total_billed_revenue",  # getmagic_com
        "offline_source_drill_down_2",  # qmarkets_net,
        "hs_date_entered_marketingqualifiedlead",  # vestd_com
        "became_a_eupry_lead_date",  # eupry
        "became_a_eupry_qualified_lead_date",  # eupry
        "became_a_eupry_sales_qualified_lead_date",  # eupry
        "lead_acquisition_group",  # coverflex
        "lead_acquisition_group_date"  # coverflex
        "first_conversion_event_name",  # sylvera_io
        "first_conversion_date",  # sylvera_io,
        "freetrial_createdate",
        "freetrial_approveddate",
    ],
}

T = TypeVar("T")


def chunker(iter: Iterable[T], size: int) -> Iterable[List[T]]:
    i = 0
    chunk = []
    for o in iter:
        chunk.append(o)
        i += 1
        if i != 0 and i % size == 0:
            yield chunk
            chunk = []
    yield chunk


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
        self.access_token_ttl = None
        self.config = config
        self.tap_stream_id = tap_stream_id
        self.event_state = event_state
        self.timeout = timeout

    def streams(self, start_date: datetime, end_date: datetime):
        if self.tap_stream_id == "owners":
            yield from self.get_owners()
        elif self.tap_stream_id == "companies":
            yield from self.get_companies(start_date, end_date)
        elif self.tap_stream_id == "contacts":
            self.event_state["contacts_start_date"] = start_date
            self.event_state["contacts_end_date"] = end_date
            yield from self.get_contacts_v2(start_date, end_date)
        elif self.tap_stream_id == "contact_lists":
            yield from self.get_contact_lists()
        elif self.tap_stream_id == "contacts_in_contact_lists":
            yield from self.get_contacts_in_contact_lists()
        elif self.tap_stream_id == "deal_pipelines":
            yield from self.get_deal_pipelines()
        elif self.tap_stream_id == "deals":
            yield from self.get_deals(start_date, end_date)
        elif self.tap_stream_id == "email_events":
            yield from self.get_email_events(start_date=start_date, end_date=end_date)
        elif self.tap_stream_id == "forms":
            yield from self.get_forms()
        elif self.tap_stream_id == "submissions":
            yield from self.get_submissions()
        elif self.tap_stream_id == "contacts_events":
            yield from self.get_contacts_events()
        elif self.tap_stream_id == "deal_properties":
            yield from self.get_properties("deals")
        elif self.tap_stream_id == "contact_properties":
            yield from self.get_properties("contacts")
        elif self.tap_stream_id == "company_properties":
            yield from self.get_properties("companies")
        elif self.tap_stream_id == "archived_contacts":
            yield from self.get_archived_contacts()
        elif self.tap_stream_id == "archived_companies":
            yield from self.get_archived_companies()
        elif self.tap_stream_id == "archived_deals":
            yield from self.get_archived_deals()
        elif self.tap_stream_id == "calls":
            yield from self.get_calls(start_date=start_date, end_date=end_date)
        elif self.tap_stream_id == "notes":
            yield from self.get_notes(start_date=start_date, end_date=end_date)
        elif self.tap_stream_id == "meetings":
            yield from self.get_meetings(start_date=start_date, end_date=end_date)
        elif self.tap_stream_id == "tasks":
            yield from self.get_tasks(start_date=start_date, end_date=end_date)
        elif self.tap_stream_id == "emails":
            yield from self.get_engagement_emails(
                start_date=start_date, end_date=end_date
            )
        else:
            raise NotImplementedError(f"unknown stream_id: {self.tap_stream_id}")

    def get_deals(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[Tuple[Dict, datetime]]:
        filter_key = "hs_lastmodifieddate"
        obj_type = "deals"
        primary_key = "hs_object_id"

        properties = self.get_object_properties(obj_type)

        gen = self.search(
            obj_type, filter_key, start_date, end_date, properties, primary_key
        )

        for chunk in chunker(gen, 50):
            ids: List[str] = [deal["id"] for deal in chunk]

            contacts_associations = self.get_associations(obj_type, "contacts", ids)
            companies_associations = self.get_associations(obj_type, "companies", ids)
            property_history = self.get_property_history("deals", ["dealstage"], ids)

            for i, deal_id in enumerate(ids):
                deal = chunk[i]

                contacts = contacts_associations.get(deal_id, [])
                companies = companies_associations.get(deal_id, [])

                deal["associations"] = {
                    "contacts": {"results": contacts},
                    "companies": {"results": companies},
                }

                deal["propertiesWithHistory"] = property_history.get(deal_id, {})

                yield deal, parser.isoparse(
                    self.get_value(deal, ["properties", filter_key])
                )

    def get_object_properties(self, obj_type: str) -> List[str]:
        resp = self.do("GET", f"/crm/v3/properties/{obj_type}")
        data = resp.json()
        return [o["name"] for o in data["results"]]

    def get_property_history(
        self, obj_type: str, properties: List[str], ids: List[str]
    ) -> Dict[str, Dict[str, List[Dict]]]:
        body = {
            "properties": properties,
            "propertiesWithHistory": properties,
            "inputs": [{"id": id} for id in ids],
        }
        path = f"/crm/v3/objects/{obj_type}/batch/read"
        resp = self.do("POST", path, json=body)

        data = resp.json()

        history = data.get("results", [])

        result: Dict[str, Dict[str, List[Dict]]] = {}
        for entry in history:
            obj_id = entry["id"]
            result[obj_id] = entry["propertiesWithHistory"]

        return result

    def get_associations(
        self,
        from_obj: str,
        to_obj: str,
        ids: List[str],
    ) -> Dict[str, List[Any]]:
        body = {"inputs": [{"id": id} for id in ids]}
        path = f"/crm/v4/associations/{from_obj}/{to_obj}/batch/read"

        resp = self.do("POST", path, json=body)

        result: Dict[str, List[Any]] = {}
        for ass in resp.json().get("results", []):
            ass_id = ass["from"]["id"]
            result[ass_id] = ass["to"]

        return result

    def search(
        self,
        object_type: str,
        filter_key: str,
        start_date: datetime,
        end_date: datetime,
        properties: List[str],
        primary_key: str,
        limit=100,
    ) -> Iterable[Dict]:
        path = f"/crm/v3/objects/{object_type}/search"
        after: int = 0
        primary_key_value = "0"
        while True:
            try:
                body = self.build_search_body(
                    start_date,
                    end_date,
                    properties,
                    filter_key,
                    after,
                    primary_key,
                    primary_key_value,
                    limit=limit,
                )
                resp = self.do(
                    "POST",
                    path,
                    json=body,
                )
            except requests.HTTPError as err:
                if err.response.status_code == 520:
                    continue
                raise

            data = resp.json()
            LOGGER.info(f"total data to be synced: {data['total']}")
            records = data.get("results", [])

            if not records:
                return

            for record in records:
                yield record

            # pagination
            page_after: Optional[str] = (
                data.get("paging", {}).get("next", {}).get("after", None)
            )

            # when there are no more results for the query/after combination
            # the paging.next.after will not be present in the payload
            if page_after is None:
                return

            # all search-endpoints will fail with a 400 after 10,000 records returned
            # (not pages). We use the last record in the last page to filter on.
            if int(page_after) >= 10000:
                # reset all pagination values
                after = 0
                primary_key_value = self.get_value(
                    records[-1], ["properties", primary_key]
                )
                continue

            after = int(page_after)

    def build_search_body(
        self,
        start_date: datetime,
        end_date: datetime,
        properties: list,
        filter_key: str,
        after: int,
        primary_key: str,
        primary_key_value: str,
        limit: int = 100,
    ):
        q = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": filter_key,
                            "operator": "GTE",
                            "value": str(int(start_date.timestamp() * 1000)),
                        },
                        {
                            "propertyName": filter_key,
                            "operator": "LT",
                            "value": str(int(end_date.timestamp() * 1000)),
                        },
                        {
                            "propertyName": primary_key,
                            "operator": "GTE",
                            "value": primary_key_value,
                        },
                    ]
                }
            ],
            "properties": properties,
            "sorts": [
                {"propertyName": primary_key, "direction": "ASCENDING"},
            ],
            "limit": limit,
            "after": after,
        }
        LOGGER.info(f"filter option: {q['filterGroups']}. after:{q['after']}")
        return q

    def attach_engagement_associations(
        self, obj_type: str, search_result: Iterable[Dict], replication_path: List[str]
    ) -> Iterable[Tuple[Dict, datetime]]:

        for chunk in chunker(search_result, 100):
            ids: List[str] = [engagement["id"] for engagement in chunk]

            companies_associations = self.get_associations(obj_type, "companies", ids)
            contacts_associations = self.get_associations(obj_type, "contacts", ids)

            for i, engagement_id in enumerate(ids):
                engagement = chunk[i]

                companies = companies_associations.get(engagement_id, [])
                contacts = contacts_associations.get(engagement_id, [])

                engagement["associations"] = {
                    "companies": {"results": companies},
                    "contacts": {"results": contacts},
                }

                yield engagement, parser.isoparse(
                    self.get_value(engagement, replication_path)
                )

    def get_properties(self, object_type: str):
        path = f"/crm/v3/properties/{object_type}"
        data_field = "results"
        replication_path = ["updatedAt"]
        offset_key = "after"
        yield from self.get_records(
            path,
            replication_path,
            data_field=data_field,
            offset_key=offset_key,
        )

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

    def get_archived(self, object_type: str):
        path = f"/crm/v3/objects/{object_type}"
        data_field = "results"
        replication_path = ["archivedAt"]
        # "the properties we need are already there, but by adding a single property, we are preventing the api from returning too many default properties that we do not need"
        properties = ["hs_object_id"]
        offset_key = "after"
        params = {"limit": 100, "archived": True, "properties": properties}
        yield from self.get_records(
            path,
            replication_path,
            data_field=data_field,
            offset_key=offset_key,
            params=params,
        )

    def get_archived_contacts(self):
        object_type = "contacts"
        yield from self.get_archived(object_type=object_type)

    def get_archived_companies(self):
        object_type = "companies"
        yield from self.get_archived(object_type=object_type)

    def get_archived_deals(self):
        object_type = "deals"
        yield from self.get_archived(object_type=object_type)

    def get_companies_legacy(self):
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

    def get_companies(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[Tuple[Dict, datetime]]:
        filter_key = "hs_lastmodifieddate"
        obj_type = "companies"
        primary_key = "hs_object_id"

        properties = self.get_object_properties(obj_type)

        companies = self.search(
            obj_type, filter_key, start_date, end_date, properties, primary_key
        )

        for company in companies:
            yield company, parser.isoparse(
                self.get_value(company, ["properties", filter_key])
            )

    def get_contacts(self) -> Iterable[Tuple[Dict, datetime]]:
        replication_path = ["updatedAt"]

        gen = self.get_records(
            "/crm/v3/objects/contacts",
            replication_path,
            params={"limit": 100, "properties": MANDATORY_PROPERTIES["contacts"]},
            data_field="results",
            offset_key="after",
        )

        for chunk in chunker(gen, 100):
            ids: List[str] = [contact["id"] for contact, _ in chunk]
            companies_associations = self.get_associations("contacts", "companies", ids)

            for contact, replication_value in chunk:
                contact["associations"] = {
                    "companies": {
                        "results": companies_associations.get(contact["id"], [])
                    },
                }

                self.store_ids_submissions(contact)

                yield contact, replication_value

    def get_contacts_v2(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[Tuple[Dict, datetime]]:
        filter_key = "lastmodifieddate"
        obj_type = "contacts"
        primary_key = "hs_object_id"
        properties = self.get_object_properties(obj_type)

        gen = self.search(
            obj_type, filter_key, start_date, end_date, properties, primary_key
        )

        for chunk in chunker(gen, 100):
            ids: List[str] = [contact["id"] for contact in chunk]
            companies_associations = self.get_associations("contacts", "companies", ids)

            for contact in chunk:
                contact["associations"] = {
                    "companies": {
                        "results": companies_associations.get(contact["id"], [])
                    },
                }

                self.store_ids_submissions(contact)
                replication_value = parser.isoparse(
                    self.get_value(contact, ["properties", filter_key])
                )

                yield contact, replication_value

    def get_contact_lists(self) -> Iterable:
        try:
            self.test_endpoint("/contacts/v1/lists")
        except requests.HTTPError as e:
            # We assume the current token doesn't have the proper permissions
            LOGGER.warn("insufficient permissions to get contact lists, skipping")
            return []

        yield from self.get_records(
            "/contacts/v1/lists",
            replication_path=["metaData", "lastSizeChangeAt"],
            params={"count": 250},
            data_field="lists",
            offset_key="offset",
        )

    def get_contacts_in_contact_lists(self) -> Iterable:
        for contact_list, _ in self.get_contact_lists():
            list_id = contact_list["listId"]
            for contact, _ in self.get_records(
                f"/contacts/v1/lists/{list_id}/contacts/all",
                params={
                    "count": 500,
                    "formSubmissionMode": "none",
                },
                data_field="contacts",
                offset_key="vidOffset",
            ):
                contact["list_id"] = list_id
                yield contact, None

    def get_calls(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[Tuple[Dict, datetime]]:
        filter_key = "hs_lastmodifieddate"
        obj_type = "calls"
        properties = self.get_object_properties(obj_type)
        primary_key = "hs_object_id"

        gen = self.search(
            obj_type, filter_key, start_date, end_date, properties, primary_key
        )
        return self.attach_engagement_associations(
            obj_type=obj_type,
            search_result=gen,
            replication_path=["properties", filter_key],
        )

    def get_meetings(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[Tuple[Dict, datetime]]:
        filter_key = "hs_lastmodifieddate"
        obj_type = "meetings"
        properties = self.get_object_properties(obj_type)
        primary_key = "hs_object_id"
        gen = self.search(
            obj_type, filter_key, start_date, end_date, properties, primary_key
        )

        return self.attach_engagement_associations(
            obj_type=obj_type,
            search_result=gen,
            replication_path=["properties", filter_key],
        )

    def get_engagement_emails(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[Tuple[Dict, datetime]]:
        filter_key = "hs_lastmodifieddate"
        obj_type = "emails"
        properties = self.get_object_properties(obj_type)
        primary_key = "hs_object_id"

        gen = self.search(
            obj_type, filter_key, start_date, end_date, properties, primary_key
        )

        return self.attach_engagement_associations(
            obj_type=obj_type,
            search_result=gen,
            replication_path=["properties", filter_key],
        )

    def get_notes(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[Tuple[Dict, datetime]]:
        filter_key = "hs_lastmodifieddate"
        obj_type = "notes"
        properties = self.get_object_properties(obj_type)
        primary_key = "hs_object_id"

        gen = self.search(
            obj_type, filter_key, start_date, end_date, properties, primary_key
        )

        return self.attach_engagement_associations(
            obj_type=obj_type,
            search_result=gen,
            replication_path=["properties", filter_key],
        )

    def get_tasks(
        self, start_date: datetime, end_date: datetime
    ) -> Iterable[Tuple[Dict, datetime]]:
        filter_key = "hs_lastmodifieddate"
        obj_type = "tasks"
        properties = self.get_object_properties(obj_type)
        primary_key = "hs_object_id"
        gen = self.search(
            obj_type, filter_key, start_date, end_date, properties, primary_key
        )

        return self.attach_engagement_associations(
            obj_type=obj_type,
            search_result=gen,
            replication_path=["properties", filter_key],
        )

    def get_deal_pipelines(self):
        path = "/crm/v3/pipelines/deals"
        data_field = "results"
        offset_key = "after"
        replication_path = ["updatedAt"]
        yield from self.get_records(
            path, replication_path, data_field=data_field, offset_key=offset_key
        )

    def get_email_events(self, start_date: datetime, end_date: datetime):
        start_date: int = self.datetime_to_milliseconds(start_date)
        end_date: int = self.datetime_to_milliseconds(end_date)
        path = "/email/public/v1/events"
        data_field = "events"
        replication_path = ["created"]
        params = {"startTimestamp": start_date, "endTimestamp": end_date, "limit": 1000}
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
        replication_key = "submittedAt"
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
                replication_path=[replication_key],
                data_field=data_field,
                offset_key=offset_key,
                guid=guid,
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
                path, params=params, data_field=data_field, offset_key=offset_key
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
        self,
        path,
        replication_path=None,
        params=None,
        data_field=None,
        offset_key=None,
        guid=None,
    ):
        for record in self.paginate(
            path, params=params, data_field=data_field, offset_key=offset_key
        ):
            if self.tap_stream_id in [
                "owners",
                "contacts",
                "companies",
                "deal_pipelines",
                "deal_properties",
                "contact_properties",
                "company_properties",
                "archived_contacts",
                "archived_companies",
                "archived_deals",
            ]:

                replication_value = self.get_value(record, replication_path)
                if replication_value:
                    replication_value = parser.isoparse(replication_value)

            else:
                replication_value = self.milliseconds_to_datetime(
                    self.get_value(record, replication_path)
                )
            if self.tap_stream_id == "submissions":
                record["form_id"] = guid

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

            resp = self.do("GET", path, params=params)
            try:
                data = resp.json()
            except simplejson.JSONDecodeError:
                LOGGER.exception(
                    f"Failed to decode the response to json: '{resp.text}'"
                )
                raise
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
                elif "vid-offset" in data:
                    offset_value = data.get("vid-offset")
                    if data.get("has-more") == False:
                        return
                else:
                    offset_value = data.get(offset_key)
            if not offset_value:
                break

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.RequestException,
            ratelimit.RateLimitException,
            RetryAfterReauth,
        ),
        giveup=giveup_http_codes,
        jitter=backoff.full_jitter,
        max_tries=10,
        max_time=5 * 60,
    )
    @limits(calls=100, period=10)
    def do(
        self,
        method: str,
        url: str,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        params: Optional[Any] = None,
    ) -> requests.Response:
        params = params or {}
        url = f"{self.BASE_URL}{url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        # access_token is cached
        try:
            self.refresh_access_token()
        except requests.HTTPError as err:
            if err.response.status_code == 400:
                try:
                    err_data: Dict = err.response.json()
                    msg = err_data.get("message")
                    if msg is None:
                        msg = "invalid credentials"
                    raise InvalidCredentials(msg)
                except Exception:
                    raise InvalidCredentials(err.response.text)
            raise

        with self.SESSION.request(
            method,
            url,
            headers=headers,
            params=params,
            timeout=self.timeout,
            json=json,
            data=data,
        ) as response:
            if response.status_code == 401:
                raise RetryAfterReauth

            if response.status_code == 403:
                err_msg: Dict = response.json()
                if err_msg.get("category") == "MISSING_SCOPES":
                    raise MissingScope(err_msg)
            LOGGER.debug(response.url)
            response.raise_for_status()
            return response

    def test_endpoint(self, url, params={}):
        self.refresh_access_token()

        url = f"{self.BASE_URL}{url}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        with self.SESSION.get(
            url, headers=headers, params=params, timeout=self.timeout
        ) as response:
            response.raise_for_status()

    def refresh_access_token(self):
        if self.access_token_ttl and datetime.utcnow() < self.access_token_ttl:
            return

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

        data = resp.json()

        # cache the access_token for ttl (default is 1800 seconds)
        # subtract 5 minutes just to be super sure.
        expires_in_seconds = data["expires_in"]
        self.access_token_ttl = datetime.utcnow() + timedelta(
            seconds=expires_in_seconds - 60 * 5
        )
        self.access_token = data["access_token"]
