#!/usr/bin/env python3

import datetime

import backoff
import requests
import singer

from tap_hubspot import utils
from tap_hubspot.transform import transform, _transform_datetime

logger = singer.get_logger()

BASE_URL = "https://api.hubapi.com"
CONFIG = {
    "access_token": None,
    "token_expires": None,

    # in config.json
    "redirect_uri": None,
    "client_id": None,
    "client_secret": None,
    "refresh_token": None,
    "start_date": None,
}
STATE = {}

endpoints = {
    "contacts_properties":  "/properties/v1/contacts/properties",
    "contacts_all":         "/contacts/v1/lists/all/contacts/all",
    "contacts_recent":      "/contacts/v1/lists/recently_updated/contacts/recent",
    "contacts_detail":      "/contacts/v1/contact/vids/batch/",

    "companies_properties": "/companies/v2/properties",
    "companies_all":        "/companies/v2/companies/paged",
    "companies_recent":     "/companies/v2/companies/recent/modified",
    "companies_detail":     "/companies/v2/companies/{company_id}",

    "deals_properties":     "/companies/v2/properties",
    "deals_all":            "/deals/v1/deal/paged",
    "deals_recent":         "/deals/v1/deal/recent/modified",
    "deals_detail":         "/deals/v1/deal/{deal_id}",

    "campaigns_all":        "/email/public/v1/campaigns/by-id",
    "campaigns_detail":     "/email/public/v1/campaigns/{campaign_id}",

    "subscription_changes": "/email/public/v1/subscriptions/timeline",
    "email_events":         "/email/public/v1/events",
    "contact_lists":        "/contacts/v1/lists",
    "forms":                "/forms/v2/forms",
    "workflows":            "/automation/v3/workflows",
    "keywords":             "/keywords/v1/keywords",
    "owners":               "/owners/v2/owners",
}


def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def get_url(endpoint, **kwargs):
    if endpoint not in endpoints:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    return BASE_URL + endpoints[endpoint].format(**kwargs)


def get_field_type_schema(field_type):
    if field_type == "bool":
        return {"type": ["null", "boolean"]}

    elif field_type == "datetime":
        return {"type": ["null", "string"],
                "format": "date-time"}

    elif field_type == "number":
        return {"type": ["null", "number"]}

    else:
        return {"type": ["null", "string"]}


def get_field_schema(field_type, extras=False):
    if extras:
        return {
            "type": "object",
            "properties": {
                "value": get_field_type_schema(field_type),
                "timestamp": get_field_type_schema("datetime"),
                "source": get_field_type_schema("string"),
                "sourceId": get_field_type_schema("string"),
            }
        }
    else:
        return {
            "type": "object",
            "properties": {
                "value": get_field_type_schema(field_type),
            }
        }


def parse_custom_schema(entity_name, data):
    return {field['name']: get_field_schema(field['type'], entity_name != "contacts") for field in data}


def get_custom_schema(entity_name):
    return parse_custom_schema(entity_name, request(get_url(entity_name + "_properties")).json())


def load_schema(entity_name):
    schema = utils.load_schema(entity_name)
    if entity_name in ["contacts", "companies", "deals"]:
        custom_schema = get_custom_schema(entity_name)
        schema['properties']['properties'] = {
            "type": "object",
            "properties": custom_schema,
        }

    return schema


def refresh_token():
    payload = {
        "grant_type": "refresh_token",
        "redirect_uri": CONFIG['redirect_uri'],
        "refresh_token": CONFIG['refresh_token'],
        "client_id": CONFIG['client_id'],
        "client_secret": CONFIG['client_secret'],
    }

    logger.info("Refreshing token")
    resp = requests.post(BASE_URL + "/oauth/v1/token", data=payload)
    resp.raise_for_status()
    auth = resp.json()
    CONFIG['access_token'] = auth['access_token']
    CONFIG['refresh_token'] = auth['refresh_token']
    CONFIG['token_expires'] = datetime.datetime.utcnow() + datetime.timedelta(seconds=auth['expires_in'] - 600)
    logger.info("Token refreshed. Expires at {}".format(CONFIG['token_expires']))


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
def request(url, params=None):
    if CONFIG['token_expires'] is None or CONFIG['token_expires'] < datetime.datetime.utcnow():
        refresh_token()

    params = params or {}
    headers = {'Authorization': 'Bearer {}'.format(CONFIG['access_token'])}
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response


def gen_request(url, params, path, more_key, offset_keys, offset_targets):
    if not isinstance(offset_keys, list):
        offset_keys = [offset_keys]

    if not isinstance(offset_targets, list):
        offset_targets = [offset_targets]

    if len(offset_keys) != len(offset_targets):
        raise ValueError("Number of offset_keys must match number of offset_targets")

    while True:
        data = request(url, params).json()
        if path:
            for row in data[path]:
                yield row

            if not data.get(more_key, False):
                break

            for key, target in zip(offset_keys, offset_targets):
                params[target] = data[key]

        else:
            for row in data:
                yield row

            break


def sync_contacts():
    last_sync = utils.strptime(get_start("contacts"))
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "contacts_all"
        offset_keys = ['vid-offset']
        offset_targets = ['vidOffset']
    else:
        endpoint = "contacts_recent"
        offset_keys = ['vid-offset', 'time-offset']
        offset_targets = ['vidOffset', 'timeOffset']

    schema = load_schema("contacts")
    singer.write_schema("contacts", schema, ["canonical-vid"])

    url = get_url(endpoint)
    params = {
        'showListMemberships': True,
        'count': 100,
    }
    vids = []
    for row in gen_request(url, params, 'contacts', 'has-more', offset_keys, offset_targets):
        modified_time = None
        if 'lastmodifieddate' in row['properties']:
            modified_time = utils.strptime(_transform_datetime(row['properties']['lastmodifieddate']['value']))

        if not modified_time or modified_time >= last_sync:
            vids.append(row['vid'])

        if len(vids) == 100:
            data = request(get_url("contacts_detail"), params={'vid': vids}).json()
            for vid, record in data.items():
                record = transform(record, schema)
                singer.write_record("contacts", record)

                modified_time = None
                if 'lastmodifieddate' in record['properties']:
                    modified_time = record['properties']['lastmodifieddate']['value']
                    utils.update_state(STATE, "contacts", modified_time)

            vids = []

    singer.write_state(STATE)


def sync_companies():
    last_sync = utils.strptime(get_start("companies"))
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "companies_all"
        path = "companies"
        more_key = "has-more"
        offset_keys = ["offset"]
        offset_targets = ["offset"]
    else:
        endpoint = "companies_recent"
        path = "results"
        more_key = "hasMore"
        offset_keys = ["offset"]
        offset_targets = ["offset"]

    schema = load_schema('companies')
    singer.write_schema("companies", schema, ["companyId"])

    url = get_url(endpoint)
    params = {'count': 250}
    for i, row in enumerate(gen_request(url, params, path, more_key, offset_keys, offset_targets)):
        record = request(get_url("companies_detail", company_id=row['companyId'])).json()
        record = transform(record, schema)

        modified_time = None
        if 'hs_lastmodifieddate' in record:
            modified_time = utils.strptime(record['hs_lastmodifieddate']['value'])
        elif 'createdate' in record:
            modified_time = utils.strptime(record['createdate']['value'])

        if not modified_time or modified_time >= last_sync:
            singer.write_record("companies", record)
            utils.update_state(STATE, "companies", modified_time)

        if i % 250 == 0:
            singer.write_state(STATE)


def sync_deals():
    last_sync = utils.strptime(get_start("deals"))
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "deals_all"
    else:
        endpoint = "deals_recent"

    schema = load_schema("deals")
    singer.write_schema("deals", schema, ["portalId", "dealId"])

    url = get_url(endpoint)
    params = {'count': 250}
    for i, row in enumerate(gen_request(url, params, "deals", "hasMore", "offset", "offset")):
        record = request(get_url("deals_detail", deal_id=row['dealId'])).json()
        record = transform(record, schema)

        modified_time = None
        if 'hs_lastmodifieddate' in record:
            modified_time = utils.strptime(record['hs_lastmodifieddate']['value'])
        elif 'createdate' in record:
            modified_time = utils.strptime(record['createdate']['value'])

        if not modified_time or modified_time >= last_sync:
            singer.write_record("deals", record)
            utils.update_state(STATE, "deals", modified_time)

        if i % 250 == 0:
            singer.write_state(STATE)


def sync_campaigns():
    schema = load_schema("campaigns")
    singer.write_schema("campaigns", schema, ["id"])

    url = get_url("campaigns_all")
    params = {'limit': 500}
    for i, row in enumerate(gen_request(url, params, "campaigns", "hasMore", "offset", "offset")):
        record = request(get_url("campaigns_detail", campaign_id=row['id'])).json()
        record = transform(record, schema)
        singer.write_record("campaigns", record)


def sync_subscription_changes():
    schema = load_schema("subscription_changes")
    singer.write_schema("subscription_changes", schema, ["timestamp", "portalId", "recipient"])
    start = get_start("subscription_changes")

    url = get_url("subscription_changes")
    params = {
        'startTimestamp': int(utils.strptime(start).timestamp() * 1000),
        'limit': 1000,
    }
    for i, row in enumerate(gen_request(url, params, "timeline", "hasMore", "offset", "offset")):
        record = transform(row, schema)
        singer.write_record("subscription_changes", record)
        utils.update_state(STATE, "subscription_changes", record['timestamp'])

    singer.write_state(STATE)


def sync_email_events():
    schema = load_schema("email_events")
    singer.write_schema("email_events", schema, ["id"])
    start = get_start("email_events")

    url = get_url("email_events")
    params = {
        'startTimestamp': int(utils.strptime(start).timestamp() * 1000),
        'limit': 1000,
    }
    for i, row in enumerate(gen_request(url, params, "events", "hasMore", "offset", "offset")):
        record = transform(row, schema)
        singer.write_record("email_events", record)
        utils.update_state(STATE, "email_events", record['created'])

    singer.write_state(STATE)


def sync_contact_lists():
    schema = load_schema("contact_lists")
    singer.write_schema("contact_lists", schema, ["internalListId"])
    start = get_start("contact_lists")

    url = get_url("contact_lists")
    params = {'count': 250}
    for i, row in enumerate(gen_request(url, params, "lists", "has-more", "offset", "offset")):
        record = transform(row, schema)
        singer.write_record("contact_lists", record)


def sync_forms():
    schema = load_schema("forms")
    singer.write_schema("forms", schema, ["guid"])
    start = get_start("forms")

    data = request(get_url("forms")).json()
    for row in data:
        record = transform(row, schema)
        if record['updatedAt'] >= start:
            singer.write_record("forms", record)
            utils.update_state(STATE, "forms", record['updatedAt'])

    singer.write_state(STATE)


def sync_workflows():
    schema = load_schema("workflows")
    singer.write_schema("workflows", schema, ["id"])
    start = get_start("workflows")

    data = request(get_url("workflows")).json()
    for row in data['workflows']:
        record = transform(row, schema)
        if record['updatedAt'] >= start:
            singer.write_record("workflows", record)
            utils.update_state(STATE, "workflows", record['updatedAt'])

    singer.write_state(STATE)


def sync_keywords():
    schema = load_schema("keywords")
    singer.write_schema("keywords", schema, ["keyword_guid"])
    start = get_start("keywords")

    data = request(get_url("keywords")).json()
    for row in data['keywords']:
        record = transform(row, schema)
        if record['created_at'] >= start:
            singer.write_record("keywords", record)
            utils.update_state(STATE, "keywords", record['created_at'])

    singer.write_state(STATE)


def sync_owners():
    schema = load_schema("owners")
    singer.write_schema("owners", schema, ["portalId", "ownerId"])
    start = get_start("owners")

    data = request(get_url("owners")).json()
    for row in data:
        record = transform(row, schema)
        if record['updatedAt'] >= start:
            singer.write_record("owners", record)
            utils.update_state(STATE, "owners", record['updatedAt'])

    singer.write_state(STATE)


def do_sync():
    logger.info("Starting sync")
    sync_contacts()
    sync_companies()
    sync_deals()
    sync_campaigns()
    sync_subscription_changes()
    sync_email_events()
    sync_contact_lists()
    sync_forms()
    sync_workflows()
    sync_keywords()
    sync_owners()
    logger.info("Sync completed")


def main():
    args = utils.parse_args()

    config = utils.load_json(args.config)
    utils.check_config(config, ["redirect_uri", "client_id", "client_secret", "refresh_token", "start_date"])
    CONFIG.update(config)

    if args.state:
        STATE.update(utils.load_json(args.state))

    do_sync()


if __name__ == '__main__':
    main()
