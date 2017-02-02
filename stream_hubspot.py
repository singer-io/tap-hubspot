#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import sys

import backoff
import requests
import stitchstream


BASE_URL = "https://api.hubapi.com"
STITCH_REDIRECT_URL = "???"
STITCH_CLIENT_ID = "688261c2-cf70-11e5-9eb6-31930a91f78c"
STITCH_CLIENT_SECRET = None
OAUTH_CODE = None

REFRESH_TOKEN = None
ACCESS_TOKEN = None
TOKEN_EXPIRES = None

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
DEFAULT_START_DATE = datetime.datetime(2000, 1, 1).strftime(DATETIME_FMT)

state = {
    "contacts":             DEFAULT_START_DATE,
    "companies":            DEFAULT_START_DATE,
    "deals":                DEFAULT_START_DATE,
    "subscription_changes": DEFAULT_START_DATE,
    "email_events":         DEFAULT_START_DATE,
    "contact_lists":        DEFAULT_START_DATE,
    "forms":                DEFAULT_START_DATE,
    "workflows":            DEFAULT_START_DATE,
    "keywords":             DEFAULT_START_DATE,
    "owners":               DEFAULT_START_DATE,
}

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


def update_state(key, dt):
    if dt is None:
        return

    if isinstance(dt, datetime.datetime):
        dt = dt.strftime(DATETIME_FMT)

    if dt > state[key]:
        state[key] = dt


def get_url(endpoint, **kwargs):
    if endpoint not in endpoints:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    return base_url + endpoints[endpoint].format(**kwargs)


def get_field_type_schema(field_type):
    if field_type == "bool":
        return {"type": ["null", "boolean"]}

    elif field_type == "datetime":
        return {
            "anyOf": [
                {
                    "type": "string",
                    "format": "date-time",
                },
                {
                    "type": "null",
                },
            ],
        }

    elif field_type == "number":
        return {"type": ["null", "number"]}

    else:
        return {"type": ["null", "string"]}


def get_field_schema(field_type, extras=False):
    if extras:
        return {
            "type": ["null", "object"],
            "properties": {
                "value": get_field_type_schema(field_type),
                "timestamp": get_field_type_schema("datetime"),
                "source": get_field_type_schema("string"),
                "sourceId": get_field_type_schema("string"),
            }
        }
    else:
        return get_field_type_schema(field_type)

    return d


def parse_custom_schema(entity_name, data):
    extras = entity_name != "contacts"
    return {field['name']: get_field_schema(field['type'], extras) for field in data}


def get_custom_schema(entity_name):
    data = request(get_url(entity_name + "_properties")).json()
    return parse_custom_schema(entity_name, data)


def load_schema(entity_name):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        "stream_hubspot",
                        "{}.json".format(entity_name))
    with open(path) as f:
        schema = json.loads(f.read())

    if entity_name in ["contacts", "companies", "deals"]:
        custom_schema = get_custom_schema(entity_name)
        schema['properties']['properties'] = {
            "type": ["null", "object"],
            "properties": custom_schema,
        }

    return schema


def transform_timestamp(timestamp):
    return datetime.datetime.utcfromtimestamp(int(timestamp) * 0.001).strftime(DATETIME_FMT)


def transform_field(value, schema):
    if "array" in schema['type']:
        tmp = []
        for v in value:
            tmp.append(transform_field(v, schema['items']))

        return tmp

    elif "object" in schema['type']:
        tmp = {}
        for field_name, field_schema in schema['properties'].items():
            if field_name in value:
                tmp[field_name] = transform_field(value[field_name], field_schema)

        return tmp

    elif "format" in schema:
        if schema['format'] == "date-time" and value:
            return transform_timestamp(value)

    elif "integer" in schema['type'] and value:
        return int(value)

    elif "number" in schema['type'] and value:
        return float(value)


def transform_record(record, schema):
    return {field_name: transform_field(record[field_name], field_schema)
            for field_name, field_schema in schema['properties'].items()
            if field_name in record}


def _auth(payload):
    global ACCESS_TOKEN
    global REFRESH_TOKEN
    global TOKEN_EXPIRES

    payload["client_id"] = STITCH_CLIENT_ID
    payload["client_secret"] = STITCH_CLIENT_SECRET

    resp = requests.post(base_url + "/oauth/v1/token", data=data)
    if resp.status_code == 400:
        # failed to auth
        sys.exit(1)

    auth = resp.json()
    ACCESS_TOKEN = auth['access_token']
    REFRESH_TOKEN = auth['refresh_token']
    TOKEN_EXPIRES = datetime.datetime.utcnow() + datetime.timedelta(seconds=auth['expires_in'])


def authorize():
    _auth({
        "grant_type": "refresh_token",
        "code": OAUTH_CODE,
    })


def refresh_token():
    _auth({
        "grant_type": "refresh_token",
        "redirect_uri": STITCH_REDIRECT_URL,
        "refresh_token": REFRESH_TOKEN,
    })


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
def request(url, params=None):
    if datetime.datetime.utcnow() >= TOKEN_EXPIRES:
        refresh_token()

    params = params or {}
    params["access_token"] = ACCESS_TOKEN

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


def sync_contacts():
    last_sync = datetime.datetime.strptime(state['contacts'], DATETIME_FMT)
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        recent = False
        endpoint = "contacts_all"
        logger.info("Syncing all contacts")
    else:
        recent = True
        endpoint = "contacts_recent"
        logger.info("Syncing recent contacts")

    schema = load_schema('contacts')
    stream("schema", schema, "contacts")

    params = {
        'showListMemberships': True,
        'count': 100,
    }
    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get('has-more', False)
        if has_more:
            params['vidOffset'] = data['vid-offset']
            if recent:
                params['timeOffset'] = data['time-offset']

        vids = []
        for contact in data['contacts']:
            if 'lastmodifieddate' in contact['properties']:
                modified_time = transform_timestamp(contact['properties']['lastmodifieddate'])
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                resp = request(get_url('contacts_detail'), params={'vid': vids})
                transformed = transform_record(record, schema)
                stitchstream.write_record('contacts', transformed)
                persisted_count += 1
                update_state('contacts', modified_time)

        stitchstream.write_state(state)

    return persisted_count


def sync_companies():
    last_sync = datetime.datetime.strptime(state['companies'], DATETIME_FMT)
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "companies_all"
        more_key = "has-more"
        path_key = "companies"
        logger.info("Syncing all companies")
    else:
        endpoint = "companies_recent"
        more_key = "hasMore"
        path_key = "results"
        logger.info("Syncing recent companies")

    schema = load_schema('companies')
    stitchstream.write_schema("companies", schema)

    params = {'count': 250}
    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get(more_key, False)
        if has_more:
            params['offset'] = data['offset']

        for record in data[path_key]:
            resp = request(get_url('companies_detail', company_id=record['companyId']))
            transformed = transform_record(resp.json(), schema)

            if 'hs_lastmodifieddate' in transformed['properties']:
                modified_time = transformed['properties']['hs_lastmodifieddate']['value']
            elif 'createdate' in transformed['properties']:
                modified_time = transformed['properties']['createdate']['value']
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                stitchstream.write_record('companies', transformed)
                persisted_count += 1

            update_state('companies', modified_time)

        stitchstream.write_state(state)

    return persisted_count


def sync_deals():
    last_sync = datetime.datetime.strptime(state['deals'], DATETIME_FMT)
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "deals_all"
        logger.info("Syncing all deals")
    else:
        endpoint = "deals_recent"
        logger.info("Syncing recent deals")

    schema = load_schema('deals')
    stream("schema", schema, "deals")

    params = {'count': 250}
    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get('hasMore', False)
        if has_more:
            params['offset'] = data['offset']

        for record in data['deals']:
            resp = request(get_url('deals_detail', deal_id=record['dealId']))
            transformed = transform_record(resp.json(), schema)

            if 'hs_lastmodifieddate' in transformed['properties']:
                modified_time = transformed['properties']['hs_lastmodifieddate']['value']
            elif 'createdate' in transformed['properties']:
                modified_time = transformed['properties']['createdate']['value']
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                stitchstream.write_record('deals', transformed)
                persisted_count += 1

            update_state('deals', modified_time)

        stitchstream.write_state(state)

    return persisted_count


def sync_campaigns():
    # campaigns don't have any created/modified dates to update state with
    logger.info("Syncing all campaigns")

    schema = load_schema('campaigns')
    stream("schema", schema, "campaigns")

    params = {'limit': 500}
    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url('campaigns_all'), params=params)
        data = resp.json()

        has_more = data.get('hasMore', False)
        if has_more:
            params['offset'] = data['offset']

        for record in data['campaigns']:
            resp = request(get_url('campaigns_detail', campaign_id=record['id']))
            transformed = transform_record(resp.json(), schema)
            stitchstream.write_record('campaigns', transformed)
            persisted_count += 1

    return persisted_count


def sync_no_details(entity_type, entity_path, state_path):
    last_sync = datetime.datetime.strptime(state[entity_type], DATETIME_FMT)
    logger.info("Syncing {} from {}".format(entity_type, last_sync))

    schema = load_schema(entity_type)
    stitchstream.write_schema(entity_type, schema)

    params = {
        'startTimestamp': int(last_sync.timestamp() * 1000),
        'limit': 1000,
    }

    has_more = True
    while has_more:
        resp = request(get_url(entity_type), params=params)
        data = resp.json()

        has_more = data.get("hasMore", False)
        persisted_count = 0
        if has_more:
            params["offset"] = data["offset"]

        for record in data[entity_path]:
            transformed = transform_record(record, schema)
            stitchstream.write_record(entity_type, transformed)
            persisted_count += 1
            update_state(entity_type, transformed[state_path])

        stitchstream.write_state(state)

    return persisted_count


def sync_subscription_changes():
    return sync_no_details("subscription_changes", "timeline", "timestamp")


def sync_email_events():
    return sync_no_details("email_events", "events", "created")


def sync_time_filtered(entity_type, timestamp_path, entity_path=None):
    last_sync = datetime.datetime.strptime(state[entity_type], DATETIME_FMT)
    logger.info("Syncing all {} from {}".format(entity_type, last_sync))

    schema = load_schema(entity_type)
    stitchstream.write_schema(entity_type, schema)

    resp = request(get_url(entity_type))
    records = resp.json()
    if entity_path:
        records = records[entity_path]

    for record in records:
        transformed = transform_record(record, schema)
        stitchstream.write_record(entity_type, transformed)
        persisted_count += 1
        update_state(entity_type, transformed[timestamp_path])

    stitchstream.write_state(state)
    return persisted_count


def sync_contact_lists():
    last_sync = datetime.datetime.strptime(state['contact_lists'], DATETIME_FMT)
    logger.info("Syncing all contact lists")

    schema = load_schema('contact_lists')
    stream("schema", schema, "contact_lists")

    params = {'count': 250}
    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url('contact_lists'), params=params)
        data = resp.json()
        records = data['lists']

        has_more = data.get('has-more', False)
        if has_more:
            params['offset'] = data['offset']

        for record in records:
            transformed = transform_record(record, schema)
            stitchstream.write_record("contact_lists", transformed)
            persisted_count += 1
            update_state('contact_lists', transformed['updatedAt'])

        stitchstream.write_state(state)

    return persisted_count


def sync_forms():
    return sync_time_filtered("forms", "updatedAt")


def sync_workflows():
    return sync_time_filtered("workflows", "updatedAt", "workflows")


def sync_keywords():
    return sync_time_filtered("keywords", "created_at", "keywords")


def sync_owners():
    return sync_time_filtered("owners", "updatedAt")


def do_sync():
    persisted_count = 0
    persisted_count += sync_contacts()
    persisted_count += sync_companies()
    persisted_count += sync_deals()
    persisted_count += sync_campaigns()
    persisted_count += sync_subscription_changes()
    persisted_count += sync_email_events()
    persisted_count += sync_contact_lists()
    persisted_count += sync_forms()
    persisted_count += sync_workflows()
    persisted_count += sync_keywords()
    persisted_count += sync_owners()
    return persisted_count


def main():
    global OAUTH_CODE

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    args = parser.parse_args()

    if args.state:
        logger.info("Loading state from " + args.state)
        with open(args.state) as f:
            state.update(json.load(f))

    logger.info("Authorizing OAUTH code")
    with open(args.config) as f:
        config = json.load(f)

    OAUTH_CODE = config['oauth_code']
    authorize()

    try:
        logger.info("Starting sync")
        persisted_count = do_sync()
    except Exception as e:
        logger.exception("Error ocurred during sync. Aborting.")
        sys.exit(1)

    logger.info("Sync completed. Persisted {} records".format(persisted_count))


if __name__ == '__main__':
    main()
