#!/usr/bin/env python3

import argparse
import datetime
import json
import os
import sys

import backoff
import dateutil.parser
import requests
import stitchstream


CLIENT_ID = "688261c2-cf70-11e5-9eb6-31930a91f78c"
QUIET = False
API_KEY = None
REFRESH_TOKEN = None
GET_COUNT = 0
DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"

logger = stitchstream.get_logger()

default_start_date = datetime.datetime(2000, 1, 1).strftime(DATETIME_FMT)
state = {
    "contacts": default_start_date,
    "companies": default_start_date,
    "deals": default_start_date,
    "subscription_changes": default_start_date,
    "email_events": default_start_date,
    "contact_lists": default_start_date,
    "forms": default_start_date,
    "workflows": default_start_date,
    "keywords": default_start_date,
    "owners": default_start_date,
}

base_url = "https://api.hubapi.com"
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
    if isinstance(dt, datetime.datetime):
        dt = dt.strftime(DATETIME_FMT)
        
    if dt > state[key]:
        state[key] = dt

def get_url(endpoint, **kwargs):
    if endpoint not in endpoints:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    return base_url + endpoints[endpoint].format(**kwargs)


def stream(method, data, entity_type=None):
    if not QUIET:
        if method == "state":
            stitchstream.write_state(data)
        elif method == "schema":
            stitchstream.write_schema(entity_type, data)
        elif method == "records":
            stitchstream.write_records(entity_type, data)
        else:
            raise ValueError("Unknown method {}".format(method))


def get_field_type_schema(field_type):
    if field_type == "bool":
        return {"type": ["null", "boolean"]}

    elif field_type == "datetime":
        return {"type": ["null", "string"], "format": "date-time"}

    elif field_type == "number":
        return {"type": ["null", "number"]}

    else:
        return {"type": ["null", "string"]}


def get_field_schema(field_type, extras=False):
    d = {
        "type": ["null", "object"],
        "properties": {
            "value": get_field_type_schema(field_type)
        }
    }

    if extras:
        d['properties']['timestamp'] = get_field_type_schema("datetime")
        d['properties']['source'] = get_field_type_schema("string")
        d['properties']['sourceId'] = get_field_type_schema("string")

    return d


def parse_custom_schema(entity_name, data):
    extras = entity_name != "contracts"
    return {field['name']: get_field_schema(field['type'], extras) for field in data}


def get_custom_schema(entity_name):
    data = request(get_url(entity_name + "_properties")).json()
    return parse_custom_schema(entity_name, data)


def get_schema(entity_name):
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


def transform_timestamp(timestamp, iso=True):
    dt = datetime.datetime.utcfromtimestamp(int(timestamp) * 0.001)
    if iso:
        dt = dt.strftime(DATETIME_FMT)

    return dt


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


def get_api_key():
    data = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "refresh_token": REFRESH_TOKEN,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post("https://api.hubapi.com/auth/v1/refresh", headers=headers, data=data).json()
    return resp['access_token']


def load_state(state_file):
    with open(state_file) as f:
        state = json.load(f)

    state.update(state)


@backoff.on_exception(backoff.expo,
                        (requests.exceptions.RequestException),
                        max_tries=5,
                        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                        factor=2)
def request(url, params=None, reauth=False):
    global API_KEY
    global GET_COUNT
    _params = {"access_token": API_KEY}
    if params is not None:
        _params.update(params)

    response = requests.get(url, params=_params)
    GET_COUNT += 1
    if response.status_code == 401 and not reauth:
        API_KEY = get_api_key()
        return request(url, params, reauth=True)

    else:
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

    schema = get_schema('contacts')
    stream("schema", schema, "contacts")

    params = {
        'showListMemberships': True,
        'count': 100,
    }
    has_more = True
    fetched_count = 0
    persisted_count = 0
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get('has-more', False)
        if has_more:
            params['vidOffset'] = data['vid-offset']
            if recent:
                params['timeOffset'] = data['time-offset']

        fetched_count += len(data['contacts'])
        logger.info("Grabbed {} contacts".format(len(data['contacts'])))

        vids = []
        for contact in data['contacts']:
            if 'lastmodifieddate' in contact['properties']:
                modified_time = transform_timestamp(contact['properties']['lastmodifieddate']['value'], False)
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                vids.append(contact['canonical-vid'])
                update_state('contacts', modified_time)

        if len(vids) > 0:
            logger.info("Getting details for {} contacts".format(len(vids)))

            resp = request(get_url('contacts_detail'), params={'vid': vids})
            for record in resp.json().values():
                stitchstream.write_record('contacts', transform_record(record, schema))
                persisted_count += 1
    stream("state", state)

    logger.info("Persisted {} of {} contacts".format(persisted_count, fetched_count))
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

    schema = get_schema('companies')
    stream("schema", schema, "companies")

    params = {'count': 250}
    has_more = True
    fetched_count = 0
    persisted_count = 0
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get(more_key, False)
        if has_more:
            params['offset'] = data['offset']

        fetched_count += len(data[path_key])
        logger.info("Grabbed {} companies".format(len(data[path_key])))
        logger.info("Grabbing details for {} companies".format(len(data[path_key])))

        for record in data[path_key]:
            resp = request(get_url('companies_detail', company_id=record['companyId']))
            transformed_record = transform_record(resp.json(), schema)

            if 'hs_lastmodifieddate' in transformed_record['properties']:
                modified_time = datetime.datetime.strptime(
                    transformed_record['properties']['hs_lastmodifieddate']['value'],
                    DATETIME_FMT)
                update_state('companies', modified_time)
                
            elif 'createdate' in transformed_record['properties']:
                modified_time = datetime.datetime.strptime(
                    transformed_record['properties']['createdate']['value'],
                    DATETIME_FMT)
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                stitchstream.write_record('companies', transformed_record)
                persisted_count += 1
    stream("state", state)
    logger.info("Persisted {} of {} companies".format(persisted_count, fetched_count))
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

    schema = get_schema('deals')
    stream("schema", schema, "deals")

    params = {'count': 250}
    has_more = True
    fetched_count = 0
    persisted_count = 0
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get('hasMore', False)
        if has_more:
            params['offset'] = data['offset']

        fetched_count += len(data['deals'])
        logger.info("Grabbed {} deals".format(len(data['deals'])))
        logger.info("Grabbing details for {} deals".format(len(data['deals'])))

        for record in data['deals']:
            resp = request(get_url('deals_detail', deal_id=record['dealId']))
            transformed_record = transform_record(resp.json(), schema)

            if 'hs_lastmodifieddate' in transformed_record['properties']:
                modified_time = datetime.datetime.strptime(
                    transformed_record['properties']['hs_lastmodifieddate']['value'],
                    DATETIME_FMT)
                update_state('deals', modified_time)
            elif 'createdate' in transformed_record['properties']:
                modified_time = datetime.datetime.strptime(
                    transformed_record['properties']['createdate']['value'],
                    DATETIME_FMT)
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                stitchstream.write_record('deals', transformed_record)
                persisted_count += 1
    stream("state", state)
    logger.info("Persisted {} of {} deals".format(persisted_count, fetched_count))
    return persisted_count


def sync_campaigns():
    schema = get_schema('campaigns')
    stream("schema", schema, "campaigns")

    logger.info("Syncing all campaigns")

    params = {'limit': 500}
    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url('campaigns_all'), params=params)
        data = resp.json()

        has_more = data.get('hasMore', False)
        if has_more:
            params['offset'] = data['offset']

        logger.info("Grabbed {} campaigns".format(len(data['campaigns'])))
        logger.info("Grabbing details for {} campaigns".format(len(data['campaigns'])))

        transformed_records = []
        for record in data['campaigns']:
            resp = request(get_url('campaigns_detail', campaign_id=record['id']))
            stitchstream.write_record('campaigns', transform_record(resp.json(), schema))
            persisted_count += 1
    # campaigns don't have any created/modified dates to update state with
    logger.info("Persisted {} campaigns".format(persisted_count))
    return persisted_count


def sync_no_details(entity_type, entity_path, state_path):
    last_sync = datetime.datetime.strptime(state[entity_type], DATETIME_FMT)
    start_timestamp = int(last_sync.timestamp() * 1000)

    schema = get_schema(entity_type)
    stream("schema", schema, entity_type)

    logger.info("Syncing {} from {}".format(entity_type, last_sync))

    params = {
        'startTimestamp': start_timestamp,
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

        logger.info("Grabbed {} {}".format(len(data[entity_path]), entity_type))

        for record in data[entity_path]:
            transformed = transform_record(record, schema)
            stitchstream.write_record(entity_type, transformed)
            persisted_count += 1
            update_state(entity_type, transformed[state_path])

    stream("state", state)
    logger.info("Persisted {} {}".format(persisted_count, entity_type))
    return persisted_count


def sync_subscription_changes():
    return sync_no_details("subscription_changes", "timeline", "timestamp")


def sync_email_events():
    return sync_no_details("email_events", "events", "created")


def sync_time_filtered(entity_type, timestamp_path, entity_path=None):
    last_sync = datetime.datetime.strptime(state[entity_type], DATETIME_FMT)

    logger.info("Syncing all {}".format(entity_type))

    schema = get_schema(entity_type)
    stream("schema", schema, entity_type)

    resp = request(get_url(entity_type))
    records = resp.json()
    if entity_path:
        records = records[entity_path]

    fetched_count = len(records)
    logger.info("Grabbed {} {}".format(len(records), entity_type))

    transformed_records = []
    for record in records:
        transformed_record = transform_record(record, schema)
        ts = datetime.datetime.strptime(transformed_record[timestamp_path], DATETIME_FMT)
        if ts >= last_sync:
            update_state(entity_type, ts)
            transformed_records.append(transformed_record)

    if len(transformed_records) > 0:
        logger.info("Persisting {} {} up to {}".format(len(transformed_records), entity_type, state[entity_type]))
        stream("records", transformed_records, entity_type)
        persisted_count = len(transformed_records)

    stream("state", state)
    logger.info("Persisted {} of {} {}".format(persisted_count, fetched_count, entity_type))
    return persisted_count


def sync_contact_lists():
    last_sync = datetime.datetime.strptime(state['contact_lists'], DATETIME_FMT)

    logger.info("Syncing all contact lists")

    schema = get_schema('contact_lists')
    stream("schema", schema, "contact_lists")

    params = {'count': 250}
    has_more = True
    fetched_count = 0
    persisted_count = 0
    while has_more:
        resp = request(get_url('contact_lists'), params=params)
        data = resp.json()
        records = data['lists']

        has_more = data.get('has-more', False)
        if has_more:
            params['offset'] = data['offset']

        fetched_count += len(records)
        logger.info("Grabbed {} contact lists".format(len(records)))

        transformed_records = []
        for record in records:
            transformed_record = transform_record(record, schema)
            ts = datetime.datetime.strptime(transformed_record['updatedAt'], DATETIME_FMT)
            if ts >= last_sync:
                update_state('contact_lists', ts)
                transformed_records.append(transformed_record)

        if len(transformed_records) > 0:
            logger.info("Persisting {} contact lists up to {}".format(len(transformed_records), state['contact_lists']))
            stream("records", transformed_records, "contact_lists")
            persisted_count += len(transformed_records)

        state['contact_lists'] = transformed_records[-1]['updatedAt']
        stream("state", state)

    logger.info("Persisted {} of {} contact lists".format(persisted_count, fetched_count))
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
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_companies()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_deals()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_campaigns()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_subscription_changes()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_email_events()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_contact_lists()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_forms()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_workflows()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_keywords()
    logger.info("# API requests: {}".format(GET_COUNT))
    persisted_count += sync_owners()
    logger.info("# API requests: {}".format(GET_COUNT))
    return persisted_count


def do_check():
    try:
        request(get_url('contacts_recent'))
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using {e.request.url}; "
                     "received status {e.response.status_code}: {e.response.test}".format(e=e))
        sys.exit(-1)


def main():
    global REFRESH_TOKEN
    global API_KEY
    global QUIET

    parser = argparse.ArgumentParser()
    parser.add_argument('func', choices=['check', 'sync'])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Sets the log level to DEBUG (default INFO)')
    parser.add_argument('-q', '--quiet', dest='quiet', action='store_true',
                        help='Do not output to stdout (no persisting)')
    parser.set_defaults(debug=False, quiet=False)
    args = parser.parse_args()

    QUIET = args.quiet

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.state:
        logger.info("Loading state from " + args.state)
        load_state(args.state)

    logger.info("Refreshing oath token")

    with open(args.config) as f:
        config = json.load(f)

    REFRESH_TOKEN = config['refresh_token']
    API_KEY = get_api_key()

    if args.func == 'check':
        do_check()

    elif args.func == 'sync':
        logger.info("Starting sync")
        try:
            persisted_count = do_sync()
        except Exception as e:
            logger.exception("Error ocurred during sync. Aborting.")
            sys.exit(1)

        logger.info("Sync completed. Persisted {} records".format(persisted_count))


if __name__ == '__main__':
    main()
