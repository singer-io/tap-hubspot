#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import logging.config
import sys

import backoff
import dateutil.parser
import requests
import stitchstream


CLIENT_ID = "688261c2-cf70-11e5-9eb6-31930a91f78c"
API_KEY = None
REFRESH_TOKEN = None

base_url = "https://api.hubapi.com"
default_start_date = datetime.datetime(2000, 1, 1).isoformat()

# logging.config.fileConfig("/etc/stitch/logging.conf")
logger = logging.getLogger("stitch.streamer")

entities = [
    "contacts",
    "companies",
    "deals",
    "subscription_changes",
    "campaigns",
    "email_events",
    "contact_lists",
    "forms",
    "workflows",
    "keywords",
    "owners",
]
state = {entity: default_start_date
         for entity in entities
         if entity not in ["campaigns"]}

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


def get_field_type_schema(field_type):
    if field_type == "bool":
        return {"type": ["null", "boolean"]}

    elif field_type == "datetime":
        return {"type": ["null", "integer"], "format": "date-time"}

    elif field_type == "number":
        return {"type": ["null", "number"]}

    else:
        return {"type": ["null", "string"]}


def get_contracts_field_schema(field_type):
    return {
        "type": ["null", "object"],
        "properties": {
            "value": get_field_type_schema(field_type)
        }
    }


def get_field_schema(field_type):
    return {
        "type": ["null", "object"],
        "properties": {
            "value": get_field_type_schema(field_type),
            "timestamp": get_field_type_schema("datetime"),
            "source": get_field_type_schema("string"),
            "sourceId": get_field_type_schema("string"),
        }
    }


def parse_custom_schema(entity_name, data):
    if entity_name == "contracts":
        func = get_contracts_field_schema
    else:
        func = get_field_schema

    return {field['name']: func(field['type']) for field in data}


def get_custom_schema(entity_name):
    data = request(get_url(entity_name + "_properties")).json()
    return parse_custom_schema(entity_name, data)


def get_schema(entity_name):
    with open("schemas/{}.json".format(entity_name)) as f:
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
        dt = dt.isoformat()

    return dt


def get_url(endpoint, **kwargs):
    if endpoint not in endpoints:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    return base_url + endpoints[endpoint].format(**kwargs)


def transform_field(value, schema):
    if "array" in schema['type']:
        tmp = []
        for v in value:
            tmp.append(transform_field(v, schema['items']))

        return tmp

    if "object" in schema['type']:
        tmp = {}
        for field_name, field_schema in schema['properties'].items():
            if field_name in value:
                tmp[field_name] = transform_field(value[field_name], field_schema)

        return tmp

    if "integer" in schema['type'] and value:
        value = int(value)

    if "number" in schema['type'] and value:
        value = float(value)

    if "format" in schema:
        if schema['format'] == "date-time" and value:
            value = transform_timestamp(value)

    return value


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
def request(url, params=None):
    global API_KEY
    _params = {"access_token": API_KEY}
    if params is not None:
        _params.update(params)

    response = requests.get(url, params=_params)
    if response.status_code == 401:
        API_KEY = get_api_key()
        return request(url, params)

    else:
        response.raise_for_status()
        return response


def do_check():
    try:
        request(get_url('contacts_recent'))
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using {e.request.url}; "
                     "received status {e.response.status_code}: {e.response.test}".format(e=e))
        sys.exit(-1)


def sync_contacts():
    last_sync = dateutil.parser.parse(state['contacts'])
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        recent = False
        endpoint = "contacts_all"
        logger.error("Syncing all contacts")
    else:
        recent = True
        endpoint = "contacts_recent"
        logger.error("Syncing recent contacts")

    schema = get_schema('contacts')
    stitchstream.write_schema('contacts', schema)

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
        logger.error("Grabbed {} contacts".format(len(data['contacts'])))

        vids = []
        for contact in data['contacts']:
            if 'lastmodifieddate' in contact['properties']:
                modified_time = transform_timestamp(contact['properties']['lastmodifieddate']['value'], False)
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                vids.append(contact['canonical-vid'])

        if len(vids) > 0:
            logger.error("Getting details for {} contacts".format(len(vids)))

            resp = request(get_url('contacts_detail'), params={'vid': vids})
            transformed_records = [transform_record(record, schema) for record in resp.json().values()]

            logger.error("Persisting {} contacts".format(len(transformed_records)))
            stitchstream.write_records('contacts', transformed_records)
            persisted_count += len(vids)

    state['contacts'] = datetime.datetime.utcnow().isoformat()
    stitchstream.write_state(state)
    logger.error("Persisted {} of {} contacts".format(persisted_count, fetched_count))
    return persisted_count


def sync_companies():
    last_sync = dateutil.parser.parse(state['companies'])
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "companies_all"
        more_key = "has-more"
        path_key = "companies"
        logger.error("Syncing all companies")
    else:
        endpoint = "companies_recent"
        more_key = "hasMore"
        path_key = "results"
        logger.error("Syncing recent companies")

    schema = get_schema('companies')
    stitchstream.write_schema('companies', schema)

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
        logger.error("Grabbed {} companies".format(len(data[path_key])))
        logger.error("Grabbing details for {} companies".format(len(data[path_key])))

        transformed_records = []
        for record in data[path_key]:
            resp = request(get_url('companies_detail', company_id=record['companyId']))
            transformed_record = transform_record(resp.json(), schema)

            if 'hs_lastmodifieddate' in transformed_record['properties']:
                modified_time = dateutil.parser.parse(transformed_record['properties']['hs_lastmodifieddate']['value'])
            elif 'createdate' in transformed_record['properties']:
                modified_time = dateutil.parser.parse(transformed_record['properties']['createdate']['value'])
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                transformed_records.append(transformed_record)

        logger.error("Persisting {} companies".format(len(transformed_records)))
        stitchstream.write_records('companies', transformed_records)
        persisted_count += len(transformed_records)

    state['companies'] = datetime.datetime.utcnow().isoformat()
    stitchstream.write_state(state)
    logger.error("Persisted {} of {} companies".format(persisted_count, fetched_count))
    return persisted_count


def sync_deals():
    last_sync = dateutil.parser.parse(state['deals'])
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "deals_all"
        logger.error("Syncing all deals")
    else:
        endpoint = "deals_recent"
        logger.error("Syncing recent deals")

    schema = get_schema('deals')
    stitchstream.write_schema('deals', schema)

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
        logger.error("Grabbed {} deals".format(len(data['deals'])))
        logger.error("Grabbing details for {} deals".format(len(data['deals'])))

        transformed_records = []
        for record in data['deals']:
            resp = request(get_url('deals_detail', deal_id=record['dealId']))
            transformed_record = transform_record(resp.json(), schema)

            if 'hs_lastmodifieddate' in transformed_record['properties']:
                modified_time = dateutil.parser.parse(transformed_record['properties']['hs_lastmodifieddate']['value'])
            elif 'createdate' in transformed_record['properties']:
                modified_time = dateutil.parser.parse(transformed_record['properties']['createdate']['value'])
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                transformed_records.append(transformed_record)

        logger.error("Persisting {} deals".format(len(transformed_records)))
        stitchstream.write_records('deals', transformed_records)
        persisted_count += len(transformed_records)

    state['deals'] = datetime.datetime.utcnow().isoformat()
    stitchstream.write_state(state)
    logger.error("Persisted {} of {} deals".format(persisted_count, fetched_count))
    return persisted_count


def sync_campaigns():
    schema = get_schema('campaigns')
    stitchstream.write_schema('campaigns', schema)

    logger.error("Syncing all campaigns")

    params = {'limit': 500}
    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url('campaigns_all'), params=params)
        data = resp.json()

        has_more = data.get('hasMore', False)
        if has_more:
            params['offset'] = data['offset']

        logger.error("Grabbed {} campaigns".format(len(data['campaigns'])))
        logger.error("Grabbing details for {} campaigns".format(len(data['campaigns'])))

        transformed_records = []
        for record in data['campaigns']:
            resp = request(get_url('campaigns_detail', campaign_id=record['id']))
            transformed_records.append(transform_record(resp.json(), schema))

        logger.error("Persisting {} campaigns".format(len(transformed_records)))
        stitchstream.write_records('campaigns', transformed_records)
        persisted_count += len(data['campaigns'])

    # campaigns don't have any created/modified dates to update state with
    logger.error("Persisted {} campaigns".format(persisted_count))
    return persisted_count


def sync_no_details(entity_type, entity_path, state_path):
    last_sync = dateutil.parser.parse(state[entity_type])
    start_timestamp = int(last_sync.timestamp() * 1000)

    schema = get_schema(entity_type)
    stitchstream.write_schema(entity_type, schema)

    logger.error("Syncing {} from {}".format(entity_type, last_sync))

    params = {
        'startTimestamp': start_timestamp,
        'limit': 1000,
    }

    all_records = []
    has_more = True
    while has_more:
        resp = request(get_url(entity_type), params=params)
        data = resp.json()

        has_more = data.get("hasMore", False)
        persisted_count = 0
        if has_more:
            params["offset"] = data["offset"]

        logger.error("Grabbed {} {}".format(len(data[entity_path]), entity_type))

        transformed_records = [transform_record(record, schema) for record in data[entity_path]]

        logger.error("Persisting {} {} up to {}".format(len(transformed_records), entity_type, state[entity_type]))
        stitchstream.write_records(entity_type, transformed_records)
        persisted_count += len(data[entity_path])

    state[entity_type] = transformed_records[-1][state_path]
    stitchstream.write_state(state)
    logger.error("Persisted {} {}".format(persisted_count, entity_type))
    return persisted_count


def sync_subscription_changes():
    return sync_no_details("subscription_changes", "timeline", "timestamp")


def sync_email_events():
    return sync_no_details("email_events", "events", "created")


def sync_time_filtered(entity_type, timestamp_path, entity_path=None):
    last_sync = dateutil.parser.parse(state[entity_type])

    logger.error("Syncing all {}".format(entity_type))

    schema = get_schema(entity_type)
    stitchstream.write_schema(entity_type, schema)

    resp = request(get_url(entity_type))
    records = resp.json()
    if entity_path:
        records = records[entity_path]

    fetched_count = len(records)
    logger.error("Grabbed {} {}".format(len(records), entity_type))

    transformed_records = []
    for record in records:
        transformed_record = transform_record(record, schema)
        if dateutil.parser.parse(transformed_record[timestamp_path]) >= last_sync:
            transformed_records.append(transformed_record)

    if len(transformed_records) > 0:
        logger.error("Persisting {} {} up to {}".format(len(transformed_records), entity_type, state[entity_type]))
        stitchstream.write_records(entity_type, transformed_records)
        persisted_count = len(transformed_records)

    state[entity_type] = transformed_records[-1][timestamp_path]
    stitchstream.write_state(state)
    logger.error("Persisted {} of {} {}".format(persisted_count, fetched_count, entity_type))
    return persisted_count


def sync_contact_lists():
    last_sync = dateutil.parser.parse(state['contact_lists'])

    logger.error("Syncing all contact lists")

    schema = get_schema('contact_lists')
    stitchstream.write_schema('contact_lists', schema)

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
        logger.error("Grabbed {} contact lists".format(len(records)))

        transformed_records = []
        for record in records:
            transformed_record = transform_record(record, schema)
            if dateutil.parser.parse(transformed_record['updatedAt']) >= last_sync:
                transformed_records.append(transformed_record)

        if len(transformed_records) > 0:
            logger.error("Persisting {} contact lists up to {}".format(len(transformed_records), state['contact_lists']))
            stitchstream.write_records('contact_lists', transformed_records)
            persisted_count += len(transformed_records)

    state['contact_lists'] = transformed_records[-1]['updatedAt']
    stitchstream.write_state(state)
    logger.error("Persisted {} of {} contact lists".format(persisted_count, fetched_count))
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
    global REFRESH_TOKEN
    global API_KEY

    parser = argparse.ArgumentParser()
    parser.add_argument('func', choices=['check', 'sync'])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Sets the log level to DEBUG (default INFO)')
    parser.set_defaults(debug=False)
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.state:
        logger.error("Loading state from " + args.state)
        load_state(args.state)

    logger.error("Refreshing oath token")

    with open(args.config) as f:
        config = json.load(f)

    REFRESH_TOKEN = config['refresh_token']
    API_KEY = get_api_key()

    if args.func == 'check':
        do_check()

    elif args.func == 'sync':
        logger.error("Starting sync")
        persisted_count = do_sync()
        logger.error("Sync completed. Persisted {} records".format(persisted_count))


if __name__ == '__main__':
    main()
