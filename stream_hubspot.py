#!/usr/bin/env python3

import argparse
import datetime
import json
import logging
import sys

import backoff
import dateutil.parser
import requests
import stitchstream


CLIENT_ID = "688261c2-cf70-11e5-9eb6-31930a91f78c"

base_url = "https://api.hubapi.com"
default_start_date = datetime.datetime(2000, 1, 1).isoformat()

logger = logging.getLogger()

entities = [
    "contacts",
    "companies",
    "deals",
    "subscription_changes",
    "campaigns",
    "contacts_by_company",
    "email_events",
    "contact_lists",
    "forms",
    "workflows",
    "keywords",
    "owners",
]
state = {entity: default_start_date for entity in entities}

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

    "contacts_by_company":  "/companies/v2/companies/{company_id}/contacts",
    "subscription_changes": "/email/public/v1/subscriptions/timeline",
    "email_events":         "/email/public/v1/events",
    "contact_lists":        "/contacts/v1/lists",
    "forms":                "/forms/v2/forms",
    "workflows":            "/automation/v3/workflows",
    "keywords":             "/keywords/v1/keywords",
    "owners":               "/owners/v2/owners",
}


def configure_logging(level=logging.INFO):
    global logger
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


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
            "value": get_field_type_schema(field_type),
            "versions": {
                "type": ["null", "array"],
                "items": {
                    "type": ["null", "object"],
                    "properties": {
                        "value": get_field_type_schema(field_type),
                        "source-type": get_field_type_schema("string"),
                        "source-id": get_field_type_schema("string"),
                        "source-label": get_field_type_schema("string"),
                        "timestamp": get_field_type_schema("datetime"),
                    }
                }
            }
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
            "versions": {
                "type": ["null", "array"],
                "items": {
                    "type": ["null", "object"],
                    "properties": {
                        "name": get_field_type_schema("string"),
                        "value": get_field_type_schema(field_type),
                        "timestamp": get_field_type_schema("datetime"),
                        "source": get_field_type_schema("string"),
                        "sourceId": get_field_type_schema("string"),
                        "sourceVid": {
                            "type": ["null", "array"],
                            "items": {
                                "type": ["null", "integer"]
                            }
                        }
                    }
                }
            }
        }
    }


def parse_custom_schema(entity_name, data):
    if entity_name == "contracts":
        func = get_contracts_field_schema
    else:
        func = get_field_schema

    return {field['name']: func(field['type']) for field in data}


def get_custom_schema(request, entity_name):
    data = request(get_url(entity_name + "_properties")).json()
    return parse_custom_schema(entity_name, data)


def get_schema(request, entity_name):
    with open("schemas/{}.json".format(entity_name)) as f:
        schema = json.loads(f.read())

    if entity_name in ["contacts", "companies", "deals"]:
        custom_schema = get_custom_schema(request, entity_name)
        schema['properties']['properties'] = {
            "type": ["null", "object"],
            "properties": custom_schema,
        }

    return schema


def get_schemas(request):
    return {entity_name: get_schema(request, entity_name) for entity_name in entities}


def get_url(endpoint, **kwargs):
    """
    get_url('contacts_by_company', company_id=2) -> /companies/v2/companies/2/contacts
    """
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
            value = datetime.datetime.utcfromtimestamp(value * 0.001).isoformat()

    return value


def transform_record(record, schema):
    field_schemas = schema['properties']
    for field_name, field_schema in field_schemas.items():
        if field_name in record:
            value = transform_field(record[field_name], field_schema)
            record[field_name] = value

    return record


def get_apikey(config_file):
    with open(config_file) as f:
        config = json.load(f)

    refresh_token = config['refresh_token']
    data = {"grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "refresh_token": refresh_token}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post("https://api.hubapi.com/auth/v1/refresh", headers=headers, data=data).json()
    return resp['access_token']


def load_state(state_file):
    with open(state_file) as f:
        state = json.load(f)

    state.update(state)


def mk_request(apikey, auth_key="access_token"):
    session = requests.Session()

    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException),
                          max_tries=5,
                          giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                          factor=2)
    def request(url, params=None):
        _params = {auth_key: apikey}
        if params is not None:
            _params.update(params)

        response = session.get(url, params=_params)
        response.raise_for_status()
        return response

    return request


def do_check(request):
    try:
        request(get_url('contacts_recent'))
    except requests.exceptions.RequestException as e:
        logger.fatal("Error checking connection using {e.request.url}; "
                     "received status {e.response.status_code}: {e.response.test}".format(e=e))
        sys.exit(-1)


def sync_contacts(request):
    last_sync = dateutil.parser.parse(state['contacts'])
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "contacts_all"
    else:
        endpoint = "contacts_recent"

    schema = get_schema(request, 'contacts')
    stitchstream.write_schema('contacts', schema)

    all_records = []
    params = {'count': 100}
    has_more = True
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get('has-more', False)
        if has_more:
            params['vidOffset'] = data['vid-offset']

        vids = (r['canonical-vid'] for r in data['contacts'])
        resp = request(get_url('contacts_detail'), params={'vid': vids})
        transformed_records = [transform_record(record, schema) for record in resp.json().values()]
        all_records.extend(transformed_records)
        stitchstream.write_records('contacts', transformed_records)
        state['contacts'] = transformed_records[-1]['properties']['createdate']['value']
        stitchstream.write_state(state)

    return all_records


def sync_companies(request):
    last_sync = dateutil.parser.parse(state['companies'])
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "companies_all"
    else:
        endpoint = "companies_recent"

    schema = get_schema(request, 'companies')
    stitchstream.write_schema('companies', schema)

    all_records = []
    params = {'count': 250}
    has_more = True
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get('has-more', False)
        if has_more:
            params['offset'] = data['offset']

        transformed_records = []
        for record in data['companies']:
            resp = request(get_url('companies_detail', company_id=record['companyId']))
            transformed_records.append(transform_record(resp.json(), schema))

        all_records.extend(transformed_records)
        stitchstream.write_records('companies', transformed_records)
        state['companies'] = transformed_records[-1]['properties']['createdate']['value']
        stitchstream.write_state(state)

    return all_records


def sync_deals(request):
    last_sync = dateutil.parser.parse(state['deals'])
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "deals_all"
    else:
        endpoint = "deals_recent"

    schema = get_schema(request, 'deals')
    stitchstream.write_schema('deals', schema)

    all_records = []
    params = {'count': 250}
    has_more = True
    while has_more:
        resp = request(get_url(endpoint), params=params)
        data = resp.json()

        has_more = data.get('hasMore', False)
        if has_more:
            params['offset'] = data['offset']

        transformed_records = []
        for record in data['deals']:
            resp = request(get_url('deals_detail', deal_id=record['dealId']))
            transformed_records.append(transform_record(resp.json(), schema))

        all_records.extend(transformed_records)
        stitchstream.write_records('deals', transformed_records)
        state['deals'] = transformed_records[-1]['properties']['createdate']['value']
        stitchstream.write_state(state)

        has_more = False

    return all_records


def sync_contacts_by_company(request):
    pass


def sync_campaigns(request):
    pass


def sync_subscription_changes(request):
    pass


def sync_email_events(request):
    pass


def sync_contact_lists(request):
    pass


def sync_forms(request):
    pass


def sync_workflows(request):
    pass


def sync_keywords(request):
    pass


def sync_owners(request):
    pass


def do_sync(request):
    sync_contacts(request)
    sync_companies(request)
    sync_contacts_by_company(request)
    sync_deals(request)
    sync_campaigns(request)
    sync_subscription_changes(request)
    sync_email_events(request)
    sync_contact_lists(request)
    sync_forms(request)
    sync_workflows(request)
    sync_keywords(request)
    sync_owners(request)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('func', choices=['check', 'sync'])
    parser.add_argument('-c', '--config', help='Config file', required=True)
    parser.add_argument('-s', '--state', help='State file')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true',
                        help='Sets the log level to DEBUG (default INFO)')
    parser.set_defaults(debug=False)
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    configure_logging(level)

    if args.state:
        logger.info("Loading state from " + args.state)
        load_state(args.state)

    apikey = get_apikey(args.config)
    request = mk_request(apikey)

    if args.func == 'check':
        do_check(request)

    elif args.func == 'sync':
        do_sync(request)


if __name__ == '__main__':
    main()
