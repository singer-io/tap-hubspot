#!/usr/bin/env python3

import datetime
import itertools
import os
import re
import sys
import json

import attr
import backoff
import requests
import singer
import singer.metrics as metrics
from singer import utils
from singer import transform


LOGGER = singer.get_logger()
SESSION = requests.Session()
RUN_START = utils.strftime(datetime.datetime.utcnow())


class InvalidAuthException(Exception):
    pass

class SourceUnavailableException(Exception):
    pass

class DataFields:
    offset = 'offset'

class StateFields:
    offset = 'offset'
    this_stream = 'this_stream'

CHUNK_SIZES = {
    "email_events": 1000 * 60 * 60 * 24,
    "subscription_changes": 1000 * 60 * 60 * 24,
}

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

ENDPOINTS = {
    "contacts_properties":  "/properties/v1/contacts/properties",
    "contacts_all":         "/contacts/v1/lists/all/contacts/all",
    "contacts_recent":      "/contacts/v1/lists/recently_updated/contacts/recent",
    "contacts_detail":      "/contacts/v1/contact/vids/batch/",

    "companies_properties": "/companies/v2/properties",
    "companies_all":        "/companies/v2/companies/paged",
    "companies_recent":     "/companies/v2/companies/recent/modified",
    "companies_detail":     "/companies/v2/companies/{company_id}",

    "deals_properties":     "/properties/v1/deals/properties",
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


def xform(record, schema):
    return transform.transform(record, schema, transform.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING)


def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def get_url(endpoint, **kwargs):
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    return BASE_URL + ENDPOINTS[endpoint].format(**kwargs)


def get_field_type_schema(field_type):
    if field_type == "bool":
        return {"type": ["null", "boolean"]}

    elif field_type == "datetime":
        # valid unix milliseconds are not returned for this type,
        # so we have to just make these strings
        return {"type": ["null", "string"]}

    elif field_type == "number":
        # A value like 'N/A' can be returned for this type,
        # so we have to let this be a string sometimes
        return {"type": ["null", "number", "string"]}

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
    return {
        field['name']: get_field_schema(
            field['type'], entity_name != "contacts")
        for field in data
    }


def get_custom_schema(entity_name):
    return parse_custom_schema(entity_name, request(get_url(entity_name + "_properties")).json())


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity_name):
    schema = utils.load_json(get_abs_path('schemas/{}.json'.format(entity_name)))
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

    LOGGER.info("Refreshing token")
    resp = requests.post(BASE_URL + "/oauth/v1/token", data=payload)
    if resp.status_code == 403:
        raise InvalidAuthException(resp.content)

    resp.raise_for_status()
    auth = resp.json()
    CONFIG['access_token'] = auth['access_token']
    CONFIG['refresh_token'] = auth['refresh_token']
    CONFIG['token_expires'] = (
        datetime.datetime.utcnow() +
        datetime.timedelta(seconds=auth['expires_in'] - 600))
    LOGGER.info("Token refreshed. Expires at %s", CONFIG['token_expires'])


def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429

def on_giveup(details):
    if len(details['args']) == 2:
        url, params = details['args']
    else:
        url = details['args']
        params = {}

    raise Exception("Giving up on request after {} tries with url {} and params {}" \
                    .format(details['tries'], url, params))

URL_SOURCE_RE = re.compile(BASE_URL + r'/(\w+)/')

def parse_source_from_url(url):
    match = URL_SOURCE_RE.match(url)
    if match:
        return match.group(1)
    return None


@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=5,
                      giveup=giveup,
                      on_giveup=on_giveup,
                      factor=2)
@utils.ratelimit(9, 1)
def request(url, params=None):
    if CONFIG['token_expires'] is None or CONFIG['token_expires'] < datetime.datetime.utcnow():
        refresh_token()

    params = params or {}
    headers = {'Authorization': 'Bearer {}'.format(CONFIG['access_token'])}
    if 'user_agent' in CONFIG:
        headers['User-Agent'] = CONFIG['user_agent']

    req = requests.Request('GET', url, params=params, headers=headers).prepare()
    LOGGER.info("GET %s", req.url)
    with metrics.http_request_timer(parse_source_from_url(url)) as timer:
        resp = SESSION.send(req)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        if resp.status_code == 403:
            raise SourceUnavailableException(resp.content)
        else:
            resp.raise_for_status()

    return resp


def gen_request(url, params, path, more_key, offset_keys, offset_targets):
    if len(offset_keys) != len(offset_targets):
        raise ValueError("Number of offset_keys must match number of offset_targets")

    if STATE.get(StateFields.offset):
        params.update(STATE[StateFields.offset])

    with metrics.record_counter(parse_source_from_url(url)) as counter:
        while True:
            data = request(url, params).json()
            for row in data[path]:
                counter.increment()
                yield row

            if not data.get(more_key, False):
                break

            STATE[StateFields.offset] = {}
            for key, target in zip(offset_keys, offset_targets):
                if key in data:
                    params[target] = data[key]
                    STATE[StateFields.offset][target] = data[key]

            singer.write_state(STATE)

    STATE.pop(StateFields.offset, None)
    singer.write_state(STATE)


def _sync_contact_vids(vids, schema):
    if len(vids) == 0:
        return

    data = request(get_url("contacts_detail"), params={'vid': vids}).json()
    for _, record in data.items():
        record = xform(record, schema)
        singer.write_record("contacts", record)


def sync_contacts():
    now = datetime.datetime.utcnow()
    last_sync = utils.strptime(get_start("contacts"))
    days_since_sync = (now - last_sync).days
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
            modified_time = utils.strptime(
                transform._transform_datetime( # pylint: disable=protected-access
                    row['properties']['lastmodifieddate']['value'],
                    transform.UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING))

        if not modified_time or modified_time >= last_sync:
            vids.append(row['vid'])

        if len(vids) == 100:
            _sync_contact_vids(vids, schema)
            vids = []

    _sync_contact_vids(vids, schema)
    STATE["contacts"] = RUN_START
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

    for row in gen_request(url, params, path, more_key, offset_keys, offset_targets):
        record = request(get_url("companies_detail", company_id=row['companyId'])).json()
        record = xform(record, schema)

        modified_time = None
        if 'hs_lastmodifieddate' in record:
            modified_time = utils.strptime(record['hs_lastmodifieddate']['value'])
        elif 'createdate' in record:
            modified_time = utils.strptime(record['createdate']['value'])

        if not modified_time or modified_time >= last_sync:
            singer.write_record("companies", record)

    STATE["companies"] = RUN_START
    singer.write_state(STATE)


def sync_deals():
    last_sync = utils.strptime(get_start("deals"))
    days_since_sync = (datetime.datetime.utcnow() - last_sync).days
    if days_since_sync > 30:
        endpoint = "deals_all"
        path = "deals"
    else:
        endpoint = "deals_recent"
        path = "results"

    schema = load_schema("deals")
    singer.write_schema("deals", schema, ["portalId", "dealId"])

    url = get_url(endpoint)
    params = {'count': 250}

    for row in gen_request(url, params, path, "hasMore", ["offset"], ["offset"]):
        if STATE.get(StateFields.offset) == 10000:
            STATE.pop(StateFields.offset, None)

        record = request(get_url("deals_detail", deal_id=row['dealId'])).json()
        record = xform(record, schema)

        modified_time = None
        if 'hs_lastmodifieddate' in record:
            modified_time = utils.strptime(record['hs_lastmodifieddate']['value'])
        elif 'createdate' in record:
            modified_time = utils.strptime(record['createdate']['value'])

        if not modified_time or modified_time >= last_sync:
            singer.write_record("deals", record)

    STATE["deals"] = RUN_START
    singer.write_state(STATE)


def sync_campaigns():
    schema = load_schema("campaigns")
    singer.write_schema("campaigns", schema, ["id"])

    url = get_url("campaigns_all")
    params = {'limit': 500}

    for row in gen_request(url, params, "campaigns", "hasMore", ["offset"], ["offset"]):
        record = request(get_url("campaigns_detail", campaign_id=row['id'])).json()
        record = xform(record, schema)
        singer.write_record("campaigns", record)

    STATE["campaigns"] = RUN_START
    singer.write_state(STATE)


def sync_entity_chunked(entity_name, key_properties, path):
    schema = load_schema(entity_name)
    singer.write_schema(entity_name, schema, key_properties)

    start = get_start(entity_name)
    now_ts = int(datetime.datetime.utcnow().timestamp() * 1000)
    start_ts = int(utils.strptime(start).timestamp() * 1000)

    url = get_url(entity_name)

    with metrics.record_counter(entity_name) as counter:
        while start_ts < now_ts:
            end_ts = start_ts + CHUNK_SIZES[entity_name]
            params = {
                'startTimestamp': start_ts,
                'endTimestamp': end_ts,
                'limit': 1000,
            }

            while True:
                if STATE.get(StateFields.offset):
                    params[StateFields.offset] = STATE[StateFields.offset]
                data = request(url, params).json()
                for row in data[path]:
                    counter.increment()
                    singer.write_record(entity_name, xform(row, schema))
                if data.get('hasMore'):
                    STATE[StateFields.offset] = data[DataFields.offset]
                    LOGGER.info('Got more %s', STATE)
                    singer.write_state(STATE)
                else:
                    break

            utils.update_state(STATE, entity_name, datetime.datetime.utcfromtimestamp(end_ts / 1000)) # pylint: disable=line-too-long
            singer.write_state(STATE)
            start_ts = end_ts

    STATE.pop(StateFields.offset, None)
    singer.write_state(STATE)


def sync_subscription_changes():
    sync_entity_chunked("subscription_changes", ["timestamp", "portalId", "recipient"], "timeline")


def sync_email_events():
    sync_entity_chunked("email_events", ["id"], "events")


def sync_contact_lists():
    schema = load_schema("contact_lists")
    singer.write_schema("contact_lists", schema, ["internalListId"])

    url = get_url("contact_lists")
    params = {'count': 250}
    for row in gen_request(url, params, "lists", "has-more", ["offset"], ["offset"]):
        record = xform(row, schema)
        singer.write_record("contact_lists", record)

    STATE["contact_lists"] = RUN_START
    singer.write_state(STATE)


def sync_forms():
    schema = load_schema("forms")
    singer.write_schema("forms", schema, ["guid"])
    start = get_start("forms")

    data = request(get_url("forms")).json()
    for row in data:
        record = xform(row, schema)
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
        record = xform(row, schema)
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
        record = xform(row, schema)
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
        record = xform(row, schema)
        if record['updatedAt'] >= start:
            singer.write_record("owners", record)
            utils.update_state(STATE, "owners", record['updatedAt'])

    singer.write_state(STATE)

@attr.s
class Stream(object):
    name = attr.ib()
    sync = attr.ib()
    key_properties = attr.ib()

STREAMS = [
    # Do these first as they are incremental
    Stream('subscription_changes', sync_subscription_changes,
           ["timestamp", "portalId", "recipient"]),
    Stream('email_events', sync_email_events, ["id"]),

    # Do these last as they are full table
    Stream('forms', sync_forms, ["guid"]),
    Stream('workflows', sync_workflows, ["id"]),
    Stream('keywords', sync_keywords, ["keyword_guid"]),
    Stream('owners', sync_owners, ["portalId", "ownerId"]),
    Stream('campaigns', sync_campaigns, ["id"]),
    Stream('contact_lists', sync_contact_lists, ["internalListId"]),
    Stream('contacts', sync_contacts, ["canonical-vid"]),
    Stream('companies', sync_companies, ["companyId"]),
    Stream('deals', sync_deals, ["portalId", "dealId"])
]

def get_streams_to_sync(streams, state):
    target_stream = state.get(StateFields.this_stream)
    result = streams
    if target_stream:
        result = list(itertools.dropwhile(
            lambda x: x.name != target_stream, streams))
    if not result:
        raise Exception('Unknown stream {} in state'.format(target_stream))
    return result


def get_selected_streams(streams, annotated_schema):
    selected_streams = []
    for stream in annotated_schema['streams']:
        schema = stream.get('schema')
        name = stream.get('stream')
        if schema.get('selected'):
            selected_stream = next((s for s in streams if s.name == name), None)
            if selected_stream:
                selected_streams.append(selected_stream)
    return selected_streams


def do_sync(annotated_schema):
    streams = get_streams_to_sync(STREAMS, STATE)
    streams = get_selected_streams(streams, annotated_schema)
    LOGGER.info('Starting sync. Will sync these streams: %s',
                [stream.name for stream in streams])
    for stream in streams:
        LOGGER.info('Syncing %s', stream.name)
        STATE[StateFields.this_stream] = stream.name
        singer.write_state(STATE)

        try:
            stream.sync() # pylint: disable=not-callable
        except SourceUnavailableException:
            pass

    STATE[StateFields.this_stream] = None
    singer.write_state(STATE)
    LOGGER.info("Sync completed")


def load_discovered_schema(stream):
    schema = load_schema(stream.name)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema


def discover_schemas():
    result = {'streams': []}
    for stream in STREAMS:
        LOGGER.info('Loading schema for %s', stream.name)
        result['streams'].append({'stream': stream.name,
                                  'tap_stream_id': stream.name,
                                  'schema': load_discovered_schema(stream)})
    return result


def do_discover():
    LOGGER.info('Loading schemas')
    json.dump(discover_schemas(), sys.stdout, indent=4)


def main():
    args = utils.parse_args(
        ["redirect_uri",
         "client_id",
         "client_secret",
         "refresh_token",
         "start_date"])

    CONFIG.update(args.config)

    if args.state:
        STATE.update(args.state)

    if args.discover:
        do_discover()
    elif args.properties:
        do_sync(args.properties)
    else:
        LOGGER.info("No properties were selected")


if __name__ == '__main__':
    main()
