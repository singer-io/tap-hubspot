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
import singer.messages
import singer.metrics as metrics
from singer import utils
from singer import (transform,
                    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    Transformer, _transform_datetime)

LOGGER = singer.get_logger()
SESSION = requests.Session()

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


ENDPOINTS = {
    "contacts_properties":  "/properties/v1/contacts/properties",
    "contacts_all":         "/contacts/v1/lists/all/contacts/all",
    "contacts_recent":      "/contacts/v1/lists/recently_updated/contacts/recent",
    "contacts_detail":      "/contacts/v1/contact/vids/batch/",

    "companies_properties": "/companies/v2/properties",
    "companies_all":        "/companies/v2/companies/paged",
    "companies_recent":     "/companies/v2/companies/recent/modified",
    "companies_detail":     "/companies/v2/companies/{company_id}",
    "contacts_by_company":  "/companies/v2/companies/{company_id}/vids",

    "deals_properties":     "/properties/v1/deals/properties",
    "deals_all":            "/deals/v1/deal/paged",
    "deals_recent":         "/deals/v1/deal/recent/modified",
    "deals_detail":         "/deals/v1/deal/{deal_id}",

    "campaigns_all":        "/email/public/v1/campaigns/by-id",
    "campaigns_detail":     "/email/public/v1/campaigns/{campaign_id}",

    "engagements_all":        "/engagements/v1/engagements/paged",

    "subscription_changes": "/email/public/v1/subscriptions/timeline",
    "email_events":         "/email/public/v1/events",
    "contact_lists":        "/contacts/v1/lists",
    "forms":                "/forms/v2/forms",
    "workflows":            "/automation/v3/workflows",
    "keywords":             "/keywords/v1/keywords",
    "owners":               "/owners/v2/owners",
}

def get_start(state, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(state, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        return CONFIG['start_date']
    return current_bookmark

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

def load_associated_company_schema():
    associated_company_schema = load_schema("companies")
    #pylint: disable=line-too-long
    associated_company_schema['properties']['company-id'] = associated_company_schema['properties'].pop('companyId')
    associated_company_schema['properties']['portal-id'] = associated_company_schema['properties'].pop('portalId')
    return associated_company_schema

def load_schema(entity_name):
    schema = utils.load_json(get_abs_path('schemas/{}.json'.format(entity_name)))
    if entity_name in ["contacts", "companies", "deals"]:
        custom_schema = get_custom_schema(entity_name)
        schema['properties']['properties'] = {
            "type": "object",
            "properties": custom_schema,
        }

    if entity_name == "contacts":
        schema['properties']['associated-company'] = load_associated_company_schema()

    return schema

#pylint: disable=invalid-name
def acquire_access_token_from_refresh_token():
    payload = {
        "grant_type": "refresh_token",
        "redirect_uri": CONFIG['redirect_uri'],
        "refresh_token": CONFIG['refresh_token'],
        "client_id": CONFIG['client_id'],
        "client_secret": CONFIG['client_secret'],
    }


    resp = requests.post(BASE_URL + "/oauth/v1/token", data=payload)
    LOGGER.info(resp.content)
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
        acquire_access_token_from_refresh_token()

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
# {"bookmarks" : {"contacts" : { "lastmodifieddate" : "2001-01-01"
#                                "offset" : {"vidOffset": 1234
#                                           "timeOffset": "3434434 }}
#                 "users" : { "timestamp" : "2001-01-01"}}
#  "currently_syncing" : "contacts"
# }
# }

#pylint: disable=line-too-long
def gen_request(STATE, tap_stream_id, url, params, path, more_key, offset_keys, offset_targets):
    if len(offset_keys) != len(offset_targets):
        raise ValueError("Number of offset_keys must match number of offset_targets")

    if singer.get_offset(STATE, tap_stream_id):
        params.update(singer.get_offset(STATE, tap_stream_id))

    with metrics.record_counter(parse_source_from_url(url)) as counter:
        while True:
            data = request(url, params).json()

            for row in data[path]:
                counter.increment()
                yield row

            if not data.get(more_key, False):
                break

            STATE = singer.clear_offset(STATE, tap_stream_id)
            for key, target in zip(offset_keys, offset_targets):
                if key in data:
                    params[target] = data[key]
                    STATE = singer.set_offset(STATE, tap_stream_id, target, data[key])

            singer.write_state(STATE)

    STATE = singer.clear_offset(STATE, tap_stream_id)
    singer.write_state(STATE)


def _sync_contact_vids(catalog, vids, schema, bumble_bee):
    if len(vids) == 0:
        return

    data = request(get_url("contacts_detail"), params={'vid': vids, 'showListMemberships' : True}).json()
    for _, record in data.items():
        record = bumble_bee.transform(record, schema)
        singer.write_record("contacts", record, catalog.get('stream_alias'))

default_contact_params = {
    'showListMemberships': True,
    'count': 100,
}

def sync_contacts(STATE, catalog):
    last_sync = utils.strptime(get_start(STATE, "contacts", 'lastmodifieddate'))
    LOGGER.info("sync_contacts from %s", last_sync)

    schema = load_schema("contacts")

    singer.write_schema("contacts", schema, ["vid"], catalog.get('stream_alias'))

    url = get_url("contacts_all")

    vids = []
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in gen_request(STATE, 'contacts', url, default_contact_params, 'contacts', 'has-more', ['vid-offset'], ['vidOffset']):
            modified_time = None
            if 'lastmodifieddate' in row['properties']:
                modified_time = utils.strptime(
                    _transform_datetime( # pylint: disable=protected-access
                        row['properties']['lastmodifieddate']['value'],
                        UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING))

            if not modified_time or modified_time >= last_sync:
                vids.append(row['vid'])

            if modified_time and modified_time >= last_sync:
                STATE = singer.write_bookmark(STATE, 'contacts', 'lastmodifieddate', utils.strftime(modified_time))

            if len(vids) == 100:
                _sync_contact_vids(catalog, vids, schema, bumble_bee)
                singer.write_state(STATE)
                vids = []

        _sync_contact_vids(catalog, vids, schema, bumble_bee)

    singer.write_state(STATE)
    return STATE

class ValidationPredFailed(Exception):
    pass

# companies_recent only supports 10,000 results. If there are more than this,
# we'll need to use the companies_all endpoint
def use_recent_companies_endpoint(response):
    return response["total"] < 10000

default_contacts_by_company_params = {'count' : 100}

# NB> to do: support stream aliasing and field selection
def _sync_contacts_by_company(STATE, company_id):
    schema = load_schema('hubspot_contacts_by_company')
    singer.write_schema("hubspot_contacts_by_company", schema, ["company-id", "contact-id"])

    url = get_url("contacts_by_company", company_id=company_id)
    path = 'vids'
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for vid in gen_request(STATE, 'contacts_by_company', url, default_contacts_by_company_params, path, 'hasMore', ['vidOffset'], ['vidOffset']):
            record = {'company-id' : company_id,
                      'contact-id' : vid}
            record = bumble_bee.transform(record, schema)
            singer.write_record("hubspot_contacts_by_company", record)

    return STATE

default_company_params = {
    'limit': 250, 'properties': ["createdate", "hs_lastmodifieddate"]
}

def sync_companies(STATE, catalog):
    bumble_bee = Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING)
    last_sync = utils.strptime(get_start(STATE, "companies", 'hs_lastmodifieddate'))

    LOGGER.info("sync_companies from %s", last_sync)
    schema = load_schema('companies')
    singer.write_schema("companies", schema, ["companyId"], catalog.get('stream_alias'))

    url = get_url("companies_all")
    most_recent_modified_time = last_sync

    with bumble_bee:
        for row in gen_request(STATE, 'companies', url, default_company_params, 'companies', 'has-more', ['offset'], ['offset']):
            row_properties = row['properties']
            modified_time = None
            if 'hs_lastmodifieddate' in row_properties:
                # Hubspot returns timestamps in millis
                timestamp_millis = row_properties['hs_lastmodifieddate']['timestamp'] / 1000.0
                modified_time = datetime.datetime.fromtimestamp(timestamp_millis)
            elif 'createdate' in row_properties:
                # Hubspot returns timestamps in millis
                timestamp_millis = row_properties['createdate']['timestamp'] / 1000.0
                modified_time = datetime.datetime.fromtimestamp(timestamp_millis)

            if not modified_time or modified_time >= last_sync:
                record = request(get_url("companies_detail", company_id=row['companyId'])).json()
                record = bumble_bee.transform(record, schema)
                singer.write_record("companies", record, catalog.get('stream_alias'))
                if (modified_time and modified_time > most_recent_modified_time):
                    most_recent_modified_time = modified_time
                STATE = _sync_contacts_by_company(STATE, record['companyId'])

    STATE = singer.write_bookmark(STATE, 'companies', 'hs_lastmodifieddate', utils.strftime(most_recent_modified_time))
    singer.write_state(STATE)
    return STATE

def sync_deals(STATE, catalog):
    last_sync = utils.strptime(get_start(STATE, "deals", 'hs_lastmodifieddate'))
    LOGGER.info("sync_deals from %s", last_sync)
    most_recent_modified_time = last_sync
    params = {'count': 250,
              'properties' : []}

    schema = load_schema("deals")
    singer.write_schema("deals", schema, ["dealId"], catalog.get('stream_alias'))

    # Append all the properties fields for deals to the request
    additional_properties = schema.get("properties").get("properties").get("properties")
    for key in additional_properties.keys():
        params['properties'].append(key)

    url = get_url('deals_all')
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in gen_request(STATE, 'deals', url, params, 'deals', "hasMore", ["offset"], ["offset"]):
            row_properties = row['properties']
            modified_time = None
            if 'hs_lastmodifieddate' in row_properties:
                # Hubspot returns timestamps in millis
                timestamp_millis = row_properties['hs_lastmodifieddate']['timestamp'] / 1000.0
                modified_time = datetime.datetime.fromtimestamp(timestamp_millis)
            elif 'createdate' in row_properties:
                # Hubspot returns timestamps in millis
                timestamp_millis = row_properties['createdate']['timestamp'] / 1000.0
                modified_time = datetime.datetime.fromtimestamp(timestamp_millis)
            if not modified_time or modified_time >= last_sync:
                record = bumble_bee.transform(row, schema)
                singer.write_record("deals", record, catalog.get('stream_alias'))
                if (modified_time and modified_time > most_recent_modified_time):
                    most_recent_modified_time = modified_time

    STATE = singer.write_bookmark(STATE, 'deals', 'hs_lastmodifieddate', utils.strftime(most_recent_modified_time))
    singer.write_state(STATE)
    return STATE

#NB> no suitable bookmark is available: https://developers.hubspot.com/docs/methods/email/get_campaigns_by_id
def sync_campaigns(STATE, catalog):
    schema = load_schema("campaigns")
    singer.write_schema("campaigns", schema, ["id"], catalog.get('stream_alias'))
    LOGGER.info("sync_campaigns(NO bookmarks)")
    url = get_url("campaigns_all")
    params = {'limit': 500}

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in gen_request(STATE, 'campaigns', url, params, "campaigns", "hasMore", ["offset"], ["offset"]):
            record = request(get_url("campaigns_detail", campaign_id=row['id'])).json()
            record = bumble_bee.transform(record, schema)
            singer.write_record("campaigns", record, catalog.get('stream_alias'))

    return STATE


def sync_entity_chunked(STATE, catalog, entity_name, key_properties, path):
    schema = load_schema(entity_name)
    singer.write_schema(entity_name, schema, key_properties, catalog.get('stream_alias'))

    start = get_start(STATE, entity_name, 'startTimestamp')
    LOGGER.info("sync_%s from %s", entity_name, start)

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
            with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
                while True:
                    our_offset = singer.get_offset(STATE, entity_name)
                    if bool(our_offset) and our_offset.get('offset') != None:
                        params[StateFields.offset] = our_offset.get('offset')

                    data = request(url, params).json()
                    for row in data[path]:
                        counter.increment()
                        record = bumble_bee.transform(row, schema)
                        singer.write_record(entity_name, record,
                                            catalog.get('stream_alias'))
                    if data.get('hasMore'):
                        STATE = singer.set_offset(STATE, entity_name, 'offset', data['offset'])
                        singer.write_state(STATE)
                    else:
                        STATE = singer.clear_offset(STATE, entity_name)
                        singer.write_state(STATE)
                        break

            STATE = singer.write_bookmark(STATE, entity_name, 'startTimestamp', utils.strftime(datetime.datetime.utcfromtimestamp(end_ts / 1000))) # pylint: disable=line-too-long
            singer.write_state(STATE)
            start_ts = end_ts

    STATE = singer.clear_offset(STATE, entity_name)
    singer.write_state(STATE)
    return STATE

def sync_subscription_changes(STATE, catalog):
    STATE = sync_entity_chunked(STATE, catalog, "subscription_changes", ["timestamp", "portalId", "recipient"],
                                "timeline")
    return STATE

def sync_email_events(STATE, catalog):
    STATE = sync_entity_chunked(STATE, catalog, "email_events", ["id"], "events")
    return STATE

def sync_contact_lists(STATE, catalog):
    schema = load_schema("contact_lists")
    singer.write_schema("contact_lists", schema, ["listId"], catalog.get('stream_alias'))

    start = get_start(STATE, "contact_lists", 'updatedAt')
    LOGGER.info("sync_contact_lists from %s", start)

    url = get_url("contact_lists")
    params = {'count': 250}
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in gen_request(STATE, 'contact_lists', url, params, "lists", "has-more", ["offset"], ["offset"]):
            record = bumble_bee.transform(row, schema)
            if record['updatedAt'] >= start:
                singer.write_record("contact_lists", record, catalog.get('stream_alias'))
                STATE = singer.write_bookmark(STATE, 'contact_lists', 'updatedAt', record['updatedAt'])
                singer.write_state(STATE)

    return STATE

def sync_forms(STATE, catalog):
    schema = load_schema("forms")
    singer.write_schema("forms", schema, ["guid"], catalog.get('stream_alias'))
    start = get_start(STATE, "forms", 'updatedAt')

    LOGGER.info("sync_forms from %s", start)

    data = request(get_url("forms")).json()
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in data:
            record = bumble_bee.transform(row, schema)
            if record['updatedAt'] >= start:
                singer.write_record("forms", record, catalog.get('stream_alias'))
                STATE = singer.write_bookmark(STATE, 'forms', 'updatedAt', record['updatedAt'])
                singer.write_state(STATE)

    return STATE

def sync_workflows(STATE, catalog):
    schema = load_schema("workflows")
    singer.write_schema("workflows", schema, ["id"], catalog.get('stream_alias'))
    start = get_start(STATE, "workflows", 'updatedAt')

    STATE = singer.write_bookmark(STATE, 'workflows', 'updatedAt', start)
    singer.write_state(STATE)

    LOGGER.info("sync_workflows from %s", start)

    data = request(get_url("workflows")).json()
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in data['workflows']:
            record = bumble_bee.transform(row, schema)
            if record['updatedAt'] >= start:
                singer.write_record("workflows", record, catalog.get('stream_alias'))
                STATE = singer.write_bookmark(STATE, 'workflows', 'updatedAt', record['updatedAt'])
                singer.write_state(STATE)

    return STATE

def sync_keywords(STATE, catalog):
    schema = load_schema("keywords")
    singer.write_schema("keywords", schema, ["keyword_guid"], catalog.get('stream_alias'))
    start = get_start(STATE, "keywords", 'created_at')

    STATE = singer.write_bookmark(STATE, 'keywords', 'created_at', start)
    singer.write_state(STATE)

    LOGGER.info("sync_keywords from %s", start)
    data = request(get_url("keywords")).json()
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in data['keywords']:
            record = bumble_bee.transform(row, schema)
            if record['created_at'] >= start:
                singer.write_record("keywords", record, catalog.get('stream_alias'))
                STATE = singer.write_bookmark(STATE, 'keywords', 'created_at', record['created_at'])
                singer.write_state(STATE)

    return STATE

def sync_owners(STATE, catalog):
    schema = load_schema("owners")
    singer.write_schema("owners", schema, ["ownerId"], catalog.get('stream_alias'))
    start = get_start(STATE, "owners", 'updatedAt')

    LOGGER.info("sync_owners from %s", start)
    data = request(get_url("owners")).json()
    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for row in data:
            record = bumble_bee.transform(row, schema)
            if record['updatedAt'] >= start:
                singer.write_record("owners", record, catalog.get('stream_alias'))
                STATE = singer.write_bookmark(STATE, 'owners', 'updatedAt', record['updatedAt'])
                singer.write_state(STATE)

    return STATE

def sync_engagements(STATE, catalog):
    schema = load_schema("engagements")
    singer.write_schema("engagements", schema, ["engagement_id"], catalog.get('stream_alias'))
    start = get_start(STATE, "engagements", 'lastUpdated')
    LOGGER.info("sync_engagements from %s", start)

    STATE = singer.write_bookmark(STATE, 'engagements', 'lastUpdated', start)
    singer.write_state(STATE)

    url = get_url("engagements_all")
    params = {'limit': 250}
    top_level_key = "results"
    engagements = gen_request(STATE, 'engagements', url, params, top_level_key, "hasMore", ["offset"], ["offset"])

    with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as bumble_bee:
        for engagement in engagements:
            record = bumble_bee.transform(engagement, schema)
            if record['engagement']['lastUpdated'] >= start:
                record['engagement_id'] = record['engagement']['id']
                singer.write_record("engagements", record, catalog.get('stream_alias'))
                STATE = singer.write_bookmark(STATE, 'engagements', 'lastUpdated', record['engagement']['lastUpdated'])
                singer.write_state(STATE)

    return STATE

@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    sync = attr.ib()

STREAMS = [
    # Do these first as they are incremental
    Stream('subscription_changes', sync_subscription_changes),
    Stream('email_events', sync_email_events),

    # Do these last as they are full table
    Stream('forms', sync_forms),
    Stream('workflows', sync_workflows),
    Stream('keywords', sync_keywords),
    Stream('owners', sync_owners),
    Stream('campaigns', sync_campaigns),
    Stream('contact_lists', sync_contact_lists),
    Stream('contacts', sync_contacts),
    Stream('companies', sync_companies),
    Stream('deals', sync_deals),
    Stream('engagements', sync_engagements)
]

def get_streams_to_sync(streams, state):
    target_stream = singer.get_currently_syncing(state)
    result = streams
    if target_stream:
        result = list(itertools.dropwhile(
            lambda x: x.tap_stream_id != target_stream, streams))
    if not result:
        raise Exception('Unknown stream {} in state'.format(target_stream))
    return result


def get_selected_streams(remaining_streams, annotated_schema):
    selected_streams = []

    for stream in remaining_streams:
        tap_stream_id = stream.tap_stream_id
        selected_stream = next((s for s in annotated_schema['streams'] if s['tap_stream_id'] == tap_stream_id), None)
        if selected_stream and selected_stream.get('schema').get('selected'):
            selected_streams.append(stream)

    return selected_streams


def do_sync(STATE, catalogs):
    remaining_streams = get_streams_to_sync(STREAMS, STATE)
    selected_streams = get_selected_streams(remaining_streams, catalogs)
    LOGGER.info('Starting sync. Will sync these streams: %s',
                [stream.tap_stream_id for stream in selected_streams])
    for stream in selected_streams:
        LOGGER.info('Syncing %s', stream.tap_stream_id)
        STATE = singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            catalog = [c for c in catalogs.get('streams')
                       if c.get('stream') == stream.tap_stream_id][0]
            STATE = stream.sync(STATE, catalog) # pylint: disable=not-callable
        except SourceUnavailableException:
            pass

    STATE = singer.set_currently_syncing(STATE, None)
    singer.write_state(STATE)
    LOGGER.info("Sync completed")


def load_discovered_schema(stream):
    schema = load_schema(stream.tap_stream_id)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema

def discover_schemas():
    result = {'streams': []}
    for stream in STREAMS:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        result['streams'].append({'stream': stream.tap_stream_id,
                                  'tap_stream_id': stream.tap_stream_id,
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
    STATE = {}

    if args.state:
        STATE.update(args.state)

    if args.discover:
        do_discover()
    elif args.properties:
        do_sync(STATE, args.properties)
    else:
        LOGGER.info("No properties were selected")


if __name__ == '__main__':
    main()
