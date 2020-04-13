#!/usr/bin/env python3
import os
import sys
import json
import singer
from singer import utils, metadata, Catalog, CatalogEntry, Schema
from tap_hubspot.stream import Stream

KEY_PROPERTIES = "id"
STREAMS = {
    "email_events": {
        "valid_replication_keys": ["startTimestamp"],
        "key_properties": "id",
    },
    "forms": {"valid_replication_keys": ["updatedAt"], "key_properties": "guid",},
    "contacts": {
        "valid_replication_keys": ["versionTimestamp"],
        "key_properties": "vid",
    },
    "companies": {
        "valid_replication_keys": ["hs_lastmodifieddate"],
        "key_properties": "companyId",
    },
    "deals": {
        "valid_replication_keys": ["hs_lastmodifieddate"],
        "key_properties": "dealId",
    },
    "deal_pipelines": {
        "valid_replication_keys": ["updatedAt"],
        "key_properties": "pipelineId",
    },
    "engagements": {
        "valid_replication_keys": ["lastUpdated"],
        "key_properties": "engagement_id",
    },
}
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "refresh_token",
    "redirect_uri",
]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover() -> Catalog:
    schemas = load_schemas()
    streams = []

    for tap_stream_id, props in STREAMS.items():
        schema = schemas[tap_stream_id]
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=props.get("key_properties", None),
            valid_replication_keys=props.get("valid_replication_keys", []),
        )
        streams.append(
            CatalogEntry(
                stream=tap_stream_id,
                tap_stream_id=tap_stream_id,
                key_properties=KEY_PROPERTIES,
                schema=Schema.from_dict(schema),
                metadata=mdata,
            )
        )
    return Catalog(streams)


def sync(catalog, config, state=None):
    for catalog_entry in catalog.streams:
        if not catalog_entry.is_selected():
            continue
        LOGGER.info(f"syncing {catalog_entry.tap_stream_id}")
        stream = Stream(catalog_entry, config)
        stream.do_sync(state)


@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(catalog, args.config, args.state)


if __name__ == "__main__":
    main()
