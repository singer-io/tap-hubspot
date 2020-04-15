#!/usr/bin/env python3
import json
import singer
from singer import utils, metadata, Catalog, CatalogEntry, Schema
from tap_hubspot.stream import Stream
from pathlib import Path

KEY_PROPERTIES = "id"
STREAMS = {
    "email_events": {"valid_replication_keys": ["created"], "key_properties": "id",},
    "forms": {"valid_replication_keys": ["updatedAt"], "key_properties": "guid",},
    "contacts": {
        "valid_replication_keys": ["lastmodifieddate"],
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


def load_schemas():
    schemas = {}
    schemas_path = Path(__file__).parent.absolute() / "schemas"
    for schema_path in schemas_path.iterdir():
        stream_name = schema_path.stem
        schemas[stream_name] = json.loads(schema_path.read_text())

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
