#!/usr/bin/env python3
import json
import singer
from singer import utils, metadata, Catalog, CatalogEntry, Schema
from tap_hubspot.stream import Stream
from pathlib import Path
from collections import defaultdict
from typing import DefaultDict, Set

STREAMS = {
    "email_events": {"bookmark_key": "created"},
    "forms": {"bookmark_key": "updatedAt"},
    "contacts": {"bookmark_key": "updatedAt",},
    "companies": {"bookmark_key": "updatedAt"},
    "deals": {"bookmark_key": "updatedAt"},
    "deal_pipelines": {"bookmark_key": "updatedAt"},
    "engagements": {"bookmark_key": "lastUpdated"},
    "submissions": {},
    "contacts_events": {"bookmark_key": "lastSynced"},
}
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "refresh_token",
    "redirect_uri",
]
LOGGER = singer.get_logger()


def sync(config, state=None):
    event_state: DefaultDict[Set, str] = defaultdict(set)

    for tap_stream_id, stream_config in STREAMS.items():
        LOGGER.info(f"syncing {tap_stream_id}")
        stream = Stream(
            config=config, tap_stream_id=tap_stream_id, stream_config=stream_config
        )
        state, event_state = stream.do_sync(state, event_state)


@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    sync(args.config, args.state)


if __name__ == "__main__":
    main()
