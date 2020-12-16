#!/usr/bin/env python3
import json
import shelve
import os
import tempfile
import singer
from singer import utils, metadata, Catalog, CatalogEntry, Schema
from tap_hubspot.stream import Stream
from pathlib import Path
from collections import defaultdict
from typing import DefaultDict, Set

STREAMS = {
    "companies": {"bookmark_key": "updatedAt"},
    "owners": {"bookmark_key": "updatedAt"},
    "forms": {"bookmark_key": "updatedAt"},
    "contacts": {
        "bookmark_key": "updatedAt",
    },
    "contacts_events": {"bookmark_key": "lastSynced"},
    "deal_pipelines": {"bookmark_key": "updatedAt"},
    "engagements": {"bookmark_key": "lastUpdated"},
    "submissions": {},
    "email_events": {"bookmark_key": "created"},
    "deals": {"bookmark_key": "updatedAt"},
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
    with tempfile.TemporaryDirectory(
        prefix=f"{os.getcwd()}/temp_event_state_"
    ) as temp_dirname:
        event_state: DefaultDict[Set, str] = defaultdict(set)

        event_state["contacts_events_ids"] = shelve.open(
            f"{temp_dirname}/contacts_events_ids"
        )
        event_state["hs_calculated_form_submissions"] = shelve.open(
            f"{temp_dirname}/hs_calculated_form_submissions"
        )

        for tap_stream_id, stream_config in STREAMS.items():
            try:
                LOGGER.info(f"syncing {tap_stream_id}")
                stream = Stream(
                    config=config,
                    tap_stream_id=tap_stream_id,
                    stream_config=stream_config,
                )
                state, event_state = stream.do_sync(state, event_state)

            except Exception:
                LOGGER.exception(f"{tap_stream_id} failed")
                continue


@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    sync(args.config, args.state)


if __name__ == "__main__":
    main()
