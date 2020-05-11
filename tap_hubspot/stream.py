import singer
from singer import (
    metadata,
    CatalogEntry,
    Transformer,
    UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
)
from typing import Union
from datetime import timedelta, datetime
from dateutil import parser
from tap_hubspot.hubspot import Hubspot
import pytz

LOGGER = singer.get_logger()


class Stream:
    def __init__(self, catalog: CatalogEntry, config):
        self.tap_stream_id = catalog.tap_stream_id
        self.schema = catalog.schema.to_dict()
        self.key_properties = catalog.key_properties
        self.mdata = metadata.to_map(catalog.metadata)
        valid_replication_keys = self.mdata.get(()).get("valid-replication-keys")
        self.bookmark_key = (
            None if not valid_replication_keys else valid_replication_keys[0]
        )
        self.config = config
        self.hubspot = Hubspot(config, self.tap_stream_id)

    def get_properties(self):
        properties = []
        if self.mdata.get(("properties", "properties"), {}).get("selected"):
            additional_properties = (
                self.schema.get("properties").get("properties").get("properties")
            )
            properties = [key for key in additional_properties.keys()]
        return properties

    def do_sync(self, state):
        singer.write_schema(
            self.tap_stream_id, self.schema, self.key_properties,
        )
        prev_bookmark = None
        start_date, end_date = self.__get_start_end(state)
        with Transformer(UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING) as transformer:
            try:
                data = self.hubspot.streams(start_date, end_date, self.get_properties())
                for d, replication_value in data:
                    if replication_value and (
                        start_date >= replication_value or end_date <= replication_value
                    ):
                        continue

                    record = transformer.transform(d, self.schema, self.mdata)
                    singer.write_record(self.tap_stream_id, record)
                    if not replication_value:
                        continue

                    new_bookmark = replication_value
                    if not prev_bookmark:
                        prev_bookmark = new_bookmark

                    if prev_bookmark < new_bookmark:
                        state = self.__advance_bookmark(state, prev_bookmark)
                        prev_bookmark = new_bookmark

                return self.__advance_bookmark(state, prev_bookmark)

            except Exception:
                self.__advance_bookmark(state, prev_bookmark)
                raise

    def __get_start_end(self, state: dict):
        end_date = pytz.utc.localize(datetime.utcnow())
        LOGGER.info(f"sync data until: {end_date}")

        config_start_date = self.config.get("start_date")
        if config_start_date:
            config_start_date = parser.isoparse(config_start_date)
        else:
            config_start_date = datetime.utcnow() + timedelta(weeks=4)

        if not state:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        account_record = state["bookmarks"].get(self.tap_stream_id, None)
        if not account_record:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        current_bookmark = account_record.get(self.bookmark_key, None)
        if not current_bookmark:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        start_date = parser.isoparse(current_bookmark)
        LOGGER.info(f"using 'start_date' from previous state: {start_date}")
        return start_date, end_date

    def __advance_bookmark(self, state: dict, bookmark: Union[str, datetime, None]):
        if not bookmark:
            singer.write_state(state)
            return state

        if isinstance(bookmark, datetime):
            bookmark_datetime = bookmark
        elif isinstance(bookmark, str):
            bookmark_datetime = parser.isoparse(bookmark)
        else:
            raise ValueError(
                f"bookmark is of type {type(bookmark)} but must be either string or datetime"
            )

        state = singer.write_bookmark(
            state, self.tap_stream_id, self.bookmark_key, bookmark_datetime.isoformat()
        )
        singer.write_state(state)
        return state
