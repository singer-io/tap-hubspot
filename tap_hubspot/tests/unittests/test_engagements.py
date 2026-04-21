import json
import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

from tap_hubspot import sync_engagements


_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'schemas', 'engagements.json')
with open(_SCHEMA_PATH) as _f:
    ENGAGEMENTS_SCHEMA = json.load(_f)


def make_engagement(eid, last_updated_millis):
    return {
        "engagement": {
            "id": eid,
            "lastUpdated": last_updated_millis,
            "type": "NOTE"
        },
        "associations": {"contactIds": [], "companyIds": [], "dealIds": []},
        "attachments": [],
        "metadata": {"body": "test"}
    }


class TestSyncEngagements(unittest.TestCase):
    def make_ctx(self):
        mock_ctx = MagicMock()
        mock_ctx.get_catalog_from_id.return_value = {
            "metadata": [],
            "stream_alias": None
        }
        return mock_ctx

    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('singer.utils.strptime_to_utc')
    @patch('singer.utils.strftime')
    @patch('tap_hubspot.utils.now')
    @patch('tap_hubspot.get_current_sync_start', return_value=None)
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    @patch('tap_hubspot.load_schema', return_value=ENGAGEMENTS_SCHEMA)
    @patch('tap_hubspot.gen_request', return_value=[])
    def test_gen_request_called_with_correct_url_and_params(
        self, mock_gen_request, mock_load_schema, mock_get_start,
        mock_get_current_sync_start, mock_now, mock_strftime, mock_strptime_to_utc,
        mock_write_schema, mock_write_state
    ):
        state = {"currently_syncing": "engagements"}
        mock_now.return_value = datetime(2023, 12, 1, tzinfo=timezone.utc)
        mock_strftime.return_value = '2023-01-01T00:00:00.000000Z'
        mock_strptime_to_utc.return_value = datetime(2023, 1, 1, tzinfo=timezone.utc)

        sync_engagements(state, self.make_ctx())

        args = mock_gen_request.call_args[0]
        self.assertEqual(args[1], 'engagements')
        self.assertEqual(args[2], 'https://api.hubapi.com/engagements/v1/engagements/recent/modified')
        self.assertIn('count', args[3])
        self.assertEqual(args[3]['count'], 190)
        self.assertIn('since', args[3])
        self.assertEqual(args[4], 'results')
        self.assertEqual(args[5], 'hasMore')
        self.assertEqual(args[6], ['offset'])
        self.assertEqual(args[7], ['offset'])

    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('singer.utils.strptime_to_utc')
    @patch('singer.utils.strftime')
    @patch('tap_hubspot.utils.now')
    @patch('tap_hubspot.get_current_sync_start', return_value=None)
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    @patch('tap_hubspot.load_schema', return_value=ENGAGEMENTS_SCHEMA)
    @patch('tap_hubspot.gen_request', return_value=[])
    def test_engagements_page_size_config_option(
        self, mock_gen_request, mock_load_schema, mock_get_start,
        mock_get_current_sync_start, mock_now, mock_strftime, mock_strptime_to_utc,
        mock_write_schema, mock_write_state
    ):
        state = {"currently_syncing": "engagements"}
        mock_now.return_value = datetime(2023, 12, 1, tzinfo=timezone.utc)
        mock_strftime.return_value = '2023-01-01T00:00:00.000000Z'
        mock_strptime_to_utc.return_value = datetime(2023, 1, 1, tzinfo=timezone.utc)

        with patch.dict('tap_hubspot.CONFIG', {'engagements_page_size': 100}, clear=False):
            sync_engagements(state, self.make_ctx())

        args = mock_gen_request.call_args[0]
        self.assertEqual(args[3]['count'], 100)

    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('singer.utils.strptime_to_utc')
    @patch('singer.utils.strftime')
    @patch('tap_hubspot.utils.now')
    @patch('tap_hubspot.get_current_sync_start', return_value=None)
    @patch('tap_hubspot.get_start', return_value='2023-06-15T00:00:00.000000Z')
    @patch('tap_hubspot.load_schema', return_value=ENGAGEMENTS_SCHEMA)
    @patch('tap_hubspot.gen_request', return_value=[])
    def test_since_param_derived_from_bookmark(
        self, mock_gen_request, mock_load_schema, mock_get_start,
        mock_get_current_sync_start, mock_now, mock_strftime, mock_strptime_to_utc,
        mock_write_schema, mock_write_state
    ):
        state = {"currently_syncing": "engagements"}
        mock_now.return_value = datetime(2023, 12, 1, tzinfo=timezone.utc)
        mock_strftime.return_value = '2023-06-15T00:00:00.000000Z'
        mock_strptime_to_utc.return_value = datetime(2023, 6, 15, tzinfo=timezone.utc)

        sync_engagements(state, self.make_ctx())

        args = mock_gen_request.call_args[0]
        self.assertEqual(args[3]['since'], 1686787200000)

    @patch('singer.write_record')
    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.utils.now')
    @patch('tap_hubspot.get_current_sync_start', return_value=None)
    @patch('tap_hubspot.get_start', return_value='2023-06-01T00:00:00.000000Z')
    @patch('tap_hubspot.load_schema', return_value=ENGAGEMENTS_SCHEMA)
    @patch('tap_hubspot.gen_request')
    def test_records_before_bookmark_are_filtered_out(
        self, mock_gen_request, mock_load_schema, mock_get_start,
        mock_get_current_sync_start, mock_now, mock_write_schema,
        mock_write_state, mock_write_record
    ):
        state = {"currently_syncing": "engagements"}
        mock_now.return_value = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_gen_request.return_value = [
            make_engagement(1, 1672531200000),  # 2023-01-01
            make_engagement(2, 1696118400000),  # 2023-10-01
        ]

        sync_engagements(state, self.make_ctx())

        self.assertEqual(mock_write_record.call_count, 1)
        written_record = mock_write_record.call_args[0][1]
        self.assertEqual(written_record['engagement']['id'], 2)

    @patch('singer.write_record')
    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.utils.now')
    @patch('tap_hubspot.get_current_sync_start', return_value=None)
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    @patch('tap_hubspot.load_schema', return_value=ENGAGEMENTS_SCHEMA)
    @patch('tap_hubspot.gen_request')
    def test_bookmark_capped_at_sync_start(
        self, mock_gen_request, mock_load_schema, mock_get_start,
        mock_get_current_sync_start, mock_now, mock_write_schema,
        mock_write_state, mock_write_record
    ):
        state = {"currently_syncing": "engagements"}
        sync_start = datetime(2023, 9, 1, tzinfo=timezone.utc)
        mock_now.return_value = sync_start
        mock_gen_request.return_value = [
            make_engagement(1, 1696118400000),  # 2023-10-01
        ]

        result_state = sync_engagements(state, self.make_ctx())

        bk_value = result_state['bookmarks']['engagements']['lastUpdated']
        self.assertEqual(bk_value, '2023-09-01T00:00:00.000000Z')
        self.assertIsNone(result_state['bookmarks']['engagements']['current_sync_start'])

    @patch('singer.write_record')
    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.utils.now')
    @patch('tap_hubspot.get_current_sync_start', return_value=None)
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    @patch('tap_hubspot.load_schema', return_value=ENGAGEMENTS_SCHEMA)
    @patch('tap_hubspot.gen_request', return_value=[])
    def test_empty_result_set(
        self, mock_gen_request, mock_load_schema, mock_get_start,
        mock_get_current_sync_start, mock_now, mock_write_schema,
        mock_write_state, mock_write_record
    ):
        state = {"currently_syncing": "engagements"}
        mock_now.return_value = datetime(2024, 1, 1, tzinfo=timezone.utc)

        result_state = sync_engagements(state, self.make_ctx())

        mock_write_record.assert_not_called()
        bk_value = result_state['bookmarks']['engagements']['lastUpdated']
        self.assertEqual(bk_value, '2023-01-01T00:00:00.000000Z')

    @patch('singer.write_record')
    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.utils.now')
    @patch('tap_hubspot.get_current_sync_start', return_value=None)
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    @patch('tap_hubspot.load_schema', return_value=ENGAGEMENTS_SCHEMA)
    @patch('tap_hubspot.gen_request')
    def test_record_hoists_engagement_id_and_lastUpdated(
        self, mock_gen_request, mock_load_schema, mock_get_start,
        mock_get_current_sync_start, mock_now, mock_write_schema,
        mock_write_state, mock_write_record
    ):
        state = {"currently_syncing": "engagements"}
        mock_now.return_value = datetime(2024, 1, 1, tzinfo=timezone.utc)
        mock_gen_request.return_value = [
            make_engagement(42, 1696118400000),  # 2023-10-01
        ]

        sync_engagements(state, self.make_ctx())

        written_record = mock_write_record.call_args[0][1]
        self.assertEqual(written_record['engagement_id'], 42)
        self.assertIn('lastUpdated', written_record)
        self.assertEqual(
            written_record['lastUpdated'],
            written_record['engagement']['lastUpdated']
        )
