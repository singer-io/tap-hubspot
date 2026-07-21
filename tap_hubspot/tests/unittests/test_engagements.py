import copy
import json
import os
import unittest
from unittest.mock import patch, MagicMock

import singer

from tap_hubspot import sync_engagements


_SCHEMA_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'schemas', 'engagements.json')
with open(_SCHEMA_PATH) as _f:
    ENGAGEMENTS_SCHEMA = json.load(_f)


def make_response(results, has_more=False, after=None):
    resp = MagicMock()
    body = {"results": results, "hasMore": has_more}
    if after is not None:
        body["after"] = after
    resp.json.return_value = body
    return resp


class TestSyncEngagements(unittest.TestCase):
    def setUp(self):
        patcher = patch('tap_hubspot.load_schema', return_value=ENGAGEMENTS_SCHEMA)
        self.mock_load_schema = patcher.start()
        self.addCleanup(patcher.stop)

    def make_ctx(self):
        mock_ctx = MagicMock()
        mock_ctx.get_catalog_from_id.return_value = {
            "metadata": [],
            "stream_alias": None
        }
        return mock_ctx

    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.request')
    @patch('tap_hubspot.get_start', return_value='2023-06-15T00:00:00.000000Z')
    def test_modified_after_request_starts_from_last_updated_bookmark(
        self, mock_get_start, mock_request, mock_write_schema, mock_write_state
    ):
        mock_request.return_value = make_response([])
        state = {"bookmarks": {"engagements": {"lastUpdated": "2023-06-15T00:00:00.000000Z"}}}

        sync_engagements(state, self.make_ctx())

        call_params = mock_request.call_args[0][1]
        expected_millis = 1686787200000
        self.assertEqual(call_params['after'], expected_millis)

    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.request')
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    def test_modified_after_request_resumes_from_cursor_bookmark(
        self, mock_get_start, mock_request, mock_write_schema, mock_write_state
    ):
        mock_request.return_value = make_response([])
        cursor = "opaque-cursor-abc123"
        state = {"bookmarks": {"engagements": {"cursor": cursor}}}

        sync_engagements(state, self.make_ctx())

        call_params = mock_request.call_args[0][1]
        self.assertEqual(call_params['after'], cursor)

    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.request')
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    def test_engagements_page_size_maps_to_delta_limit(
        self, mock_get_start, mock_request, mock_write_schema, mock_write_state
    ):
        mock_request.return_value = make_response([])
        state = {"bookmarks": {"engagements": {}}}

        with patch.dict('tap_hubspot.CONFIG', {'engagements_page_size': 50}):
            sync_engagements(state, self.make_ctx())

        call_params = mock_request.call_args[0][1]
        self.assertEqual(call_params['limit'], 50)

    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.request')
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    def test_inflight_offset_takes_precedence_over_cursor_bookmark(
        self, mock_get_start, mock_request, mock_write_schema, mock_write_state
    ):
        mock_request.return_value = make_response([])
        state = {
            "bookmarks": {"engagements": {
                "cursor": "old-cursor",
                "offset": {"after": "inflight-cursor-xyz"},
            }},
        }

        sync_engagements(state, self.make_ctx())

        call_params = mock_request.call_args[0][1]
        self.assertEqual(call_params['after'], "inflight-cursor-xyz")

    @patch('singer.write_state')
    @patch('singer.write_schema')
    @patch('tap_hubspot.request')
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    def test_offset_persisted_per_page_and_cleared_on_completion(
        self, mock_get_start, mock_request, mock_write_schema, mock_write_state
    ):
        mock_request.side_effect = [
            make_response([], has_more=True, after="cursor-page-1"),
            make_response([], has_more=False, after="cursor-page-2"),
        ]
        state = {"bookmarks": {"engagements": {}}}

        result = sync_engagements(state, self.make_ctx())

        self.assertIsNone(singer.get_offset(result, 'engagements'))
        self.assertEqual(
            singer.get_bookmark(result, 'engagements', 'cursor'),
            'cursor-page-2'
        )

    @patch('singer.write_schema')
    @patch('tap_hubspot.request')
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00.000000Z')
    def test_offset_persists_page_one_cursor_before_fetching_page_two(
        self, mock_get_start, mock_request, mock_write_schema
    ):
        # Crash-recovery guarantee: after emitting page 1 we must persist the
        # cursor that fetches page 2, so an interrupted run resumes there
        # instead of re-reading from the last completed sync.
        mock_request.side_effect = [
            make_response([], has_more=True, after="cursor-page-1"),
            make_response([], has_more=False, after="cursor-page-2"),
        ]
        state = {"bookmarks": {"engagements": {}}}

        snapshots = []
        with patch('singer.write_state', side_effect=lambda s: snapshots.append(copy.deepcopy(s))):
            sync_engagements(state, self.make_ctx())

        # An intermediate state must have carried the page-1 cursor as the
        # in-flight offset (this is the page-2 request cursor).
        intermediate_offsets = [
            singer.get_offset(s, 'engagements')
            for s in snapshots
            if singer.get_offset(s, 'engagements')
        ]
        self.assertIn({'after': 'cursor-page-1'}, intermediate_offsets)

        # The page-2 request must have actually used that persisted cursor.
        page_two_params = mock_request.call_args_list[1][0][1]
        self.assertEqual(page_two_params['after'], 'cursor-page-1')

        # Final persisted state: offset cleared, durable cursor advanced.
        final = snapshots[-1]
        self.assertIsNone(singer.get_offset(final, 'engagements'))
        self.assertEqual(singer.get_bookmark(final, 'engagements', 'cursor'), 'cursor-page-2')
