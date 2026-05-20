import json
import os
import unittest
from unittest.mock import patch, MagicMock

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
