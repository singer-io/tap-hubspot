import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

import singer
import tap_hubspot
from tap_hubspot import sync_contact_lists


class MockResponse:
    def __init__(self, json_data):
        self.json_data = json_data
        self.status_code = 200

    def json(self):
        return self.json_data


class MockContext:
    def __init__(self, selected_stream_ids=None):
        self.selected_stream_ids = selected_stream_ids or ["contact_lists"]

    def get_catalog_from_id(self, stream_name):
        return {
            "stream": "contact_lists",
            "tap_stream_id": "contact_lists",
            "stream_alias": None,
            "schema": {
                "type": "object",
                "properties": {
                    "listId": {"type": ["null", "string"]},
                    "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                    "name": {"type": ["null", "string"]},
                    "createdAt": {"type": ["null", "string"], "format": "date-time"},
                    "processingType": {"type": ["null", "string"]},
                }
            },
            "metadata": [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "table-key-properties": ["listId"],
                        "forced-replication-method": "INCREMENTAL",
                        "valid-replication-keys": ["updatedAt"],
                        "selected": True
                    }
                },
                {
                    "breadcrumb": ["properties", "listId"],
                    "metadata": {"inclusion": "automatic"}
                },
                {
                    "breadcrumb": ["properties", "updatedAt"],
                    "metadata": {"inclusion": "automatic"}
                },
                {
                    "breadcrumb": ["properties", "name"],
                    "metadata": {"inclusion": "available", "selected": True}
                },
                {
                    "breadcrumb": ["properties", "createdAt"],
                    "metadata": {"inclusion": "available", "selected": True}
                },
                {
                    "breadcrumb": ["properties", "processingType"],
                    "metadata": {"inclusion": "available", "selected": True}
                },
            ]
        }


def make_list_record(list_id, updated_at, name="Test List"):
    """Helper to create a mock HubSpot list record."""
    return {
        "listId": str(list_id),
        "updatedAt": updated_at,
        "name": name,
        "createdAt": "2024-01-01T00:00:00Z",
        "processingType": "MANUAL",
    }


def make_api_response(lists, has_more=False, offset=0):
    """Helper to create a mock API response."""
    return {
        "lists": lists,
        "hasMore": has_more,
        "offset": offset,
        "total": len(lists),
    }


class TestSyncContactListsHistoricSync(unittest.TestCase):
    """
    Tests for historic sync (no bookmark present in state).
    Should use both ascending and descending sort orders.
    """

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_historic_sync_uses_both_sort_orders(self, mock_now, mock_load_schema, mock_post):
        """
        When no bookmark exists, sync should iterate in ascending order first,
        then descending order to maximize coverage (up to ~20K records).
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        asc_records = [
            make_list_record(1, "2024-01-01T00:00:00Z"),
            make_list_record(2, "2024-01-02T00:00:00Z"),
            make_list_record(3, "2024-01-03T00:00:00Z"),
        ]
        desc_records = [
            make_list_record(6, "2024-05-01T00:00:00Z"),
            make_list_record(5, "2024-04-01T00:00:00Z"),
            make_list_record(4, "2024-03-01T00:00:00Z"),
        ]

        mock_post.side_effect = [
            MockResponse(make_api_response(asc_records, has_more=False)),
            MockResponse(make_api_response(desc_records, has_more=False)),
        ]

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        written_records = []
        original_write_record = singer.write_record
        singer.write_record = lambda *args, **kwargs: written_records.append(args)
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            # Verify both sort orders were used
            self.assertEqual(mock_post.call_count, 2)

            # First call should be ascending
            first_call_body = mock_post.call_args_list[0][0][1]
            self.assertEqual(first_call_body['sort'], 'HS_UPDATED_AT')

            # Second call should be descending
            second_call_body = mock_post.call_args_list[1][0][1]
            self.assertEqual(second_call_body['sort'], '-HS_UPDATED_AT')

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_historic_sync_deduplication_via_start_update(self, mock_now, mock_load_schema, mock_post):
        """
        After the ascending pass, `start` is updated to `max_bk_value`.
        The descending pass should only emit records with updatedAt >= new start,
        preventing duplicates (except at the boundary).
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        # Ascending: records from Jan to Mar (max = Mar 3)
        asc_records = [
            make_list_record(1, "2024-01-01T00:00:00Z"),
            make_list_record(2, "2024-02-01T00:00:00Z"),
            make_list_record(3, "2024-03-03T00:00:00Z"),
        ]
        # Descending: records from May to Feb (some overlap with ascending)
        desc_records = [
            make_list_record(6, "2024-05-01T00:00:00Z"),
            make_list_record(5, "2024-04-01T00:00:00Z"),
            make_list_record(3, "2024-03-03T00:00:00Z"),  # boundary record (same as asc max)
            make_list_record(2, "2024-02-01T00:00:00Z"),  # already seen, older than start
        ]

        mock_post.side_effect = [
            MockResponse(make_api_response(asc_records, has_more=False)),
            MockResponse(make_api_response(desc_records, has_more=False)),
        ]

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        written_records = []
        original_write_record = singer.write_record
        singer.write_record = lambda stream, record, *args, **kwargs: written_records.append(record)
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            # Ascending pass writes all 3 records (all >= start_date)
            # Descending pass: start is updated to "2024-03-03T00:00:00Z"
            # - list 6 (May): >= start -> written
            # - list 5 (Apr): >= start -> written
            # - list 3 (Mar 3): >= start -> written (boundary duplicate, 1 overlap acceptable)
            # - list 2 (Feb): < start -> NOT written
            #
            # Total: 3 (asc) + 3 (desc) = 6 records written (with 1 acceptable duplicate of list 3)
            written_list_ids = [r.get('listId') for r in written_records]
            self.assertEqual(len(written_records), 6)

            # list 2 should only appear once (from ascending pass)
            self.assertEqual(written_list_ids.count("2"), 1)

            # list 3 appears twice (boundary overlap) - acceptable
            self.assertEqual(written_list_ids.count("3"), 2)

            # list 6 and 5 only from descending
            self.assertIn("6", written_list_ids)
            self.assertIn("5", written_list_ids)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_historic_sync_bookmark_set_to_max_value(self, mock_now, mock_load_schema, mock_post):
        """
        After historic sync, bookmark should be set to the maximum updatedAt value
        seen across both passes (capped by sync_start_time).
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        asc_records = [make_list_record(1, "2024-01-01T00:00:00Z")]
        desc_records = [make_list_record(2, "2024-05-15T00:00:00Z")]

        mock_post.side_effect = [
            MockResponse(make_api_response(asc_records, has_more=False)),
            MockResponse(make_api_response(desc_records, has_more=False)),
        ]

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()

        try:
            result_state = sync_contact_lists(STATE, ctx)

            # Bookmark should be max(all records seen) = 2024-05-15, capped by sync_start_time (June 1)
            bookmark = singer.get_bookmark(result_state, 'contact_lists', 'updatedAt')
            self.assertEqual(bookmark, "2024-05-15T00:00:00.000000Z")

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_historic_sync_bookmark_capped_by_sync_start_time(self, mock_now, mock_load_schema, mock_post):
        """
        If max_bk_value is in the future (unlikely but defensive), bookmark should
        be capped at sync_start_time.
        """
        mock_now.return_value = datetime(2024, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        asc_records = [make_list_record(1, "2024-01-01T00:00:00Z")]
        desc_records = [make_list_record(2, "2024-05-01T00:00:00Z")]

        mock_post.side_effect = [
            MockResponse(make_api_response(asc_records, has_more=False)),
            MockResponse(make_api_response(desc_records, has_more=False)),
        ]

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()

        try:
            result_state = sync_contact_lists(STATE, ctx)

            # Bookmark capped at sync_start_time (March 1)
            bookmark = singer.get_bookmark(result_state, 'contact_lists', 'updatedAt')
            self.assertEqual(bookmark, "2024-03-01T00:00:00.000000Z")

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state


class TestSyncContactListsIncrementalSync(unittest.TestCase):
    """
    Tests for incremental sync (bookmark present in state).
    Should use only descending sort order.
    """

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_incremental_sync_uses_only_desc_order(self, mock_now, mock_load_schema, mock_post):
        """
        When bookmark exists, sync should only use descending sort order.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        desc_records = [
            make_list_record(3, "2024-05-01T00:00:00Z"),
            make_list_record(2, "2024-04-01T00:00:00Z"),
            make_list_record(1, "2024-03-01T00:00:00Z"),
        ]

        mock_post.return_value = MockResponse(make_api_response(desc_records, has_more=False))

        STATE = {
            "currently_syncing": "contact_lists",
            "bookmarks": {"contact_lists": {"updatedAt": "2024-02-01T00:00:00.000000Z"}}
        }
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            # Only one API call (descending only)
            self.assertEqual(mock_post.call_count, 1)
            call_body = mock_post.call_args_list[0][0][1]
            self.assertEqual(call_body['sort'], '-HS_UPDATED_AT')

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_incremental_sync_only_writes_records_gte_bookmark(self, mock_now, mock_load_schema, mock_post):
        """
        In incremental sync, only records with updatedAt >= bookmark should be written.
        Records older than bookmark are still iterated but not written.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        desc_records = [
            make_list_record(3, "2024-05-01T00:00:00Z"),  # newer than bookmark
            make_list_record(2, "2024-04-01T00:00:00Z"),  # newer than bookmark
            make_list_record(1, "2024-01-01T00:00:00Z"),  # older than bookmark
        ]

        mock_post.return_value = MockResponse(make_api_response(desc_records, has_more=False))

        STATE = {
            "currently_syncing": "contact_lists",
            "bookmarks": {"contact_lists": {"updatedAt": "2024-03-01T00:00:00.000000Z"}}
        }
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        written_records = []
        original_write_record = singer.write_record
        singer.write_record = lambda stream, record, *args, **kwargs: written_records.append(record)
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            # Only 2 records should be written (list 3 and list 2)
            self.assertEqual(len(written_records), 2)
            written_ids = [r['listId'] for r in written_records]
            self.assertIn("3", written_ids)
            self.assertIn("2", written_ids)
            self.assertNotIn("1", written_ids)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_incremental_sync_iterates_all_pages(self, mock_now, mock_load_schema, mock_post):
        """
        Incremental sync should iterate through all pages (no early stopping),
        even when records are older than bookmark.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        page1 = [make_list_record(3, "2024-05-01T00:00:00Z")]
        page2 = [make_list_record(1, "2024-01-01T00:00:00Z")]

        responses = [
            MockResponse(make_api_response(page1, has_more=True, offset=250)),
            MockResponse(make_api_response(page2, has_more=False)),
        ]
        captured_bodies = []

        def capture_post(url, body):
            captured_bodies.append(dict(body))
            return responses.pop(0)

        mock_post.side_effect = capture_post

        STATE = {
            "currently_syncing": "contact_lists",
            "bookmarks": {"contact_lists": {"updatedAt": "2024-03-01T00:00:00.000000Z"}}
        }
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            # Both pages should be fetched (no early stopping)
            self.assertEqual(mock_post.call_count, 2)

            # Second call should include offset from first response
            self.assertEqual(captured_bodies[1]['offset'], 250)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_incremental_sync_bookmark_advances_to_max(self, mock_now, mock_load_schema, mock_post):
        """
        After incremental sync, bookmark should advance to the max updatedAt seen.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        desc_records = [
            make_list_record(2, "2024-05-20T00:00:00Z"),
            make_list_record(1, "2024-05-10T00:00:00Z"),
        ]

        mock_post.return_value = MockResponse(make_api_response(desc_records, has_more=False))

        STATE = {
            "currently_syncing": "contact_lists",
            "bookmarks": {"contact_lists": {"updatedAt": "2024-04-01T00:00:00.000000Z"}}
        }
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()

        try:
            result_state = sync_contact_lists(STATE, ctx)

            bookmark = singer.get_bookmark(result_state, 'contact_lists', 'updatedAt')
            self.assertEqual(bookmark, "2024-05-20T00:00:00.000000Z")

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state


class TestSyncContactListsPagination(unittest.TestCase):
    """
    Tests for pagination behavior.
    """

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_pagination_follows_hasMore_and_offset(self, mock_now, mock_load_schema, mock_post):
        """
        Sync should continue paginating while hasMore is True, using the offset
        from each response.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        page1 = [make_list_record(1, "2024-05-01T00:00:00Z")]
        page2 = [make_list_record(2, "2024-05-02T00:00:00Z")]
        page3 = [make_list_record(3, "2024-05-03T00:00:00Z")]

        responses = [
            MockResponse(make_api_response(page1, has_more=True, offset=250)),
            MockResponse(make_api_response(page2, has_more=True, offset=500)),
            MockResponse(make_api_response(page3, has_more=False, offset=750)),
        ]
        captured_bodies = []

        def capture_post(url, body):
            captured_bodies.append(dict(body))
            return responses.pop(0)

        mock_post.side_effect = capture_post

        STATE = {
            "currently_syncing": "contact_lists",
            "bookmarks": {"contact_lists": {"updatedAt": "2024-01-01T00:00:00.000000Z"}}
        }
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            # 3 pages fetched
            self.assertEqual(mock_post.call_count, 3)

            # Verify offsets were passed correctly
            self.assertEqual(captured_bodies[1]['offset'], 250)
            self.assertEqual(captured_bodies[2]['offset'], 500)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_historic_sync_pagination_across_both_passes(self, mock_now, mock_load_schema, mock_post):
        """
        Historic sync should paginate fully through both ascending and descending passes.
        Each pass resets offset independently.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        asc_page1 = [make_list_record(1, "2024-01-01T00:00:00Z")]
        asc_page2 = [make_list_record(2, "2024-02-01T00:00:00Z")]
        desc_page1 = [make_list_record(4, "2024-05-01T00:00:00Z")]
        desc_page2 = [make_list_record(3, "2024-03-01T00:00:00Z")]

        mock_post.side_effect = [
            MockResponse(make_api_response(asc_page1, has_more=True, offset=250)),
            MockResponse(make_api_response(asc_page2, has_more=False, offset=500)),
            MockResponse(make_api_response(desc_page1, has_more=True, offset=250)),
            MockResponse(make_api_response(desc_page2, has_more=False, offset=500)),
        ]

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            # 4 total API calls (2 ascending + 2 descending)
            self.assertEqual(mock_post.call_count, 4)

            # Verify ascending pass sort
            self.assertEqual(mock_post.call_args_list[0][0][1]['sort'], 'HS_UPDATED_AT')
            self.assertEqual(mock_post.call_args_list[1][0][1]['sort'], 'HS_UPDATED_AT')

            # Verify descending pass sort
            self.assertEqual(mock_post.call_args_list[2][0][1]['sort'], '-HS_UPDATED_AT')
            self.assertEqual(mock_post.call_args_list[3][0][1]['sort'], '-HS_UPDATED_AT')

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark


class TestSyncContactListsEmptyResponses(unittest.TestCase):
    """
    Tests for edge cases with empty or no data.
    """

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_empty_response_no_records(self, mock_now, mock_load_schema, mock_post):
        """
        When API returns no lists, bookmark should still be written using start_date.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        mock_post.return_value = MockResponse(make_api_response([], has_more=False))

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()

        try:
            result_state = sync_contact_lists(STATE, ctx)

            # Bookmark should still be written (using start_date as max_bk_value)
            bookmark = singer.get_bookmark(result_state, 'contact_lists', 'updatedAt')
            self.assertIsNotNone(bookmark)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_no_data_synced_list_memberships_bookmark_written(self, mock_now, mock_load_schema, mock_post):
        """
        When no data is synced and list_memberships is selected,
        list_memberships bookmark should still be written.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

        def schema_side_effect(name):
            return {
                "type": "object",
                "properties": {
                    "listId": {"type": ["null", "string"]},
                    "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                    "name": {"type": ["null", "string"]},
                    "createdAt": {"type": ["null", "string"], "format": "date-time"},
                    "processingType": {"type": ["null", "string"]},
                    "recordId": {"type": ["null", "string"]},
                    "membershipTimestamp": {"type": ["null", "string"], "format": "date-time"},
                }
            }

        mock_load_schema.side_effect = schema_side_effect
        mock_post.return_value = MockResponse(make_api_response([], has_more=False))

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext(selected_stream_ids=["contact_lists", "list_memberships"])
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()

        try:
            result_state = sync_contact_lists(STATE, ctx)

            # Both bookmarks should be written
            cl_bookmark = singer.get_bookmark(result_state, 'contact_lists', 'updatedAt')
            lm_bookmark = singer.get_bookmark(result_state, 'list_memberships', 'membershipTimestamp')
            self.assertIsNotNone(cl_bookmark)
            self.assertIsNotNone(lm_bookmark)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state


class TestSyncContactListsChildStream(unittest.TestCase):
    """
    Tests for list_memberships child stream interaction.
    """

    @patch('tap_hubspot.sync_list_memberships')
    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_list_memberships_called_for_all_records(self, mock_now, mock_load_schema, mock_post, mock_sync_memberships):
        """
        list_memberships should be synced for ALL records regardless of whether they
        pass the bookmark filter (to ensure membership data is complete).
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

        def schema_side_effect(name):
            return {
                "type": "object",
                "properties": {
                    "listId": {"type": ["null", "string"]},
                    "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                    "name": {"type": ["null", "string"]},
                    "createdAt": {"type": ["null", "string"], "format": "date-time"},
                    "processingType": {"type": ["null", "string"]},
                    "recordId": {"type": ["null", "string"]},
                    "membershipTimestamp": {"type": ["null", "string"], "format": "date-time"},
                }
            }

        mock_load_schema.side_effect = schema_side_effect
        mock_sync_memberships.return_value = (
            {"currently_syncing": "contact_lists", "bookmarks": {"contact_lists": {"updatedAt": "2024-04-01T00:00:00.000000Z"}}},
            "2020-01-01T00:00:00Z"
        )

        desc_records = [
            make_list_record(3, "2024-05-01T00:00:00Z"),  # newer than bookmark
            make_list_record(2, "2024-04-01T00:00:00Z"),  # equal to bookmark
            make_list_record(1, "2024-01-01T00:00:00Z"),  # older than bookmark
        ]

        mock_post.return_value = MockResponse(make_api_response(desc_records, has_more=False))

        STATE = {
            "currently_syncing": "contact_lists",
            "bookmarks": {"contact_lists": {"updatedAt": "2024-04-01T00:00:00.000000Z"}}
        }
        ctx = MockContext(selected_stream_ids=["contact_lists", "list_memberships"])
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            # sync_list_memberships called for ALL 3 records (even the one older than bookmark)
            self.assertEqual(mock_sync_memberships.call_count, 3)

            # Verify it was called with correct list_ids
            called_list_ids = [c[0][0] for c in mock_sync_memberships.call_args_list]
            self.assertEqual(called_list_ids, ["3", "2", "1"])

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark

    @patch('tap_hubspot.sync_list_memberships')
    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_list_memberships_not_called_when_not_selected(self, mock_now, mock_load_schema, mock_post, mock_sync_memberships):
        """
        list_memberships should NOT be called when it's not in selected_stream_ids.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        records = [make_list_record(1, "2024-05-01T00:00:00Z")]
        mock_post.return_value = MockResponse(make_api_response(records, has_more=False))

        STATE = {
            "currently_syncing": "contact_lists",
            "bookmarks": {"contact_lists": {"updatedAt": "2024-01-01T00:00:00.000000Z"}}
        }
        ctx = MockContext(selected_stream_ids=["contact_lists"])
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)
            mock_sync_memberships.assert_not_called()

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark


class TestSyncContactListsRequestBody(unittest.TestCase):
    """
    Tests verifying the request body sent to the API.
    """

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_request_body_count_is_250(self, mock_now, mock_load_schema, mock_post):
        """
        Request body should always use count=250.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        mock_post.return_value = MockResponse(make_api_response([], has_more=False))

        STATE = {
            "currently_syncing": "contact_lists",
            "bookmarks": {"contact_lists": {"updatedAt": "2024-01-01T00:00:00.000000Z"}}
        }
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        original_write_record = singer.write_record
        singer.write_record = MagicMock()
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            call_body = mock_post.call_args_list[0][0][1]
            self.assertEqual(call_body['count'], 250)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark


class TestSyncContactListsStartUpdate(unittest.TestCase):
    """
    Tests for the `start = max_bk_value` logic between passes.
    """

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_start_updated_between_passes_filters_desc_records(self, mock_now, mock_load_schema, mock_post):
        """
        After ascending pass, `start = max_bk_value` should prevent the descending pass
        from writing records that were already emitted in the ascending pass.
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        asc_records = [
            make_list_record(1, "2024-01-01T00:00:00Z"),
            make_list_record(2, "2024-02-01T00:00:00Z"),
            make_list_record(3, "2024-03-01T00:00:00Z"),  # max_bk_value after asc pass
        ]
        desc_records = [
            make_list_record(5, "2024-05-01T00:00:00Z"),  # new, >= start -> written
            make_list_record(4, "2024-04-01T00:00:00Z"),  # new, >= start -> written
            make_list_record(3, "2024-03-01T00:00:00Z"),  # boundary, >= start -> written (1 dup OK)
            make_list_record(2, "2024-02-01T00:00:00Z"),  # < start -> NOT written
            make_list_record(1, "2024-01-01T00:00:00Z"),  # < start -> NOT written
        ]

        mock_post.side_effect = [
            MockResponse(make_api_response(asc_records, has_more=False)),
            MockResponse(make_api_response(desc_records, has_more=False)),
        ]

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        written_records = []
        original_write_record = singer.write_record
        singer.write_record = lambda stream, record, *args, **kwargs: written_records.append(record)
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            written_ids = [r['listId'] for r in written_records]

            # Ascending pass: all 3 written (1, 2, 3)
            # Descending pass: 5, 4, 3 written (>= "2024-03-01"); 2, 1 NOT written (< start)
            # Total: 6 records written
            self.assertEqual(len(written_records), 6)

            # Records 1 and 2 should appear only once (from ascending)
            self.assertEqual(written_ids.count("1"), 1)
            self.assertEqual(written_ids.count("2"), 1)

            # Record 3 appears twice (boundary) - acceptable
            self.assertEqual(written_ids.count("3"), 2)

            # Records 4 and 5 appear once (from descending)
            self.assertEqual(written_ids.count("4"), 1)
            self.assertEqual(written_ids.count("5"), 1)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark

    @patch('tap_hubspot.post_search_endpoint')
    @patch('tap_hubspot.load_schema')
    @patch('tap_hubspot.utils.now')
    def test_all_records_in_both_passes_when_total_under_10k(self, mock_now, mock_load_schema, mock_post):
        """
        When total records < 10K, both passes return ALL records.
        The `start = max_bk_value` ensures desc pass doesn't re-emit records
        already in the ascending pass (only boundary record may duplicate).
        """
        mock_now.return_value = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        mock_load_schema.return_value = {
            "type": "object",
            "properties": {
                "listId": {"type": ["null", "string"]},
                "updatedAt": {"type": ["null", "string"], "format": "date-time"},
                "name": {"type": ["null", "string"]},
                "createdAt": {"type": ["null", "string"], "format": "date-time"},
                "processingType": {"type": ["null", "string"]},
            }
        }

        all_records_asc = [
            make_list_record(1, "2024-01-01T00:00:00Z"),
            make_list_record(2, "2024-02-01T00:00:00Z"),
            make_list_record(3, "2024-03-01T00:00:00Z"),
        ]
        all_records_desc = [
            make_list_record(3, "2024-03-01T00:00:00Z"),
            make_list_record(2, "2024-02-01T00:00:00Z"),
            make_list_record(1, "2024-01-01T00:00:00Z"),
        ]

        mock_post.side_effect = [
            MockResponse(make_api_response(all_records_asc, has_more=False)),
            MockResponse(make_api_response(all_records_desc, has_more=False)),
        ]

        STATE = {"currently_syncing": "contact_lists", "bookmarks": {}}
        ctx = MockContext()
        tap_hubspot.CONFIG['start_date'] = "2020-01-01T00:00:00Z"

        written_records = []
        original_write_record = singer.write_record
        singer.write_record = lambda stream, record, *args, **kwargs: written_records.append(record)
        original_write_schema = singer.write_schema
        singer.write_schema = MagicMock()
        original_write_state = singer.write_state
        singer.write_state = MagicMock()
        original_write_bookmark = singer.write_bookmark
        singer.write_bookmark = lambda state, stream, key, val: singer.bookmarks.write_bookmark(state, stream, key, val)

        try:
            sync_contact_lists(STATE, ctx)

            written_ids = [r['listId'] for r in written_records]

            # Ascending: all 3 written. max_bk_value = "2024-03-01"
            # Descending: only record 3 (>= "2024-03-01") written. Records 2, 1 filtered out.
            # Total: 4 records (3 + 1 boundary duplicate)
            self.assertEqual(len(written_records), 4)

            # Only record 3 is duplicated (boundary)
            self.assertEqual(written_ids.count("3"), 2)
            self.assertEqual(written_ids.count("2"), 1)
            self.assertEqual(written_ids.count("1"), 1)

        finally:
            singer.write_record = original_write_record
            singer.write_schema = original_write_schema
            singer.write_state = original_write_state
            singer.write_bookmark = original_write_bookmark


if __name__ == '__main__':
    unittest.main()
