"""Unit tests for CRM list pagination plus Batch Read property hydration."""

# pylint: disable=too-few-public-methods

import unittest
from unittest.mock import patch

import tap_hubspot


class MockResponse:
    """Minimal requests response substitute."""

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        """Return the configured response payload."""
        return self.payload


class MockContext:
    """Provide selected Singer catalog metadata to sync functions."""

    def __init__(self, stream_id, property_names):
        properties = {
            "id": {"type": "string"},
            "updatedAt": {"type": ["null", "string"], "format": "date-time"},
        }
        catalog_metadata = [
            {
                "breadcrumb": [],
                "metadata": {
                    "selected": True,
                    "table-key-properties": ["id"],
                    "forced-replication-method": "INCREMENTAL",
                    "valid-replication-keys": ["updatedAt"],
                },
            }
        ]
        for property_name in property_names:
            field_name = f"property_{property_name}"
            properties[field_name] = {"type": ["null", "string"]}
            catalog_metadata.append(
                {
                    "breadcrumb": ["properties", field_name],
                    "metadata": {"inclusion": "available", "selected": True},
                }
            )

        self.catalog = {
            "stream": stream_id,
            "tap_stream_id": stream_id,
            "stream_alias": stream_id,
            "schema": {"type": "object", "properties": properties},
            "metadata": catalog_metadata,
        }

    def get_catalog_from_id(self, _stream_id):
        """Return the synthetic catalog for the selected stream."""
        return self.catalog


def crm_row(record_id, associations=None, properties=None):
    """Build a property-only fake CRM record without customer data."""
    row = {
        "id": str(record_id),
        "createdAt": "2024-01-01T00:00:00.000Z",
        "updatedAt": "2024-01-02T00:00:00.000Z",
        "archived": False,
        "properties": properties or {},
    }
    if associations is not None:
        row["associations"] = associations
    return row


class TestCrmBatchRead(unittest.TestCase):
    """Verify safe property hydration for Contacts and Tickets."""

    @patch("tap_hubspot.post_search_endpoint")
    @patch("tap_hubspot.request")
    def test_large_property_selection_is_sent_in_post_body_not_list_get(
        self, list_request, batch_request
    ):
        """A large field list belongs only in the Batch Read JSON body."""
        property_names = [f"custom_field_{index:04d}" for index in range(500)]
        list_request.return_value = MockResponse(
            {"results": [crm_row("1", associations={"companies": {"results": []}})]}
        )
        batch_request.return_value = MockResponse(
            {"results": [crm_row("1", properties={name: "value" for name in property_names})]}
        )

        rows = list(
            tap_hubspot.get_v3_records_with_batch_read(
                "https://api.hubapi.com/crm/v3/objects/contacts",
                {"limit": 100, "associations": "tickets,companies,deals"},
                "https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
                property_names,
            )
        )

        self.assertEqual(1, len(rows))
        list_params = list_request.call_args.args[1]
        self.assertNotIn("properties", list_params)
        batch_body = batch_request.call_args.args[1]
        self.assertEqual(property_names, batch_body["properties"])

    @patch("tap_hubspot.post_search_endpoint")
    @patch("tap_hubspot.request")
    def test_ids_are_split_at_the_supported_batch_limit(self, list_request, batch_request):
        """No Batch Read request contains more than 100 record IDs."""
        source_rows = [crm_row(index) for index in range(205)]
        list_request.return_value = MockResponse({"results": source_rows})

        def batch_response(_url, body, params=None):
            del params
            return MockResponse(
                {"results": [crm_row(item["id"], properties={"subject": "value"})
                             for item in body["inputs"]]}
            )

        batch_request.side_effect = batch_response

        rows = list(
            tap_hubspot.get_v3_records_with_batch_read(
                "list-url", {}, "batch-url", ["subject"]
            )
        )

        self.assertEqual(205, len(rows))
        self.assertEqual([100, 100, 5], [
            len(call.args[1]["inputs"]) for call in batch_request.call_args_list
        ])

    @patch("tap_hubspot.post_search_endpoint")
    @patch("tap_hubspot.request")
    def test_batch_properties_are_merged_by_id_and_associations_are_preserved(
        self, list_request, batch_request
    ):
        """Hydrated rows retain list order and list-only associations."""
        associations = {"companies": {"results": [{"id": "company-1", "type": "x"}]}}
        list_request.return_value = MockResponse(
            {"results": [crm_row("2", associations=associations), crm_row("1", associations={})]}
        )
        batch_request.return_value = MockResponse(
            {
                "results": [
                    crm_row("1", properties={"subject": "first"}),
                    crm_row("2", properties={"subject": "second"}),
                ]
            }
        )

        rows = list(
            tap_hubspot.get_v3_records_with_batch_read(
                "list-url", {}, "batch-url", ["subject"]
            )
        )

        self.assertEqual(["2", "1"], [row["id"] for row in rows])
        self.assertEqual("second", rows[0]["properties"]["subject"])
        self.assertEqual(associations, rows[0]["associations"])

    @patch("tap_hubspot.post_search_endpoint")
    @patch("tap_hubspot.request")
    def test_list_pagination_continues_after_each_batch_read(self, list_request, batch_request):
        """The next list cursor is used only after the current page is hydrated."""
        list_request.side_effect = [
            MockResponse(
                {"results": [crm_row("1")], "paging": {"next": {"after": "cursor-2"}}}
            ),
            MockResponse({"results": [crm_row("2")]}),
        ]

        def batch_response(_url, body, params=None):
            del params
            return MockResponse(
                {"results": [crm_row(item["id"], properties={"subject": "value"})
                             for item in body["inputs"]]}
            )

        batch_request.side_effect = batch_response
        params = {"limit": 100}

        rows = list(
            tap_hubspot.get_v3_records_with_batch_read(
                "list-url", params, "batch-url", ["subject"]
            )
        )

        self.assertEqual(["1", "2"], [row["id"] for row in rows])
        self.assertNotIn("after", list_request.call_args_list[0].args[1])
        self.assertEqual("cursor-2", list_request.call_args_list[1].args[1]["after"])
        self.assertEqual(2, batch_request.call_count)

    @patch("tap_hubspot.singer.write_bookmark")
    @patch("tap_hubspot.singer.write_record")
    @patch("tap_hubspot.singer.write_schema")
    @patch("tap_hubspot.load_schema", return_value={"type": "object", "properties": {}})
    @patch("tap_hubspot.get_start", return_value="2024-01-01T00:00:00Z")
    @patch("tap_hubspot.get_v3_records_with_batch_read")
    def test_bookmark_does_not_advance_when_a_batch_fails(
        self, batch_records, _get_start, _load_schema, _write_schema,
        write_record, write_bookmark
    ):
        """A later batch failure leaves the replication bookmark unchanged."""
        def first_batch_then_failure():
            yield crm_row("1")
            raise RuntimeError("batch failed")

        batch_records.return_value = first_batch_then_failure()
        context = MockContext("contacts", ["firstname"])

        with self.assertRaisesRegex(RuntimeError, "batch failed"):
            tap_hubspot.sync_v3_stream(
                {"currently_syncing": "contacts"},
                context,
                "contacts",
                {"limit": 100},
                batch_read_url="batch-url",
                selected_properties=["firstname"],
            )

        write_record.assert_called_once()
        write_bookmark.assert_not_called()

    @patch("tap_hubspot.sync_v3_stream")
    def test_contacts_configures_v3_batch_read(self, sync_v3_stream):
        """Contacts uses v3 list and v3 Batch Read endpoints."""
        state = {"currently_syncing": "contacts"}
        context = MockContext("contacts", ["firstname", "lastname"])

        tap_hubspot.sync_contacts(state, context)

        args, kwargs = sync_v3_stream.call_args
        self.assertNotIn("properties", args[3])
        self.assertEqual(["firstname", "lastname"], kwargs["selected_properties"])
        self.assertEqual(
            "https://api.hubapi.com/crm/v3/objects/contacts/batch/read",
            kwargs["batch_read_url"],
        )

    @patch("tap_hubspot.sync_v3_stream")
    def test_tickets_keeps_v4_list_and_configures_v3_batch_read(self, sync_v3_stream):
        """Tickets keeps v4 listing while hydrating through v3 Batch Read."""
        state = {"currently_syncing": "tickets"}
        context = MockContext("tickets", ["subject", "content"])

        tap_hubspot.sync_tickets(state, context)

        args, kwargs = sync_v3_stream.call_args
        self.assertEqual("tickets", args[2])
        self.assertNotIn("properties", args[3])
        self.assertEqual(False, args[3]["archived"])
        self.assertEqual(["subject", "content"], kwargs["selected_properties"])
        self.assertEqual(
            "https://api.hubapi.com/crm/v3/objects/tickets/batch/read",
            kwargs["batch_read_url"],
        )
        self.assertEqual({"archived": False}, kwargs["batch_read_params"])


if __name__ == "__main__":
    unittest.main()
