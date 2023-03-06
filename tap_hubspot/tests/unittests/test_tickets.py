from unittest.mock import patch, ANY
from tap_hubspot import sync_tickets, gen_request_tickets
import unittest

mock_response_data = {
        "results": [{
		"updatedAt": "2022-08-18T12:57:17.587Z",
		"createdAt": "2019-08-06T02:43:01.930Z",
		"name": "hs_file_upload",
		"label": "File upload",
		"type": "string",
		"fieldType": "file",
		"description": "Files attached to a support form by a contact.",
		"groupName": "ticketinformation",
		"options": [],
		"displayOrder": -1,
		"calculated": False,
		"externalOptions": False,
		"hasUniqueValue": False,
		"hidden": False,
		"hubspotDefined": True,
		"modificationMetadata": {
			"archivable": True,
			"readOnlyDefinition": True,
			"readOnlyValue": False
		},
		"formField": True
	}]
}
    

class MockResponse:

    def __init__(self, json_data):
        self.json_data = json_data

    def json(self):
        return self.json_data

class MockContext:
    def get_catalog_from_id(self, stream_name):
        return {"stream":"tickets","tap_stream_id":"tickets","schema":{"type":"object","properties":{"id":{"type":"string"},"createdAt":{"type":["null","string"],"format":"date-time"},"updatedAt":{"type":["null","string"],"format":"date-time"},"properties":{"type":"object","properties":{"closed_date":{"type":["null","string"],"format":"date-time"},"hs_all_team_ids":{"type":["null","string"]}}},"associations":{"type":["null","object"],"properties":{"companies":{"type":["null","object"],"properties":{"results":{"type":["null","array"],"items":{"type":["null","object"],"properties":{"id":{"type":["null","string"]},"type":{"type":["null","string"]}}}}}}}},"property_closed_date":{"type":["null","string"],"format":"date-time"},"property_hs_all_team_ids":{"type":["null","string"]}}},"metadata":[{"breadcrumb":[],"metadata":{"table-key-properties":["id"],"forced-replication-method":"INCREMENTAL","valid-replication-keys":["updatedAt"],"selected":True}},{"breadcrumb":["properties","id"],"metadata":{"inclusion":"automatic"}},{"breadcrumb":["properties","createdAt"],"metadata":{"inclusion":"available"}},{"breadcrumb":["properties","updatedAt"],"metadata":{"inclusion":"automatic"}},{"breadcrumb":["properties","properties"],"metadata":{"inclusion":"available"}},{"breadcrumb":["properties","associations"],"metadata":{"inclusion":"available"}},{"breadcrumb":["properties","property_closed_date"],"metadata":{"inclusion":"available", "selected": True}},{"breadcrumb":["properties","property_hs_all_team_ids"],"metadata":{"inclusion":"available"}}]}


class TestTickets(unittest.TestCase):

    @patch('tap_hubspot.request', return_value=MockResponse(mock_response_data))
    @patch('tap_hubspot.get_start', return_value='2023-01-01T00:00:00Z')
    @patch('tap_hubspot.gen_request_tickets')
    def test_ticket_params_are_validated(self, mocked_gen_request, mocked_get_start, mock_request_response):
        """
        # Validating the parameters passed while making the API request to list the tickets
        """
        mock_context = MockContext()
        expected_param = {'limit': 100,
                        'associations': 'contact,company,deals',
                        'archived': False
                        }
        expected_return_value = {'currently_syncing': 'tickets', 'bookmarks': {'tickets': {'updatedAt': '2023-01-01T00:00:00.000000Z'}}}
        
        return_value = sync_tickets({'currently_syncing': 'tickets'}, mock_context)
        self.assertEqual(
                        expected_return_value,
                        return_value
                    )
        mocked_gen_request.assert_called_once_with('tickets', 'https://api.hubapi.com/crm/v4/objects/tickets', {'limit': 100, 'associations': 'contact,company,deals', 'properties': 'closed_date', 'archived': False}, 'results', 'paging')

