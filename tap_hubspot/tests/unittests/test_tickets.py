import unittest
from unittest.mock import patch

from tap_hubspot import sync_tickets

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
        return {
            "stream": "tickets",
            "tap_stream_id": "tickets",
            "schema": {
                "type": "object",
                "properties": {
                    "id": {
                        "type": "string"
                    },
                    "updatedAt": {
                        "type": [
                            "null",
                            "string"
                        ],
                        "format": "date-time"
                    },
                    "properties": {
                        "type": ["null", "object"],
                        "properties": {
                            "hs_all_team_ids": {
                                "type": [
                                    "null",
                                    "string"
                                ]
                            }
                        }
                    },
                    "property_hs_all_team_ids": {
                        "type": [
                            "null",
                            "string"
                        ]
                    }
                }
            },
            "metadata": [{
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": ["id"],
                    "forced-replication-method": "INCREMENTAL",
                    "valid-replication-keys": [
                        "updatedAt"
                    ],
                    "selected": True
                }
            },
                {
                    "breadcrumb": ["properties", "id"],
                    "metadata": {
                        "inclusion": "automatic"
                    }
                },

                {
                    "breadcrumb": ["properties", "updatedAt"],
                    "metadata": {
                        "inclusion": "automatic"
                    }
                },
                {
                    "breadcrumb": ["properties", "properties"],
                    "metadata": {
                        "inclusion": "available"
                    }
                },

                {
                    "breadcrumb": ["properties", "property_hs_all_team_ids"],
                    "metadata": {
                        "inclusion": "available",
                        "selected": True
                    }
                }
            ]
        }


class TestTickets(unittest.TestCase):

    @patch('tap_hubspot.sync_v3_stream')
    def test_ticket_params_are_validated(self, mocked_sync_v3_stream):
        """
        # Validating the parameters passed while making the API request to list the tickets
        """
        mock_context = MockContext()
        expected_param = {'limit': 100,
                          'associations': 'contact,company,deals',
                          'archived': False
                          }
        expected_return_value = {'currently_syncing': 'tickets'}
        mocked_sync_v3_stream.return_value = expected_return_value

        return_value = sync_tickets({'currently_syncing': 'tickets'}, mock_context)
        self.assertEqual(
            expected_return_value,
            return_value
        )
        mocked_sync_v3_stream.assert_called_once_with(
            {'currently_syncing': 'tickets'},
            mock_context,
            'tickets',
            expected_param,
            batch_read_url='https://api.hubapi.com/crm/v3/objects/tickets/batch/read',
            selected_properties=['hs_all_team_ids'],
            batch_read_params={'archived': False},
        )
