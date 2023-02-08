from unittest.mock import patch, ANY
from tap_hubspot import sync_tickets, gen_request_tickets
from unittest.mock import patch


class MockResponse:

    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


def get_mocked_response(mock_output):
    return MockResponse(mock_output, status_code=200)


@patch('tap_hubspot.Context.get_catalog_from_id',
       return_value={'metadata': ''})
@patch('tap_hubspot.singer.utils.strptime_with_tz',
       return_value='2017-01-01 00:00:00+00:00')
@patch('tap_hubspot.singer.utils.strftime')
@patch('tap_hubspot.get_start', return_value='')
@patch('tap_hubspot.gen_request_tickets', return_value=[])
def test_ticket_params_are_validated(mocked_gen_request, mocked_catalog_from_id,
                                     mocked_get_start, mocked_utils_strptime,
                                     mocked_utils_strftime,):
    """
    # Validating the parameters passed while making the API request to list the tickets
    """

    sync_tickets({}, mocked_catalog_from_id)

    expected_param = {'limit': 100,
                      'associations': 'contact,company,deals',
                      'archived': False
                      }
    mocked_gen_request.assert_called_once_with(ANY, ANY, expected_param, ANY, ANY,)

output_results = {
    'results': [{
        'id': '190756785',
        'properties': {
            'content': 'also a test',
            'createdate': '2020-08-26T15:18:14.135Z',
            'hs_lastmodifieddate': '2020-11-24T19:45:08.380Z',
            'hs_object_id': '190756785',
            'hs_pipeline': '0',
            'hs_pipeline_stage': '1',
            'hs_ticket_category': None,
            'hs_ticket_priority': 'MEDIUM',
            'subject': 'ticket 2',
        },
        'createdAt': '2020-08-26T15:18:14.135Z',
        'updatedAt': '2020-11-24T19:45:08.380Z',
        'archived': False,
        'associations': {
            'companies': {
                'results': [{
                    'id': '635376281',
                    'type': 'ticket_to_company'
                }, {
                    'id': '635376281',
                    'type': 'ticket_to_company_unlabeled'
                }]
            },
            'deals': {
                'results': [{
                    'id': '2870386661',
                    'type': 'ticket_to_deal'
                }]
            },
            'contacts': {
                'results': [{
                    'id': '1',
                    'type': 'ticket_to_contact'
                }, {
                    'id': '51',
                    'type': 'ticket_to_contact'
                }, {
                    'id': '101',
                    'type': 'ticket_to_contact'
                }]
            }
        },
    }]
}


@patch('tap_hubspot.Context.get_catalog_from_id',
       return_value={'metadata': ''})
@patch('tap_hubspot.singer.utils.strptime_with_tz',
       return_value='2017-01-01 00:00:00+00:00')
@patch('tap_hubspot.singer.utils.strftime')
@patch('tap_hubspot.get_start', return_value='')
@patch('tap_hubspot.request')
def test_ticket_gen_request_validated(mocked_request, mocked_catalog_from_id,
                                      mocked_get_start, mocked_utils_strptime,
                                      mocked_utils_strftime,):
    """
    # Checking if gen_request_tickets() is calling request function atleast once
    """

    mocked_request.return_value = get_mocked_response(output_results)
    sync_tickets({}, mocked_catalog_from_id)
    input_params = {'limit': 100,
                    'associations': 'contact,company,deals',
                    'archived': False
                    }
    input_url = 'https://api.hubapi.com/crm/v4/objects/tickets'
    stream_id = 'tickets'
    gen_request_tickets(stream_id, input_url, input_params,
                        'results', 'paging',)

    mocked_request.assert_called_once_with(input_url, input_params)
