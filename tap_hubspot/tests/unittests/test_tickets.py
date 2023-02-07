from unittest.mock import patch, ANY
from tap_hubspot import sync_tickets


@patch('tap_hubspot.Context.get_catalog_from_id', return_value={"metadata": ""})
@patch('tap_hubspot.singer.utils.strptime_with_tz', return_value="2017-01-01 00:00:00+00:00")
@patch('tap_hubspot.singer.utils.strftime')
@patch('tap_hubspot.get_start', return_value="")
@patch('tap_hubspot.gen_request_tickets', return_value=[])
def test_ticket_params_are_validated_1(mocked_gen_request, mocked_catalog_from_id, mocked_get_start,
                                       mocked_utils_strptime, mocked_utils_strftime):
    """
    # parametrs passed while making the request to request API for tickets
    """
    sync_tickets({}, mocked_catalog_from_id)

    expected_param = {'limit': 100,
                      'associations': 'contact,company,deals', 'properties': []}
    mocked_gen_request.assert_called_once_with(
        ANY, ANY, ANY, expected_param, ANY, ANY)
