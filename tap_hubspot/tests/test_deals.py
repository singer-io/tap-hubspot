from tap_hubspot import sync_deals
from unittest.mock import patch, ANY


@patch('tap_hubspot.Context.get_catalog_from_id', return_value={"metadata": ""})
@patch('tap_hubspot.gen_request', return_value=[])
def test_associations_are_not_validated(mocked_gen_request, mocked_catalog_from_id):

    sync_deals({}, mocked_catalog_from_id)

    expected_param = {'includeAssociations': False, 'properties': [], 'limit': 100}

    mocked_gen_request.assert_called_once_with(ANY, ANY, ANY, expected_param, ANY, ANY, ANY, ANY, v3_fields=None)


@patch('tap_hubspot.Context.get_catalog_from_id', return_value={"metadata": ""})
@patch('tap_hubspot.gen_request', return_value=[])
def test_associations_are_validated(mocked_gen_request, mocked_catalog_from_id):

    sync_deals({}, mocked_catalog_from_id)

    expected_param = {'includeAssociations': True, 'properties': [], 'limit': 100}

    mocked_gen_request.assert_called_once_with(ANY, ANY, ANY, expected_param, ANY, ANY, ANY, ANY, v3_fields=None)
