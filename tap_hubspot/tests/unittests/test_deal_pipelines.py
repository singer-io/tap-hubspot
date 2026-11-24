import unittest
from unittest.mock import MagicMock, patch

from tap_hubspot import ENDPOINTS, get_url, sync_deal_pipelines


class TestSyncDealPipelines(unittest.TestCase):
    @patch('tap_hubspot.LOGGER.warning')
    @patch('tap_hubspot.request')
    @patch('singer.write_schema')
    @patch('singer.write_state')
    def test_sync_emits_deprecation_warning(
            self, mock_write_state, mock_write_schema, mock_request, mock_warning):
        ctx = MagicMock()
        ctx.get_catalog_from_id.return_value = {'metadata': [], 'stream_alias': None}
        mock_request.return_value.json.return_value = []
        state = {'currently_syncing': 'deal_pipelines'}

        self.assertEqual(sync_deal_pipelines(state, ctx), state)

        mock_warning.assert_called_once()
        args = mock_warning.call_args.args
        self.assertEqual(args[1:], (ENDPOINTS['deal_pipelines'],))
        warning = args[0] % args[1:]
        self.assertIn('deal_pipelines', warning)
        self.assertIn('deprecated', warning)
        self.assertIn(ENDPOINTS['deal_pipelines'], warning)
        mock_request.assert_called_once_with(get_url('deal_pipelines'))
        mock_write_schema.assert_called_once()
        mock_write_state.assert_called_once_with(state)
