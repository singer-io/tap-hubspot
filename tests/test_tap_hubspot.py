import unittest
from tap_hubspot import *

class TestTapHubspot(unittest.TestCase):

    def test_get_streams_to_sync_with_no_this_stream(self):
        streams = [
            Stream('a', 'a', ['id']),
            Stream('b', 'b', ['id'])
        ]
        state = {}
        self.assertEqual(streams, get_streams_to_sync(streams, state))

    def test_get_streams_to_sync_with_no_this_stream(self):
        streams = [
            Stream('a', 'a', ['id']),
            Stream('b', 'b', ['id']),
            Stream('c', 'c', ['id']),
        ]
        state = {'this_stream': None}
        self.assertEqual(streams, get_streams_to_sync(streams, state))

    def test_get_streams_to_sync_with_this_stream(self):
        streams = [
            Stream('a', 'a', ['id']),
            Stream('b', 'b', ['id']),
            Stream('c', 'c', ['id']),
        ]
        state = {'this_stream': 'b'}
        self.assertEqual(streams[1:], list(get_streams_to_sync(streams, state)))

    def test_get_streams_to_sync_throws_on_bad_state(self):
        streams = [
            Stream('a', 'a', ['id']),
            Stream('b', 'b', ['id']),
            Stream('c', 'c', ['id']),
        ]
        with self.assertRaises(Exception):
            get_streams_to_sync(streams, {'this_stream': 'Some bad stream'})

    def test_parse_source_from_url_succeeds(self):
        url = "https://api.hubapi.com/companies/v2/companies/recent/modified"
        self.assertEqual('companies', parse_source_from_url(url))
