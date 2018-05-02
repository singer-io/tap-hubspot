from contextlib import contextmanager
from io import StringIO
from singer import utils
from tap_hubspot import *
import time
import datetime
import json
import requests_mock
import unittest

class TestGetStreamsToSync(unittest.TestCase):

    def test_get_streams_to_sync_with_no_this_stream(self):
        streams = [
            Stream('a', 'a', [], None, None),
            Stream('b', 'b', [], None, None),
            Stream('c', 'c', [], None, None),
        ]
        state = {'this_stream': None}
        self.assertEqual(streams, get_streams_to_sync(streams, state))

    def test_get_streams_to_sync_with_this_stream(self):
        streams = [
            Stream('a', 'a', [], None, None),
            Stream('b', 'b', [], None, None),
            Stream('c', 'c', [], None, None),
        ]
        state = {'currently_syncing': 'b'}
        self.assertEqual(streams[1:], list(get_streams_to_sync(streams, state)))

    def test_parse_source_from_url_succeeds(self):
        url = "https://api.hubapi.com/companies/v2/companies/recent/modified"
        self.assertEqual('companies', parse_source_from_url(url))
