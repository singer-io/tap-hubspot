import unittest
from tap_hubspot import *

class TestTapHubspot(unittest.TestCase):

    def test_get_streams_to_sync_with_no_this_stream(self):
        streams = [
            Stream('a', 'a'),
            Stream('b', 'b')
        ]
        state = {}
        self.assertEqual(streams, get_streams_to_sync(streams, state))

    def test_get_streams_to_sync_with_this_stream(self):
        streams = [
            Stream('a', 'a'),
            Stream('b', 'b'),
            Stream('c', 'c'),            
        ]
        state = {'this_stream': 'b'}
        self.assertEqual(streams[1:], list(get_streams_to_sync(streams, state)))
             
            
