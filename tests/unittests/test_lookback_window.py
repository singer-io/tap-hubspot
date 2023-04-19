"""
Unit tests at the functions need to test lookback window
"""
import os
import unittest

from datetime import timedelta
from tap_hubspot import CONFIG
from tap_hubspot import get_lookback_window
from parameterized import parameterized



class TestDeals(unittest.TestCase):
    """
    This class gets an access token for the tests to use and then tests
    assumptions we have about the tap
    """
    def setUp(self):
        """
        This functions reads in the variables need to get an access token
        """
        CONFIG['start_date'] = "2023-01-01T00:00:00Z"
        CONFIG['lookback_window'] = "2"

    @parameterized.expand([
        ("", timedelta(days=0)),
        (None, timedelta(days=0)),
        ("2023-01-02T01:00:00Z", timedelta(days=1, hours=1)),
        ("2023-01-05T01:00:00Z", timedelta(days=2))])
    def test_parse_lookback_window(self, bookmark_value, expected_lookback_window):
        """
        Verify that lookback window value is set properly
        """
        self.assertEqual(get_lookback_window(bookmark_value), expected_lookback_window)
