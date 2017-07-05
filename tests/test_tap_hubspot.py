from contextlib import contextmanager
from io import StringIO
from singer import utils
from tap_hubspot import *
import time
import datetime
import json
import requests_mock
import unittest

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

# See https://stackoverflow.com/a/17981937
@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

def get_first_record(stdout):
    lines = stdout.getvalue().split("\n")
    for line in lines:
        try:
            message = json.loads(line)
            if message["type"] == "RECORD":
                return message
        except:
            pass

class TestTapHubspotCompanies(unittest.TestCase):

    def set_config(self, older_than_30_days):
        start_days_ago = 1
        if older_than_30_days:
            start_days_ago = 31

        CONFIG.update({
            "access_token": "FAKEFAKEFAKE",
            # needs to be less more 30 days
            "start_date": utils.strftime(
                datetime.datetime.utcnow() -
                datetime.timedelta(days=start_days_ago)
            ),
            "token_expires": (
                datetime.datetime.utcnow() +
                datetime.timedelta(hours=1)
            )
        })

    def sync(self):
        do_sync({
            "streams": [{
                "schema": {
                    "selected": True
                },
                "stream": "companies"
            }]
        })

    def test_companies_last_sync_less_than_30_days_ago_less_than_10k_results(self):
        with requests_mock.Mocker() as mocker:
            with captured_output() as (out, err):
                STATE.clear()
                self.set_config(False)

                mocker.get(
                    "https://api.hubapi.com/companies/v2/properties",
                    json={}
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/paged?properties=createdate&properties=hs_lastmodifieddate&count=250",
                    complete_qs=True,
                    json={
                        "companies": [],
                        "has-more": True,
                        "offset": 252,
                        "total": 400
                    }
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/paged?count=250&properties=createdate&properties=hs_lastmodifieddate&offset=252",
                    complete_qs=True,
                    json={
                        "companies": [{
                            "portalId": 62515,
                            "companyId": 19411477,
                            "isDeleted": False,
                            "properties": {
                                "hs_lastmodifieddate": {
                                    "timestamp": time.time() * 1000.0
                                }
                            }
                        }],
                        "hasMore": False,
                        "offset": 250,
                        "total": 400
                    }
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/19411477",
                    json={
                        "portalId": 62515,
                        "companyId": 10444744,
                        "isDeleted": True # need to differentiate this in some way
                    }
                )

                self.sync()

        expected_record = {
            "type": "RECORD",
            "stream": "companies",
            "record": {
                "portalId": 62515,
                "companyId": 10444744,
                "isDeleted": True
            }
        }
        self.assertEqual(get_first_record(out), expected_record)

    def test_companies_last_sync_less_than_30_days_ago_more_than_10k_results(self):
        with requests_mock.Mocker() as mocker:
            with captured_output() as (out, err):
                STATE.clear()
                self.set_config(False)

                mocker.get(
                    "https://api.hubapi.com/companies/v2/properties",
                    json={}
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/paged?properties=createdate&properties=hs_lastmodifieddate&count=250",
                    complete_qs=True,
                    json={
                        "results": [],
                        "hasMore": True,
                        "offset": 250,
                        "total": 10001
                    }
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/paged?properties=createdate&properties=hs_lastmodifieddate&count=250",
                    complete_qs=True,
                    json={
                        "companies": [],
                        "has-more": True,
                        "offset": 250
                    }
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/paged?count=250&properties=createdate&properties=hs_lastmodifieddate&offset=250",
                    complete_qs=True,
                    json={
                        "companies": [{
                            "portalId": 62515,
                            "companyId": 19411477,
                            "isDeleted": False,
                            "properties": {}
                        }],
                        "has-more": False,
                        "offset": 500
                    }
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/19411477",
                    json={
                        "portalId": 62515,
                        "companyId": 10444744,
                        "isDeleted": True # need to differentiate this in some way
                    }
                )

                self.sync()

        expected_record = {
            "type": "RECORD",
            "stream": "companies",
            "record": {
                "portalId": 62515,
                "companyId": 10444744,
                "isDeleted": True
            }
        }
        self.assertEqual(get_first_record(out), expected_record)

    def test_companies_last_sync_more_than_30_days_ago(self):
        with requests_mock.Mocker() as mocker:
            with captured_output() as (out, err):
                STATE.clear()
                self.set_config(True)

                mocker.get(
                    "https://api.hubapi.com/companies/v2/properties",
                    json={}
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/paged?count=250",
                    complete_qs=True,
                    json={
                        "companies": [],
                        "has-more": True,
                        "offset": 250
                    }
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/paged?count=250&properties=createdate&properties=hs_lastmodifieddate",
                    complete_qs=True,
                    json={
                        "companies": [{
                            "portalId": 62515,
                            "companyId": 19411477,
                            "isDeleted": False,
                            "properties": {}
                        }],
                        "has-more": False,
                        "offset": 500
                    }
                )
                mocker.get(
                    "https://api.hubapi.com/companies/v2/companies/19411477",
                    json={
                        "portalId": 62515,
                        "companyId": 10444744,
                        "isDeleted": True # need to differentiate this in some way
                    }
                )

                self.sync()

        expected_record = {
            "type": "RECORD",
            "stream": "companies",
            "record": {
                "portalId": 62515,
                "companyId": 10444744,
                "isDeleted": True
            }
        }
        self.assertEqual(get_first_record(out), expected_record)
