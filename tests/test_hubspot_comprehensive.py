"""
Comprehensive integration test that validates multiple scenarios in a single test run.

Replaces individual discovery, automatic fields, and basic bookmarks tests by combining
their assertions into a single test that does 1 discovery + 2 syncs, avoiding redundant
API calls and test data setup.
"""
import datetime
import re
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
from tap_tester import LOGGER

from base import HubspotBaseTest
from client import TestClient


class TestHubspotComprehensive(HubspotBaseTest):
    """Comprehensive test covering discovery, field selection, bookmarks, and automatic fields.

    Coverage carried over from the individual tests:
    - test_hubspot_discovery.py: naming conventions, breadcrumb structure, replication
      keys/method, primary keys, inclusion metadata (automatic vs available)
    - test_hubspot_automatic_fields.py: only automatic fields sent to target when
      select_all_fields=False, no duplicate records by primary key
    - test_hubspot_bookmarks.py (subset): bookmark advancement, incremental behavior
    """

    @staticmethod
    def name():
        return "tt_hubspot_comprehensive"

    def get_properties(self):
        return {
            'start_date': datetime.datetime.strftime(
                datetime.datetime.utcnow() - datetime.timedelta(days=3),
                self.START_DATE_FORMAT
            )
        }

    def streams_to_test(self):
        """Test a representative subset of streams for sync phases"""
        return {
            'companies',    # Incremental with hs_lastmodifieddate
            'contacts',     # Incremental with associations
            'deals',        # Incremental with v3 properties
        }

    def test_run(self):
        """
        Single test that validates:
        1. Discovery - catalog structure, metadata, naming conventions
        2. Field selection - automatic fields only
        3. First sync - data replication, automatic fields verification
        4. Second sync - bookmark advancement, incremental behavior
        """

        test_client = TestClient(self.get_properties()['start_date'])
        expected_streams = self.streams_to_test()

        # Create 1 test record per stream
        LOGGER.info("Creating test data...")
        for stream in expected_streams:
            test_client.create(stream)

        ##################################################################
        # PHASE 1: Discovery - validate catalog structure and metadata
        ##################################################################
        LOGGER.info("PHASE 1: Running discovery...")
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}

        # Verify stream names follow naming convention (lowercase alphas and underscores)
        self.assertTrue(
            all(re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names),
            msg="One or more streams don't follow standard naming"
        )

        # Validate discovery metadata for ALL expected streams (not just streams_to_test)
        all_expected_streams = self.expected_streams()
        for stream in all_expected_streams:
            with self.subTest(stream=stream, phase="discovery"):
                catalog = next(
                    (c for c in found_catalogs if c["stream_name"] == stream), None
                )
                self.assertIsNotNone(catalog, msg=f"Stream {stream} not found in catalog")

                schema_and_metadata = menagerie.get_annotated_schema(
                    conn_id, catalog['stream_id']
                )
                metadata = schema_and_metadata["metadata"]

                # Verify exactly 1 top-level breadcrumb
                stream_properties = [
                    item for item in metadata if item.get("breadcrumb") == []
                ]
                self.assertEqual(
                    len(stream_properties), 1,
                    msg=f"There is NOT only one top level breadcrumb for {stream}"
                )

                # Verify replication keys match expected
                actual_rep_keys = set(
                    stream_properties[0]
                    .get("metadata", {})
                    .get(self.REPLICATION_KEYS, [])
                )
                self.assertEqual(
                    actual_rep_keys,
                    self.expected_replication_keys()[stream],
                    msg=f"{stream}: expected replication keys "
                        f"{self.expected_replication_keys()[stream]} "
                        f"but got {actual_rep_keys}"
                )

                # Verify primary keys match expected
                actual_primary_keys = set(
                    stream_properties[0]
                    .get("metadata", {})
                    .get(self.PRIMARY_KEYS, [])
                )
                self.assertSetEqual(
                    self.expected_primary_keys()[stream],
                    actual_primary_keys,
                    msg=f"{stream}: expected primary keys "
                        f"{self.expected_primary_keys()[stream]} "
                        f"but got {actual_primary_keys}"
                )

                # Verify replication method: if replication key exists -> INCREMENTAL, else FULL
                actual_replication_method = stream_properties[0].get(
                    "metadata", {}
                ).get(self.REPLICATION_METHOD)
                if actual_rep_keys:
                    # BUG_TDL-9939: only some streams report forced-replication-method correctly
                    if stream in {"contacts", "companies", "deals"}:
                        self.assertEqual(
                            actual_replication_method, self.INCREMENTAL,
                            msg=f"{stream}: expected INCREMENTAL since replication key exists"
                        )
                else:
                    self.assertEqual(
                        actual_replication_method, self.FULL,
                        msg=f"{stream}: expected FULL since no replication key"
                    )

                # Verify automatic fields have inclusion=automatic in metadata
                expected_automatic = (
                    self.expected_primary_keys()[stream]
                    | self.expected_replication_keys()[stream]
                )
                actual_automatic_fields = {
                    item.get("breadcrumb", ["properties", None])[1]
                    for item in metadata
                    if item.get("metadata", {}).get("inclusion") == "automatic"
                }
                # BUG_TDL-9772: only some streams have correct inclusion metadata
                if stream in {"contacts", "companies", "deals"}:
                    self.assertEqual(
                        expected_automatic, actual_automatic_fields,
                        msg=f"{stream}: expected automatic fields "
                            f"{expected_automatic} but got {actual_automatic_fields}"
                    )

                # Verify all non-automatic fields have inclusion=available
                self.assertTrue(
                    all(
                        item.get("metadata", {}).get("inclusion") == "available"
                        for item in metadata
                        if item.get("breadcrumb", []) != []
                        and item.get("breadcrumb", ["properties", None])[1]
                        not in actual_automatic_fields
                    ),
                    msg=f"{stream}: not all non-automatic fields have inclusion=available"
                )

        # Select test streams with automatic fields only
        catalog_entries = [
            ce for ce in found_catalogs
            if ce['tap_stream_id'] in expected_streams
        ]
        self.select_all_streams_and_fields(
            conn_id, catalog_entries, select_all_fields=False
        )

        ##################################################################
        # PHASE 2: First sync - validate data and automatic fields
        ##################################################################
        LOGGER.info("PHASE 2: Running first sync...")
        first_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        state_1 = menagerie.get_state(conn_id)

        for stream in expected_streams:
            with self.subTest(stream=stream, phase="first_sync"):
                self.assertGreater(
                    first_record_count.get(stream, 0), 0,
                    msg=f"{stream} should replicate data"
                )

                records = [
                    msg['data']
                    for msg in first_sync_records[stream]['messages']
                    if msg['action'] == 'upsert'
                ]
                self.assertGreater(len(records), 0)

                # Verify only automatic fields are present in every record
                expected_auto = self.expected_automatic_fields().get(stream)
                # BUG_TDL-9939: some streams include replication keys differently
                if stream in {'subscription_changes', 'email_events'}:
                    remove_keys = self.expected_metadata()[stream].get(
                        self.REPLICATION_KEYS, set()
                    )
                    expected_auto = expected_auto - remove_keys
                elif stream == 'engagements':
                    expected_auto = expected_auto | {'engagement'}

                for record in records:
                    actual_keys = set(record.keys())
                    self.assertSetEqual(
                        actual_keys, expected_auto,
                        msg=f"{stream}: expected only automatic fields "
                            f"{expected_auto}, got {actual_keys}"
                    )

                # Verify no duplicate records by primary key
                primary_keys = self.expected_primary_keys()[stream]
                pk_values = [
                    tuple(msg['data'][pk] for pk in primary_keys)
                    for msg in first_sync_records[stream]['messages']
                    if msg['action'] == 'upsert'
                ]
                self.assertEqual(
                    len(pk_values), len(set(pk_values)),
                    msg=f"{stream}: duplicate records detected by primary key"
                )

        ##################################################################
        # PHASE 3: Create new data for incremental test
        ##################################################################
        LOGGER.info("PHASE 3: Creating new records for incremental test...")
        for stream in expected_streams:
            test_client.create(stream)

        ##################################################################
        # PHASE 4: Second sync - validate bookmarks and incremental behavior
        ##################################################################
        LOGGER.info("PHASE 4: Running second sync...")
        second_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        state_2 = menagerie.get_state(conn_id)

        for stream in expected_streams:
            with self.subTest(stream=stream, phase="bookmarks"):
                replication_keys = self.expected_replication_keys().get(stream)
                if replication_keys:
                    rep_key = list(replication_keys)[0]
                    bookmark_1 = state_1.get('bookmarks', {}).get(stream, {}).get(rep_key)
                    bookmark_2 = state_2.get('bookmarks', {}).get(stream, {}).get(rep_key)

                    if bookmark_1 and bookmark_2:
                        # Bookmark should advance or stay same
                        self.assertGreaterEqual(
                            bookmark_2, bookmark_1,
                            msg=f"{stream} bookmark should not go backwards"
                        )

                # Verify second sync has data (from new records created)
                count_1 = first_record_count.get(stream, 0)
                count_2 = second_record_count.get(stream, 0)
                self.assertGreater(
                    count_2, 0,
                    msg=f"{stream} should replicate new data"
                )

                # For incremental streams, second sync should have <= records than first
                if replication_keys:
                    self.assertLessEqual(
                        count_2, count_1,
                        msg=f"{stream} second sync should have <= records "
                            f"than first (incremental)"
                    )

        LOGGER.info("COMPREHENSIVE TEST PASSED")


class TestHubspotComprehensiveStatic(HubspotBaseTest):
    """Test static data streams that don't need data creation"""

    @staticmethod
    def name():
        return "tt_hubspot_comprehensive_static"

    def get_properties(self):
        return {'start_date': '2021-05-02T00:00:00Z'}

    def streams_to_test(self):
        return {'owners', 'campaigns'}

    def test_run(self):
        """Quick validation of static data streams"""
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        catalog_entries = [
            ce for ce in found_catalogs
            if ce['tap_stream_id'] in self.streams_to_test()
        ]
        self.select_all_streams_and_fields(conn_id, catalog_entries)

        record_count = self.run_and_verify_sync(conn_id)

        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                self.assertGreater(
                    record_count.get(stream, 0), 0,
                    msg=f"{stream} should have data"
                )
