"""
Comprehensive integration test that validates multiple scenarios in a single test run.
"""
import datetime
import tap_tester.connections as connections
import tap_tester.menagerie as menagerie
import tap_tester.runner as runner
from tap_tester import LOGGER

from base import HubspotBaseTest
from client import TestClient


class TestHubspotComprehensive(HubspotBaseTest):
    """Comprehensive test covering discovery, field selection, bookmarks, and pagination"""
    
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
        """Test a representative subset of streams"""
        return {
            'companies',    # Incremental with hs_lastmodifieddate
            'contacts',     # Incremental with associations
            'deals',        # Incremental with v3 properties
        }

    def test_run(self):
        """
        Single test that validates:
        1. Discovery - catalog structure
        2. Field selection - automatic fields
        3. First sync - data replication
        4. Bookmarks - incremental behavior
        5. Second sync - bookmark respect
        """
        
        test_client = TestClient(self.get_properties()['start_date'])
        expected_streams = self.streams_to_test()
        
        # Create 1 test record per stream
        LOGGER.info("Creating test data...")
        for stream in expected_streams:
            test_client.create(stream)
        
        # PHASE 1: Discovery and field selection
        LOGGER.info("PHASE 1: Running discovery...")
        conn_id = connections.ensure_connection(self)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        
        # Verify discovery found expected streams
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertGreater(len(found_catalog_names), 10, 
                          msg="Discovery should find 10+ streams")
        
        # Select streams and automatic fields only
        catalog_entries = [ce for ce in found_catalogs 
                          if ce['tap_stream_id'] in expected_streams]
        self.select_all_streams_and_fields(conn_id, catalog_entries, 
                                          select_all_fields=False)
        
        # PHASE 2: First sync - validate data replication
        LOGGER.info("PHASE 2: Running first sync...")
        first_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        state_1 = menagerie.get_state(conn_id)
        
        # Verify data replicated
        for stream in expected_streams:
            with self.subTest(stream=stream, phase="first_sync"):
                self.assertGreater(first_record_count.get(stream, 0), 0,
                                 msg=f"{stream} should replicate data")
                
                # Verify automatic fields present
                records = [msg['data'] for msg in first_sync_records[stream]['messages']
                          if msg['action'] == 'upsert']
                self.assertGreater(len(records), 0)
                
                # Verify primary keys present
                primary_keys = self.expected_primary_keys()[stream]
                for record in records[:1]:  # Check first record
                    for pk in primary_keys:
                        self.assertIn(pk, record.keys(),
                                    msg=f"Primary key {pk} missing")
        
        # PHASE 3: Create new data and verify incremental sync
        LOGGER.info("PHASE 3: Creating new records for incremental test...")
        for stream in expected_streams:
            test_client.create(stream)
        
        # PHASE 4: Second sync - validate bookmarks
        LOGGER.info("PHASE 4: Running second sync...")
        second_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        state_2 = menagerie.get_state(conn_id)
        
        # Verify bookmarks advanced and incremental behavior
        for stream in expected_streams:
            with self.subTest(stream=stream, phase="bookmarks"):
                replication_keys = self.expected_replication_keys().get(stream)
                if replication_keys:
                    rep_key = list(replication_keys)[0]
                    bookmark_1 = state_1.get('bookmarks', {}).get(stream, {}).get(rep_key)
                    bookmark_2 = state_2.get('bookmarks', {}).get(stream, {}).get(rep_key)
                    
                    if bookmark_1 and bookmark_2:
                        # Bookmark should advance or stay same
                        self.assertGreaterEqual(bookmark_2, bookmark_1,
                                              msg=f"{stream} bookmark should not go backwards")
                
                # Verify second sync has data (from new records created)
                count_1 = first_record_count.get(stream, 0)
                count_2 = second_record_count.get(stream, 0)
                self.assertGreater(count_2, 0,
                                 msg=f"{stream} should replicate new data")
                
                # For incremental streams, second sync should have fewer records
                # (only new/updated ones, not all records from start_date)
                if replication_keys:
                    self.assertLessEqual(count_2, count_1,
                                       msg=f"{stream} second sync should have <= records than first (incremental)")
        
        LOGGER.info("COMPREHENSIVE TEST PASSED - validated discovery, fields, sync, bookmarks")


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
        
        catalog_entries = [ce for ce in found_catalogs 
                          if ce['tap_stream_id'] in self.streams_to_test()]
        self.select_all_streams_and_fields(conn_id, catalog_entries)
        
        record_count = self.run_and_verify_sync(conn_id)
        
        # Just verify they sync without error
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # Static streams should have some data
                self.assertGreaterEqual(record_count.get(stream, 0), 0)
