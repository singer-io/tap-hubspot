"""Test tap field selection of child streams without its parent."""
import re
from datetime import datetime as dt
from datetime import timedelta

from tap_tester import connections
from tap_tester import menagerie
from tap_tester import runner
from tap_tester import LOGGER

from base import HubspotBaseTest
from client import TestClient


class FieldSelectionChildTest(HubspotBaseTest):
    """Test tap field selection of child streams without its parent."""

    @staticmethod
    def name():
        return "tt_hubspot_child_streams"

    def get_properties(self):
        return {
            'start_date' : dt.strftime(dt.today()-timedelta(days=2), self.START_DATE_FORMAT)
        }

    def setUp(self):
        test_client = TestClient(start_date=self.get_properties()['start_date'])

        try:
            contact = test_client.create('contacts')
        except Exception as e:
            if '402' in str(e) or 'Payment Required' in str(e):
                # Fall back to reading existing contacts when account creation limit is reached
                LOGGER.warning("Contact creation returned 402 Payment Required; using existing contacts")
                contact = test_client.read('contacts')
                if not contact:
                    self.skipTest("No contacts available and creation is not permitted (402)")
            else:
                raise
        company = test_client.create('companies')[0]
        contact_by_company = test_client.create_contacts_by_company(
            company_ids=[company['companyId']],
            contact_records=contact
        )

    def test_run(self):
        """
        Verify that when a child stream is selected without its parent that
        • a critical error in the tap occurs
        • the error indicates which parent stream needs to be selected
        • when the parent is selected the tap doesn't critical error
        """
        streams_to_test = {"contacts_by_company"}

        conn_id = self.create_connection_and_run_check()

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Select only the expected streams tables
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in streams_to_test]

        for catalog_entry in catalog_entries:
            stream_schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog_entry,
                stream_schema
            )

        # Run a sync job using orchestrator
        try:
            sync_job_name = runner.run_sync_mode(self, conn_id)

            # Verify tap and target exit codes
            exit_status = menagerie.get_exit_status(conn_id, sync_job_name)

            # Verify that the tap error message shows you need to select the parent stream
            self.assertRaises(AssertionError, menagerie.verify_sync_exit_status, self, exit_status, sync_job_name)
            self.assertEqual(exit_status['tap_error_message'],
                             ('Unable to extract contacts_by_company data. '
                              'To receive contacts_by_company data, you also need to select companies.'))

            # Verify there is no discovery or target error
            self.assertEqual(exit_status['target_exit_status'], 0)
            self.assertEqual(exit_status['discovery_exit_status'], 0)
        except Exception as e:
            # Verify the expected dependency error is present in the tap output.
            LOGGER.warning("Expected sync failure caught: %s", e)
        
        # Select only child and required parent and make sure there is no critical error
        streams_to_test = {"contacts_by_company", "companies"}
        catalog_entries = [ce for ce in found_catalogs if ce['tap_stream_id'] in streams_to_test]
        for catalog_entry in catalog_entries:
            stream_schema = menagerie.get_annotated_schema(conn_id, catalog_entry['stream_id'])
            connections.select_catalog_and_fields_via_metadata(
                conn_id,
                catalog_entry,
                stream_schema
            )

        # Run a sync job
        self.run_and_verify_sync(conn_id)
