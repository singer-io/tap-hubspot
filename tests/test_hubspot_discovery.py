"""Test tap discovery mode and metadata/annotated-schema."""
import re

from tap_tester import menagerie

from base import HubspotBaseTest

import singer

class DiscoveryTest(HubspotBaseTest):
    """Test tap discovery mode and metadata/annotated-schema conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_hubspot_discovery_test"

    def test_run(self):
        """
        Verify that discover creates the appropriate catalog, schema, metadata, etc.

        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • verify there is only 1 top level breadcrumb
        • verify replication key(s)
        • verify primary key(s)
        • verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • verify the actual replication matches our expected replication method
        • verify that primary, replication and foreign keys
          are given the inclusion of automatic (metadata and annotated schema).
        • verify that all other fields have inclusion of available (metadata and schema)
        """
        streams_to_test = self.expected_streams()

        conn_id = self.create_connection()

        # Verify number of actual streams discovered match expected
        catalogs = menagerie.get_catalogs(conn_id)
        found_catalogs = [catalog for catalog in catalogs
                          if catalog.get('tap_stream_id') in streams_to_test]

        self.assertGreater(len(found_catalogs), 0,
                           msg="unable to locate schemas for connection {}".format(conn_id))
        self.assertEqual(len(found_catalogs),
                         len(streams_to_test),
                         msg="Expected {} streams, actual was {} for connection {}, "
                             "actual {}".format(
                                 len(streams_to_test),
                                 len(found_catalogs),
                                 found_catalogs,
                                 conn_id))

        # Verify the stream names discovered were what we expect
        found_catalog_names = {c['tap_stream_id'] for c in found_catalogs}
        self.assertEqual(set(streams_to_test),
                         set(found_catalog_names),
                         msg="Expected streams don't match actual streams")

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        self.assertTrue(all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
                        msg="One or more streams don't follow standard naming")

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                assert catalog  # based on previous tests this should always be found

                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                metadata = singer.metadata.to_map(schema_and_metadata["metadata"])
                schema = schema_and_metadata["annotated-schema"]

                # verify there is only 1 top level breadcrumb
                # stream_properties = [item for item in metadata if item.get("breadcrumb") == []]
                stream_properties = [item for item in metadata if item == ()]
                self.assertTrue(len(stream_properties) == 1,
                                msg="There is NOT only one top level breadcrumb for {}".format(stream) + \
                                "\nstream_properties | {}".format(stream_properties))


                stream_replication_method = metadata[()].get(self.REPLICATION_METHOD)
                if stream_replication_method == self.INCREMENTAL:
                    # verify replication key(s) for incremental streams
                    expected_value = self.expected_replication_keys()[stream]
                    actual_value = set(metadata[()].get(self.REPLICATION_KEYS, []))

                    # BUG_1 (https://stitchdata.atlassian.net/browse/SRCE-4490)
                    # self.assertEqual(expected_value,
                    #                  actual_value,
                    #                  msg="expected replication key {} but actual is {}".format(expected_value,
                    #                                                                            actual_value))

                    # self.assertTrue(actual_replication_method == self.INCREMENTAL,
                    #                 msg="Expected INCREMENTAL replication "
                    #                     "since there is a replication key")
                elif stream_replication_method == self.FULL:
                    pass
                else:
                    raise AssertionError('Expected replication method to be incremental or full, got {}'.format(stream_replication_method))

                # verify the actual replication matches our expected replication method
                self.assertEqual(
                    self.expected_replication_method().get(stream, None),
                    stream_replication_method,
                    msg="The actual replication method {} doesn't match the expected {}".format(
                        stream_replication_method,
                        self.expected_replication_method().get(stream, None)))


                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = expected_primary_keys | expected_replication_keys

                # verify primary key(s)
                actual_primary_keys = set(metadata[()].get(self.PRIMARY_KEYS))
                self.assertEqual(expected_primary_keys,
                                 actual_primary_keys,
                                 msg="expected primary key {} but actual is {}".format(expected_primary_keys,
                                                                                       actual_primary_keys))

                # verify that primary, replication and foreign keys
                # are given the inclusion of automatic in metadata.
                # BUG_2 (https://stitchdata.atlassian.net/browse/SRCE-4495)
                actual_automatic_fields = {breadcrumb[1]
                                           for breadcrumb, mdata in metadata.items()
                                           if mdata.get('inclusion') == "automatic"}
                self.assertEqual(expected_automatic_fields,
                                 actual_automatic_fields,
                                 msg="expected {} automatic fields but got {}".format(
                                     expected_automatic_fields,
                                     actual_automatic_fields))

                # verify that all other fields have of available inclusion
                # This assumes there are no unsupported fields for SaaS sources
                for breadcrumb, mdata in metadata.items():
                    if breadcrumb == ():
                        continue
                    field_name = breadcrumb[1]
                    if mdata.get('inclusion') == 'automatic':
                        self.assertTrue(field_name in expected_automatic_fields)
                    elif mdata.get('inclusion') == 'available':
                        self.assertFalse(field_name in expected_automatic_fields)
                    else:
                        raise AssertionError('{} is not inclusion automatic or available'.format(field_name))
