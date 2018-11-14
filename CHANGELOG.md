# Changelog

## 2.2.5
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 2.2.4
  * Ensure that deal associations are being retrieved if `associations` are selected in the catalog [#79](https://github.com/singer-io/tap-hubspot/pull/79)

## 2.2.3
  * Scrub the access token from error messages Hubspot returns when there are insufficient permissions [#75](https://github.com/singer-io/tap-hubspot/pull/75)

## 2.2.2
  * Fix a bug with the 'engagements' stream which requires the 'engagement' field to have automatic inclusion [#74](https://github.com/singer-io/tap-hubspot/pull/74)

## 2.2.1
  * Fix a bug with the 'inclusion' metadata for replication_key fields [#72](https://github.com/singer-io/tap-hubspot/pull/72)

## 2.2.0
  * Adds property selection to the tap [#67](https://github.com/singer-io/tap-hubspot/pull/67)
  * Removed the keywords stream as it is deprecated [#68](https://github.com/singer-io/tap-hubspot/pull/68)
  * Schema updates [#69](https://github.com/singer-io/tap-hubspot/pull/69) [#70](https://github.com/singer-io/tap-hubspot/pull/70)
