# tap-hubspot

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from HubSpot's [REST API](http://developers.hubspot.com/docs/overview)
- Extracts the following resources from HubSpot
  - [Campaigns](http://developers.hubspot.com/docs/methods/email/get_campaign_data)
  - [Companies](http://developers.hubspot.com/docs/methods/companies/get_company)
  - [Contacts](https://developers.hubspot.com/docs/api-reference/crm-contacts-v3/basic/get-crm-v3-objects-contacts)
  - [Contact Lists](https://developers.hubspot.com/docs/api-reference/crm-lists-v3/lists/post-crm-v3-lists-search)
  - [List Memberships](https://developers.hubspot.com/docs/api-reference/crm-lists-v3/memberships/get-crm-v3-lists-listId-memberships)
    - **Limitation**: HubSpot's `/crm/v3/lists/search` [endpoint](https://developers.hubspot.com/docs/api-reference/latest/crm/lists/search/search-lists) enforces a hard [10,000 record offset ceiling](https://developers.hubspot.com/docs/api-reference/latest/crm/search-the-crm#:~:text=The%20search%20endpoints%20are%20limited%20to%2010%2C000%20total%20results%20for%20any%20given%20query.%20Attempting%20to%20page%20beyond%2010%2C000%20will%20result%20in%20a%20400%20error.). It does not return records beyond that. There is no other endpoint available that can replace it.
    - To maximize coverage:
      - Historic sync (no bookmark): Fetch in BOTH ascending and descending order of `HS_UPDATED_AT`. This gives up to ~20K unique records. After the ascending pass, `start` is updated to `max_bk_value` so the descending pass only emits records newer than what was already seen, effectively deduplicating (with at most 1 record overlap at the boundary).
      - From next sync onwards: Fetch only in descending order (`-HS_UPDATED_AT`). This retrieves the latest 10K updated records.

  - [Deals](http://developers.hubspot.com/docs/methods/deals/get_deals_modified)
  - [Deal Pipelines](https://developers.hubspot.com/docs/methods/deal-pipelines/get-all-deal-pipelines)
  - [Email Events](http://developers.hubspot.com/docs/methods/email/get_events)
  - [Engagements](https://developers.hubspot.com/docs/methods/engagements/get-all-engagements)
  - [Forms](http://developers.hubspot.com/docs/methods/forms/v2/get_forms)
  - [Form Submissions](https://developers.hubspot.com/docs/api-reference/legacy/forms-v1/submissions/get-form-integrations-v1-submissions-forms-form_guid)
  - [Keywords](http://developers.hubspot.com/docs/methods/keywords/get_keywords)
  - [Owners](https://developers.hubspot.com/docs/api/crm/owners)
  - [Subscription Changes](http://developers.hubspot.com/docs/methods/email/get_subscriptions_timeline)
  - [Workflows](http://developers.hubspot.com/docs/methods/workflows/v3/get_workflows)
  - [Tickets](https://developers.hubspot.com/docs/api/crm/tickets)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Configuration

This tap requires a `config.json` which specifies details regarding [OAuth 2.0](https://developers.hubspot.com/docs/methods/oauth2/oauth2-overview) authentication, a cutoff date for syncing historical data, an optional parameter request_timeout for which request should wait to get the response and an optional flag which controls collection of anonymous usage metrics. See [config.sample.json](config.sample.json) for an example. You may specify an API key instead of OAuth parameters for development purposes, as detailed below.

To run `tap-hubspot` with the configuration file, use this command:

```bash
› tap-hubspot -c my-config.json
```


## API Key Authentication (for development)

As an alternative to OAuth 2.0 authentication during development, you may specify an API key (`HAPIKEY`) to authenticate with the HubSpot API. This should be used only for low-volume development work, as the [HubSpot API Usage Guidelines](https://developers.hubspot.com/apps/api_guidelines) specify that integrations should use OAuth for authentication.

To use an API key, include a `hapikey` configuration variable in your `config.json` and set it to the value of your HubSpot API key. Any OAuth authentication parameters in your `config.json` **will be ignored** if this key is present!

---

Copyright &copy; 2017 Stitch
