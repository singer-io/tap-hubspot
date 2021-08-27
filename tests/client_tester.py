import json
import time
from client import TestClient
from base import HubspotBaseTest

class TestHubspotTestClient(HubspotBaseTest):
    """Test the basic functionality of our Test Client. This is a tool for sanity checks, nothing more."""

    test_client = TestClient()

    def test_contacts_create(self):
        # Testing contacts Post
        old_records = self.test_client.get_contacts()
        our_record = self.test_client.create_contacts()
        new_records = self.test_client.get_contacts()
        assert len(old_records) < len(new_records), \
            f"Before contacts post found {len(old_records)} records. After post found {len(new_records)} records"

    def test_companies_create(self):
        # Testing companies Post
        
        old_records = self.test_client.get_companies('2021-08-25T00:00:00.000000Z')
        our_record = self.test_client.create_companies()
        now = time.time()
        time.sleep(6)

        new_records = self.test_client.get_companies('2021-08-25T00:00:00.000000Z')
        time_for_get = time.time()-now
        print(time_for_get)

        assert len(old_records) < len(new_records), \
            f"Before companies post found {len(old_records)} records. After post found {len(new_records)} records"

        
    def test_contact_lists_create(self):
        # Testing contact_lists POST
        
        old_records = self.test_client.get_contact_lists()
        our_record = self.test_client.create_contact_lists()
        new_records = self.test_client.get_contact_lists()
        
        assert len(old_records) < len(new_records), \
            f"Before post found {len(old_records)} records. After post found {len(new_records)} records"

        
    def test_contacts_by_company_create(self):
        # Testing contacts_by_company PUT


        old_contact_records = self.test_client.get_contacts()
        old_company_records = self.test_client.get_companies('2021-08-25T00:00:00.000000Z')
        old_records = self.test_client.get_contacts_by_company([old_company_records[0]["companyId"]])
        our_record = self.test_client.create_contacts_by_company()
        new_records = self.test_client.get_contacts_by_company([old_company_records[0]["companyId"]])
        assert len(old_records) < len(new_records), \
            f"Before post found {len(old_records)} records. After post found {len(new_records)} records"


    def test_deal_pipelines_create(self):
        # Testing deal_pipelines POST

        old_records = self.test_client.get_deal_pipelines()
        our_record = self.test_client.create_deal_pipelines()
        new_records = self.test_client.get_deal_pipelines()
        assert len(old_records) < len(new_records), \
            f"Before post found {len(old_records)} records. After post found {len(new_records)} records"



    def test_deals_create(self):
        # Testing deals POST

        old_records = self.test_client.get_deals()
        our_record = self.test_client.create_deals()
        new_records = self.test_client.get_deals()
        assert len(old_records) < len(new_records), \
            f"Before post found {len(old_records)} records. After post found {len(new_records)} records"


    def test_subscription_changes_and_email_events_create(self):
        # Testing subscription_changes  and email_events POST

        old_emails = self.test_client.get_email_events()
        old_subs = self.test_client.get_subscription_changes()
        our_record = self.test_client.create_subscription_changes()
        time.sleep(10)
        new_subs = self.test_client.get_subscription_changes()
        new_emails = self.test_client.get_email_events()

        assert len(old_subs) < len(new_subs), \
            f"Before post found {len(old_subs)} subs. After post found {len(new_subs)} subs"
        assert len(old_emails) < len(new_emails), \
            f"Before post found {len(old_emails)} emails. After post found {len(new_emails)} emails"
        print(f"Before {len(old_subs)} subs. After found {len(new_subs)} subs")
        print(f"Before {len(old_emails)} emails. After found {len(new_emails)} emails")


    def test_engagements_create(self):
        # Testing create_engagements POST

        old_records = self.test_client.get_engagements()
        our_record = self.test_client.create_engagements()
        new_records = self.test_client.get_engagements()
        assert len(old_records) < len(new_records), \
            f"Before post found {len(old_records)} records. After post found {len(new_records)} records"


    def test_forms_create(self):
        # Testing create_forms POST

        old_records = self.test_client.get_forms()
        our_record = self.test_client.create_forms()
        new_records = self.test_client.get_forms()
        assert len(old_records) < len(new_records), \
            f"Before post found {len(old_records)} records. After post found {len(new_records)} records"


    def test_workflows_create(self):
        # Testing create_workflows POST

        old_records = self.test_client.get_workflows()
        our_record = self.test_client.create_workflows()
        new_records = self.test_client.get_workflows()
        assert len(old_records) < len(new_records), \
            f"Before post found {len(old_records)} records. After post found {len(new_records)} records"
