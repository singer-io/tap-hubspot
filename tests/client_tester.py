import json
import time
from client import TestClient
from base import HubspotBaseTest

class TestHubspotTestClient(HubspotBaseTest):
    """Test the basic functionality of our Test Client. This is a tool for sanity checks, nothing more."""

    def test_the_test_client(self):
        test_client = TestClient()

        # ##########################################################################
        # ### Testing contacts Post
        # ##########################################################################
        # old_records = test_client.get_contacts()
        # our_record = test_client.create_contacts()
        # new_records = test_client.get_contacts()
        # assert len(old_records) < len(new_records), \
        #     f"Before post found {len(old_records)} records. After post found {len(new_records)} records"

        ##########################################################################
        ### Testing companies Post
        ##########################################################################
        # old_records = test_client.get_companies('2021-08-25T00:00:00.000000Z')
        # our_record = test_client.create_companies()
        # now = time.time()
        # time.sleep(6)

        # new_records = test_client.get_companies('2021-08-25T00:00:00.000000Z')
        # time_for_get = time.time()-now
        # print(time_for_get)

        # assert len(old_records) < len(new_records), \
        #     f"Before post found {len(old_records)} records. After post found {len(new_records)} records"

        ##########################################################################
        ### Testing contact_lists POST
        ##########################################################################
        # old_records = test_client.get_contact_lists()
        # our_record = test_client.create_contact_lists()
        # new_records = test_client.get_contact_lists()
        
        # assert len(old_records) < len(new_records), \
        #     f"Before post found {len(old_records)} records. After post found {len(new_records)} records"

        ##########################################################################
        ### Testing contacts_by_company PUT
        ##########################################################################

        # old_contact_records = test_client.get_contacts()
        # old_company_records = test_client.get_companies('2021-08-25T00:00:00.000000Z')
        # old_records = test_client.get_contacts_by_company([old_company_records[0]["companyId"]])
        # our_record = test_client.create_contacts_by_company()
        # new_records = test_client.get_contacts_by_company([old_company_records[0]["companyId"]])
        # assert len(old_records) < len(new_records), \
        #     f"Before post found {len(old_records)} records. After post found {len(new_records)} records"

        ##########################################################################
        ### Testing deal_pipelines POST
        ##########################################################################
        # old_records = test_client.get_deal_pipelines()
        # our_record = test_client.create_deal_pipelines()
        # new_records = test_client.get_deal_pipelines()
        # assert len(old_records) < len(new_records), \
        #     f"Before post found {len(old_records)} records. After post found {len(new_records)} records"


        ##########################################################################
        ### Testing deals POST
        ##########################################################################
        # old_records = test_client.get_deals()
        # our_record = test_client.create_deals()
        # new_records = test_client.get_deals()
        # assert len(old_records) < len(new_records), \
        #     f"Before post found {len(old_records)} records. After post found {len(new_records)} records"

        ##########################################################################
        ### Testing subscription_changes POST
        ##########################################################################
        old_emails = test_client.get_email_events()
        old_subs = test_client.get_subscription_changes()
        our_record = test_client.create_subscription_changes()
        time.sleep(10)
        new_subs = test_client.get_subscription_changes()
        new_emails = test_client.get_email_events()

        assert len(old_subs) < len(new_subs), \
            f"Before post found {len(old_subs)} subs. After post found {len(new_subs)} subs"
        assert len(old_emails) < len(new_emails), \
            f"Before post found {len(old_emails)} emails. After post found {len(new_emails)} emails"
        print(f"Before {len(old_subs)} subs. After found {len(new_subs)} subs")
        print(f"Before {len(old_emails)} emails. After found {len(new_emails)} emails")
        ##########################################################################
        ### Testing create_email_events POST
        ##########################################################################
        # old_records = test_client.get_email_events()
        # old_records = test_client.get_email_events()
        # our_record = test_client.create_subscription_changes()
        # new_records = test_client.get_email_events()
        # assert len(old_records) < len(new_records), \
        #     f"Before post found {len(old_records)} records. After post found {len(new_records)} records"


        
    
