import unittest
from fluvio.cloud import login


class TestFluvioCloudLogin(unittest.TestCase):
    def test_login(self):
        login()
        # Comment/uncomment these as needed.
        #login(profile='test-profile')
        #login(useOauth2=False, email='youremail@gmail.com', password='PUT_YOURPASSWORD_HERE')
        #login(useOauth2=False)
