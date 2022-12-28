import unittest
from fluvio.cloud import login


class TestFluvioCloudLogin(unittest.TestCase):
    def test_login(self):
        login()
