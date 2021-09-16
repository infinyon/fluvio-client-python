from fluvio import (Fluvio, FluviorError)
import unittest


class TestFluvioCannotConnect(unittest.TestCase):
    '''
    These tests show that the python links to the rust correctly but that
    fluvio is not running. This is because the macOS github runner does not
    support docker.
    '''
    def test_connection_failure(self):

        error = None
        try:
            Fluvio.connect()
        except FluviorError as e:
            error = e
            print('ERROR: %s' % e)

        self.assertTrue(error is not None)
        self.assertIn(
            error.args,
            [
                ('Fluvio config error: Config has no active profile\nCaused by:\nConfig has no active profile',), # noqa: E501
                ('Fluvio socket error: Connection refused (os error 61)\nCaused by:\nConnection refused (os error 61)',),  # noqa: E501
            ]
        )
