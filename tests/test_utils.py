import unittest

from fluvio.utils import parse_byte_size
from humanfriendly import InvalidSize


class TestParsing(unittest.TestCase):
    def test_parse_byte_size(self):
        """Test byte size string parsing"""
        # Direct bytes
        self.assertEqual(parse_byte_size('1024'), 1024)

        # Kilobytes
        self.assertEqual(parse_byte_size('1K'), 1000)
        self.assertEqual(parse_byte_size('2K'), 2 * 1000)

        # Megabytes
        self.assertEqual(parse_byte_size('1M'), 1000 * 1000)
        self.assertEqual(parse_byte_size('1Mib'), 1024 * 1024)
        self.assertEqual(parse_byte_size('1MiB'), 1024 * 1024)
        self.assertEqual(parse_byte_size('500M'), 500 * 1000 * 1000)
        self.assertEqual(parse_byte_size('500MiB'), 500 * 1024 * 1024)

        # Gigabytes
        self.assertEqual(parse_byte_size('1G'), 1000 * 1000 * 1000)
        self.assertEqual(parse_byte_size('2G'), 2 * 1000 * 1000 * 1000)
        self.assertEqual(parse_byte_size('2Gib'), 2 * 1024 * 1024 * 1024)

        # Terabytes
        self.assertEqual(parse_byte_size('1T'), 1000 * 1000 * 1000 * 1000)
        self.assertEqual(parse_byte_size('1TiB'), 1024 * 1024 * 1024 * 1024)

        # Invalid formats
        with self.assertRaises(InvalidSize):
            parse_byte_size('1x')
        with self.assertRaises(InvalidSize):
            parse_byte_size('abc')
