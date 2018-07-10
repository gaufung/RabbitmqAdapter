"""
Unit test of common.utils
"""
from __future__ import unicode_literals
from __future__ import print_function
import unittest
from http_util import generate_url, _get_content_type


class TestGenerateUrl(unittest.TestCase):
    """
    Unit test generate_url
    """
    def test_error(self):
        with self.assertRaises(Exception):
            generate_url("none", "none", "none", None)

    def test_generate_url(self):
        test_cases = [
            {
                "url": "http://www.baidu.com/api",
                "router_path": "/aihome",
                "protocol": "http",
                "query_args": {"name": "aios"},
                "expect": "http://www.baidu.com/api/aihome?name=aios",
            },
            {
                "url": "wss://www.baidu.com/api",
                "router_path": "/aihome",
                "protocol": "ws",
                "query_args": {"name": "aios"},
                "expect": "wss://www.baidu.com/api/aihome?name=aios",
            },
            {
                "url": "https://www.baidu.com/api",
                "router_path": "/aihome",
                "protocol": "ws",
                "query_args": None,
                "expect": "wss://www.baidu.com/api/aihome",
            }
        ]
        for case in test_cases:
            actual = generate_url(case["url"], case["router_path"], case["protocol"], case["query_args"])
            self.assertEqual(actual, case["expect"])


class TestPostMultiFiles(unittest.TestCase):
    def test_get_content_type(self):
        test_cases = [
            {
                "file_name": "a.json",
                "expect": "application/json",
            },
            {
                "file_name": "b.html",
                "expect": "text/html",
            },
            {
                "file_name": "c.nothing",
                "expect": "application/octet-stream"
            }
        ]
        for case in test_cases:
            self.assertEqual(_get_content_type(case["file_name"]), case["expect"])


if __name__ == "__main__":
    unittest.main()
