# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import unittest
from util import remove_punctuations


class TestRemovePunctuations(unittest.TestCase):
    def test_remove_punctuations(self):
        test_cases = [
            {
                "input": "",
                "expect": "",
            },
            {
                "input": "~.。?？！!:：…～﹏\"\'`”“·•°\t\n;、/,，$=《》；",
                "expect": "",
            },
            {
                "input": "aispeech~dui?aios!《ba》",
                "expect": "aispeechduiaiosba",
            }
        ]
        for test in test_cases:
            actual = remove_punctuations(test["input"])
            self.assertEqual(actual, test["expect"])


if __name__ == "__main__":
    unittest.main()
