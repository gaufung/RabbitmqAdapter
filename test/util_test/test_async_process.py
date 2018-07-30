# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import unittest
import os
from tornado.testing import gen_test, AsyncTestCase
from util.async_process import AsyncProcess

_base_dir = os.path.dirname(os.path.abspath(__file__))


class TestAsyncProcess(AsyncTestCase):
    def setUp(self):
        super(TestAsyncProcess, self).setUp()
        self._bash_file = _base_dir + "/test_bash.sh"

    @gen_test(timeout=5)
    def test_normal_exit(self):
        ap = AsyncProcess(["bash", self._bash_file, "0", "0"])
        yield ap.invoke()
        self.assertEqual(ap._exit_code, 0)

    @gen_test(timeout=5)
    def test_bad_exit_with_2(self):
        ap = AsyncProcess(["bash", self._bash_file, "2", "0"])
        yield ap.invoke()
        self.assertEqual(ap._exit_code, 2)

    @gen_test(timeout=5)
    def test_bad_exit_exception(self):
        ap = AsyncProcess(["bash", self._bash_file, "1", "0"])
        with self.assertRaises(Exception):
            yield ap.invoke()

    @gen_test(timeout=5)
    def test_timeout_exception(self):
        ap = AsyncProcess(["bash", self._bash_file, "1", "2"], timeout=1)
        with self.assertRaises(Exception):
            yield ap.invoke()

    @gen_test(timeout=5)
    def test_timeout_noram(self):
        ap = AsyncProcess(["bash", self._bash_file, "0", "1"], timeout=2, is_logging=False)
        yield ap.invoke()
        self.assertEqual(ap._exit_code, 0)

    def tearDown(self):
        temp_file = "0"
        if os.path.exists(temp_file):
            os.remove(temp_file)
        super(TestAsyncProcess, self).tearDown()


if __name__ == "__main__":
    unittest.main()