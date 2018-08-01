# -*- encoding:utf-8 -*-
from __future__ import unicode_literals
import unittest
import os
import time
from tornado.testing import gen_test, AsyncTestCase
from util.async_process import AsyncProcess, AsyncProcessPool, AsyncThreadPool

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

    def tearDown(self):
        temp_file = "0"
        if os.path.exists(temp_file):
            os.remove(temp_file)
        super(TestAsyncProcess, self).tearDown()


def fib(n):
    print("begin calculating fibonacci at %d" % n)
    value = _fib(n)
    print("fib(%d)=%d" % (n, value, ))


def _fib(n):
    if n < 2:
        return n
    else:
        return _fib(n - 1) + _fib(n - 2)


def fib_except():
    raise Exception("Exception")


def fib_sleep(n):
    time.sleep(n)


class TestAsyncProcessPool(AsyncTestCase):
    def setUp(self):
        super(TestAsyncProcessPool, self).setUp()
        self.app = AsyncProcessPool(3)
        self._bash_file = _base_dir + "/test_bash.sh"

    @gen_test(timeout=10)
    def test_fib(self):
        values = [20, 19, 17, 2, 3]
        yield [self.app.submit(fib, value) for value in values]

    @gen_test
    def test_fib_expect(self):
        with self.assertRaises(Exception):
            yield self.app.submit(fib_except,)


class TestAsyncThreadPool(AsyncTestCase):
    def setUp(self):
        super(TestAsyncThreadPool, self).setUp()
        self.atp = AsyncThreadPool(3)

    @gen_test(timeout=10)
    def test_fib(self):
        values = [20, 19, 17, 2, 3]
        yield [self.atp.submit(fib, value) for value in values]

    @gen_test
    def test_fib_expect(self):
        with self.assertRaises(Exception):
            yield self.atp.submit(fib_except, )


if __name__ == "__main__":
    unittest.main()