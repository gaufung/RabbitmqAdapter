# -*- encoding:utf-8 -*-
import unittest

import redis
from redlock import RedLock, RedLockError


class TestRedLock(unittest.TestCase):
    def setUp(self):
        super(TestRedLock, self).setUp()
        self._host = "10.12.6.35"
        self._port = 6380
        self._password = '6Qhq8BeRWE7WYOLqRjhwP233ujB1zSPR'

    def tearDown(self):
        super(TestRedLock, self).tearDown()

    def test_with(self):
        resource = "mysource"
        conn1 = redis.StrictRedis(host=self._host,
                                  port=self._port,
                                  password=self._password,
                                  db=11)
        with RedLock(resource, [conn1]):
            pass

        with RedLock(resource, [conn1]):
            with self.assertRaises(RedLockError):
                with RedLock(resource, [conn1]):
                    pass

        conn2 = redis.StrictRedis(host=self._host,
                                  port=self._port,
                                  password=self._password,
                                  db=12)
        with RedLock(resource, [conn1]):
            with RedLock(resource, [conn2]):
                pass

    def test_acquire_release(self):
        resource = "yoursource"
        conn1 = redis.StrictRedis(host=self._host,
                                  port=self._port,
                                  password=self._password,
                                  db=11)
        locker1 = RedLock(resource, [conn1])
        result = locker1.acquire()
        self.assertTrue(result)
        is_locked = locker1.locked()
        self.assertTrue(is_locked)
        locker2 = RedLock(resource, [conn1])
        try_result = locker2.acquire()
        self.assertFalse(try_result)
        conn2 = redis.StrictRedis(host=self._host,
                                  port=self._port,
                                  password=self._password,
                                  db=13)
        locker3 = RedLock(resource, [conn2])
        result = locker3.acquire()
        self.assertTrue(result)
        locker3.release()
        locker1.release()


if __name__ == "__main__":
    unittest.main()
