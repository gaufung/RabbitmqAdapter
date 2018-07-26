"""
    unit-test of common.ps_util module
"""
import unittest
import sys
from util.ps import ProcessCpuTimesX, ProcessMemoryX, SystemResource, Resource


class TestProcessCpuTimesX(unittest.TestCase):
    def setUp(self):
        self.a = ProcessCpuTimesX([0, 8, 2, 9])
        self.b = ProcessCpuTimesX([3, 2, 7, 9])

    def test_sub(self):
        res = self.a - self.b
        self.assertEquals(res.user, -3)
        self.assertEquals(res.system, 6)
        self.assertEquals(res.children_user, -5)
        self.assertEquals(res.children_system, 0)

    def test_add(self):
        res = self.a + self.b
        self.assertEquals(res.user, 3)
        self.assertEquals(res.system, 10)
        self.assertEquals(res.children_user, 9)
        self.assertEquals(res.children_system, 18)

    def test_iadd(self):
        self.a += self.b
        self.assertEquals(self.a.user, 3)
        self.assertEquals(self.a.system, 10)
        self.assertEquals(self.a.children_user, 9)
        self.assertEquals(self.a.children_system, 18)

    def test_average(self):
        av = self.a.average(None)
        self.assertEqual(0, av)
        self.a += self.b
        av = self.a.average(10)
        expect = float(self.a.user + self.a.system) / 10 * 100
        self.assertEqual(av, expect)


class TestProcessMemoryX(unittest.TestCase):
    def setUp(self):
        # darwin
        if sys.platform.startswith("darwin"):
            self.a = ProcessMemoryX([0, 1, 2, 3])
            self.b = ProcessMemoryX([5, 6, 7, 8])
            self.res = [5, 7, 9, 11]
        # linux
        else:
            self.a = ProcessMemoryX([0, 1, 2, 3, 4, 5, 9])
            self.b = ProcessMemoryX([9, 8, 7, 6, 5, 4, 0])
            self.res = [9, 9, 9, 9, 9, 9, 9]

    def test_add(self):
        res = self.a + self.b
        for val1, val2 in zip(res, self.res):
            self.assertEquals(val1, val2)

    def test_radd(self):
        self.a += self.b
        for val1, val2 in zip(self.a, self.res):
            self.assertEquals(val1, val2)


class TestSystemResource(unittest.TestCase):
    def test_take_snapshot(self):
        sr = SystemResource()
        sr.take_snapshot()
        self.assertEquals(1, len(sr._snapshots))

    def test_clear_expired_snapshot(self):
        sr = SystemResource()
        for _ in range(10):
            sr.take_snapshot()
        sr.set_sample("hello")
        sr.take_snapshot()
        sr.clear_expired_snapshot(10)
        self.assertEquals(1, len(sr._snapshots))

    def test_clear_monitor(self):
        sr = SystemResource()
        r1 = sr.get_sample("none")
        self.assertIsNone(r1)
        sr.take_snapshot()
        sr.set_sample("first")
        r2 = sr.get_sample("first")
        self.assertTupleEqual(Resource(0, 0, 0, 0), r2)


if __name__ == "__main__":
    unittest.main()
