# -*- encoding:utf-8 -*-
import os
import unittest
import coverage

COV = None
COV = coverage.coverage(branch=True, include="test/*")
COV.start()


def _test():
    tests = unittest.TestLoader().discover("test")
    unittest.TextTestRunner(verbosity=2).run(tests)
    COV.stop()
    COV.save()
    print("Coverage Summary: ")
    COV.report()
    basedir = os.path.abspath(os.path.dirname(__file__))
    covdir = os.path.join(basedir, "tmp/coverage")
    COV.html_report(directory=covdir)
    print("HTML version: file://%s//index.html" % (covdir))
    COV.erase()


_test()
# COV = None
#
# import
#
#
# import unittest
#
# tests = unittest.TestLoader().discover("test")
# unittest.TextTestRunner(verbosity=3).run(tests)