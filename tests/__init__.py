import unittest

from . import _runner


def runner():
    return _runner.ParallelTextTestRunner


def suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('.', pattern='test_*.py')
    return test_suite
