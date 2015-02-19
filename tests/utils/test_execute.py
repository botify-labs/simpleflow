from __future__ import absolute_import

import unittest
import mock

from cdf.utils import execute


def inc(x):
    return x + 1


class TestPypy(unittest.TestCase):
    def test_execution_with_pypy(self):
        task = execute.with_pypy()(inc)

        x = 1
        self.assertEquals(task(x), inc(x))

    def test_execution_with_pypy_disable(self):
        task = execute.with_pypy(enable=False)(inc)

        x = 1
        self.assertEquals(task(x), inc(x))

    def test_execution_with_pypy_not_installed(self):
        import cdf.settings
        tmp = cdf.settings.PYPY_PATH
        cdf.settings.PYPY_PATH = '/tmp/nowhere'
        task = execute.with_pypy()(inc)

        x = 1
        self.assertEquals(task(x), inc(x))
        cdf.settings.PYPY_PATH = tmp
