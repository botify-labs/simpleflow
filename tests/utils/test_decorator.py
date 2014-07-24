import unittest
import os
import tempfile
import shutil

from cdf.tasks.decorators import with_temporary_dir as with_temp_dir


@with_temp_dir
def task_func(some_param, tmp_dir=None):
    """A test function which verifies tmp_dir and returns it"""
    if tmp_dir is None:
        raise Exception()
    return tmp_dir




class TestTempDirDecorator(unittest.TestCase):
    def test_temp_dir_with_clean(self):
        temp_dir = task_func("param")

        # after the call the temp dir should be removed
        self.assertFalse(os.path.isdir(temp_dir))
        self.assertFalse(os.path.exists(temp_dir))

    def test_temp_dir_no_clean(self):
        # always clean when in temp dir
        temp_dir = task_func("param", cleanup=True)

        # after the call the temp dir should be removed
        self.assertFalse(os.path.isdir(temp_dir))
        self.assertFalse(os.path.exists(temp_dir))

    def test_normal_execution_with_clean(self):
        temp_dir_param = tempfile.mkdtemp()
        # `tmp_dir` is given as a named argument
        # `cleanup` is set to True
        temp_dir = task_func("param", tmp_dir=temp_dir_param,
                             cleanup=True)

        # temp dir should not be modified
        self.assertEqual(temp_dir, temp_dir_param)

        # after the call the temp dir should be removed
        self.assertFalse(os.path.isdir(temp_dir))
        self.assertFalse(os.path.exists(temp_dir))

    def test_normal_execution_no_clean(self):
        # no `tmp_dir`, clean anyway
        temp_dir = task_func("param")

        # after the call the temp dir should be removed
        self.assertFalse(os.path.isdir(temp_dir))
        self.assertFalse(os.path.exists(temp_dir))

        # with `tmp_dir`, but no cleanup
        temp_dir_param = tempfile.mkdtemp()
        # `tmp_dir` is given as a named argument
        # `cleanup` defaults to False
        temp_dir = task_func("param", tmp_dir=temp_dir_param)

        # temp dir should not be modified
        self.assertEqual(temp_dir, temp_dir_param)

        # after the call the temp dir should be removed
        self.assertTrue(os.path.isdir(temp_dir))
        self.assertTrue(os.path.exists(temp_dir))

        shutil.rmtree(temp_dir)

    def test_tmp_dir_param_none(self):
        # temp dir and cleanup
        temp_dir = task_func("param", tmp_dir=None)

        self.assertIsNotNone(temp_dir)

        # after the call the temp dir should be removed
        self.assertFalse(os.path.isdir(temp_dir))
        self.assertFalse(os.path.exists(temp_dir))

        # cleanup option will not affect this, clean anyway
        temp_dir = task_func("param", tmp_dir=None, cleanup=False)

        self.assertIsNotNone(temp_dir)

        # after the call the temp dir should be removed
        self.assertFalse(os.path.isdir(temp_dir))
        self.assertFalse(os.path.exists(temp_dir))

    def test_exeption(self):
        class TestException(Exception):
            def __init__(self):
                pass

        @with_temp_dir
        def task_exception(tmp_dir=None):
            raise TestException()

        self.assertRaises(TestException, task_exception)
