from __future__ import annotations

import unittest

from psutil import Process
from pytest import mark

from simpleflow.process import NamedMixin, with_state


class TestNamedMixin(unittest.TestCase):
    @mark.skip("flaky test based on time.sleep")
    # @mark.xfail(platform.system() == 'Darwin', reason="setproctitle doesn't work reliably on MacOSX")
    def test_with_state_decorator(self):
        # example class used below
        class Example(NamedMixin):
            @with_state("running")
            def run(self):
                pass

        # tests
        inst = Example()
        assert Process().name() == "simpleflow Example()[initializing]"

        inst.run()
        assert Process().name() == "simpleflow Example()[running]"

    @mark.skip("flaky test based on time.sleep")
    # @mark.xfail(platform.system() == 'Darwin', reason="setproctitle doesn't work reliably on MacOSX")
    def test_named_mixin_exposed_properties(self):
        # example class used below
        class Example(NamedMixin):
            def __init__(self):
                self._named_mixin_properties = ["task_list"]
                self.task_list = "test-tl"

            @with_state("running")
            def run(self):
                pass

        # tests
        inst = Example()
        inst.run()
        assert Process().name() == "simpleflow Example(task_list=test-tl)[running]"
