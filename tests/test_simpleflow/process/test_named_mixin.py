import unittest

from psutil import Process
from sure import expect

from simpleflow.process import NamedMixin, with_state


class TestNamedMixin(unittest.TestCase):
    def test_with_state_decorator(self):
        # example class used below
        class Example(NamedMixin):
            @with_state("running")
            def run(self):
                pass

        # tests
        inst = Example()
        expect(Process().name()).to.equal("simpleflow Example()[initializing]")

        inst.run()
        expect(Process().name()).to.equal("simpleflow Example()[running]")

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
        expect(Process().name()).to.equal(
            "simpleflow Example(task_list=test-tl)[running]"
        )
