import unittest

from psutil import Process
from sure import expect

from simpleflow.process import NamedMixin, with_state


class Example(NamedMixin):
    @with_state("running")
    def run(self):
        pass


class TestNamedMixin(unittest.TestCase):
    def test_with_state_decorator(self):
        inst = Example()
        expect(Process().name()).to.equal("simpleflow Example[initializing]")

        inst.run()
        expect(Process().name()).to.equal("simpleflow Example[running]")
