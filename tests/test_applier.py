from simpleflow import Applier


def double(x):
    return x * 2

class Double(object):
    def __init__(self, val):
        self.val = val

    def execute(self):
        return self.val * 2


def test_applier_applies_function_correctly():
    assert Applier(double, 2).call() == 4


def test_applier_applies_class_correctly():
    assert Applier(Double, 4).call() == 8
