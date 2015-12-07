from simpleflow import activity, task


@activity.with_attributes(task_list='test')
def double(x):
    return x * 2

@activity.with_attributes(task_list='test')
class Double(object):
    def __init__(self, val):
        self.val = val

    def execute(self):
        return self.val * 2


def test_task_applies_function_correctly():
    assert task.ActivityTask(double, 2).execute() == 4


def test_task_applies_class_correctly():
    assert task.ActivityTask(Double, 4).execute() == 8


def test_task_register():
    registry = task.registry[None]
    assert registry['tests.test_task.double'] == double
    assert registry['tests.test_task.Double'] == Double
