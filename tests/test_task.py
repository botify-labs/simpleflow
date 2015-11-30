from simpleflow import activity


@activity.with_attributes(task_list='test')
def dummy_test_task(x):
    return x


@activity.with_attributes(task_list='test')
class DummyTask(object):
    def execute(self):
        pass


def test_task_register():
    from simpleflow import task

    registry = task.registry[None]
    assert registry['tests.test_task.dummy_test_task'] == dummy_test_task
    assert registry['tests.test_task.DummyTask'] == DummyTask
