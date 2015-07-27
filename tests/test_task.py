from simpleflow import activity


@activity.with_attributes(task_list='test')
def dummy_test_task(x):
    return x


def test_task_register():
    from simpleflow import task

    assert 'tests.test_task.dummy_test_task' in task.registry[None].keys()
    assert dummy_test_task in task.registry[None].values()
