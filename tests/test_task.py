from simpleflow import activity


@activity.with_attributes(task_list='test')
def increment(x):
    return x + 1


@activity.with_attributes(task_list='test')
def double(x):
    return x * 2


@activity.with_attributes(task_list='test')
def square(x):
    return x * x


def test_task_register():
    from simpleflow import task

    assert sorted(task.registry['test'].keys()) == sorted([
        'tests.test_task.increment',
        'tests.test_task.double',
        'tests.test_task.square',
    ])
    assert sorted(task.registry['test'].values()) == sorted([
        increment,
        double,
        square,
    ])
