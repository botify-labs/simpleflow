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


print increment.name

def test_task_register():
    from simpleflow import task

    assert task.registry['test'] == [increment, double, square]
