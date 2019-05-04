# Canvas: Groups, Chains, and FuncGroups

In addition to the native SWF building blocks, simpleflow provides
grouping primitives: `Group`s, `Chain`s and `FuncGroup`s loosely modeled
after the
[Celery canvas primitives](http://docs.celeryproject.org/en/latest/userguide/canvas.html).

A `Group` submits a list of tasks in parallel.
A `Chain` submits a succession of tasks, optionally passing each task's result
to the next one.
A `FuncGroup` allows for deferred submission: it is a function returning
a group or chain of tasks, assumed to be based on the result of previous tasks.


## Chains

Chains define serialized tasks.

...

## Groups

Groups are parallel tasks. Every task are submitted at once by default; a limit
can be set with the `max_parallel` argument.

...


## FuncGroups

FuncGroups are functions executed on the decider, returning a chain or group of
tasks to be submitted.

The following code example executes a different activity according to a random
result.

```python
from __future__ import print_function
import random

from simpleflow import activity, Workflow
from simpleflow.canvas import Chain, FuncGroup, Group


@activity.with_attributes(task_list='example', version='example')
def random_value():
    return random.randint(1, 10)


@activity.with_attributes(task_list='example', version='example')
def no_worry():
    return "Don't worry!"


@activity.with_attributes(task_list='example', version='example')
def panic():
    return "oh oh."


class MyWorkflow(Workflow):
    name = 'canvas'
    version = 'example'
    task_list = 'example'

    def run(self):
        future = self.submit(
            Chain(
                random_value,
                FuncGroup(
                    lambda result: Group(no_worry) if result < 5 else Group(*([panic] * result))
                ),
                send_result=True,
            )
        )
        print(future.result)
```


## Exception Handling

By default, chains and groups don't mess with their tasks' failure handling;
however, two arguments can be passed to their constructors: `raises_on_failure`
and `bubbles_exception_on_failure`.

If `raises_on_failure` is passed (and is not `None`), it is recursively
propagated to the tasks (but without changing the sub- chains or groups
`raises_on_failure` attribute).

...

## Results Passing

examples/canvas.py; examples/canvas_with_funcgroup.py

...
