Quickstart
==========

Let’s take a simple example that computes the result of `(x + 1) * 2`. You
will find this example in `examples/basic.py`.

We need to declare the functions as activities to make them available:

```python
import time

from simpleflow import activity

@activity.with_attributes(task_list="quickstart", version="example")
def increment(x):
    return x + 1

@activity.with_attributes(task_list="quickstart", version="example")
def double(x):
    return x * 2

@activity.with_attributes(task_list="quickstart", version="example")
def delay(t, x):
    time.sleep(t)
    return x
```

And then define the workflow itself in a `example.py` file:

```python
from simpleflow import (
  Workflow,
  futures,
)

from .basic import delay, double, increment


class BasicWorkflow(Workflow):
  name = "basic"
  version = "example"
  task_list = "example"

  def run(self, x, t=30):
    y = self.submit(increment, x)
    yy = self.submit(delay, t, y)
    z = self.submit(double, y)

    print(f"({x} + 1) * 2 = {z.result}")
    futures.wait(yy, z)
    return z.result
```

Now check that the workflow works locally with an integer "x" and a wait value "t":

    $ simpleflow workflow.start --local examples.basic.BasicWorkflow --input '[1, 5]'
    (1 + 1) * 2 = 4

**input** is encoded in JSON format and can contain the list of **positional**
arguments such as `'[1, 1]` or a **dict** with the `args` and `kwargs` keys
such as `{"args": [1], "kwargs": {}}`, `{"kwargs": {"x": 1}}`, or
`'{"args": [1], "kwargs": {"t": 5}}'`.

Now that you are confident that the workflow should work, you can run it on
Amazon SWF with the `standalone` command:

    $ simpleflow standalone --domain TestDomain examples.basic.BasicWorkflow --input '[1, 5]'

The **standalone** command sets a unique task list and manage all the processes
that are needed to execute the workflow: decider, activity worker, and a client
that starts the workflow. It is very convenient for testing a workflow by
executing it with SWF during the development steps or integration tests.

Let’s take a closer look to the workflow definition.

It is a **class** that inherits from `simpleflow.Workflow`:

```python
class BasicWorkflow(Workflow):
```

It defines 3 class attributes:

- **name**, the name of the SWF workflow type.
- **version**, the version of the SWF workflow type. It is currently provided
  only for labeling a workflow.
- **task_list**, the default task list (see it as a dynamically created queue)
  where decision tasks for this workflow will be sent. Any **decider** that
  listens on this task list can handle this workflow. This value can be
  overriden by the simpleflow commands and objects.

It also implements the `run` method that takes two arguments: `x` and
`t=30` (i.e. `t` is optional and has the default value `30`). These
arguments are passed with the `--input` option. The `run` method
describes the workflow and how its tasks should execute.

Each time a decider takes a decision task, it executes again the `run`
from the start. When the workflow execution starts, it evaluates `y =
self.submit(increment, x)` for the first time. **y** holds a future in state
`PENDING`. The execution continues with the line `yy = self.submit(delay, t,
y)`. **yy** holds another future in state `PENDING`. This state means the task
has not been scheduled. Now execution still continue in the `run` method
with the line `z = self.submit(double, y)`. Here it needs the value of the
**y** future to evaluate the `double` activity. As the execution cannot
continue, the decider schedules the task `increment`. **yy** is not a
dependency for any task, so it is not scheduled.

Once the decider has scheduled the task for **y**, it sleeps and waits for an
event to be wakened up. This happens when the `increment` task completes.
SWF schedules a decision task. A decider takes it and executes the
`BasicWorkflow.run` method again from the start. It evaluates the line `y
= self.submit(increment, x)`. The task associated with the **y** future has
completed. Hence, **y** is in state `FINISHED` and contains the value `2` in
`y.result`. The execution continues until it blocks. It goes by `yy =
self.submit(delay, t, y)` that stays the same. Then it reaches `z =
self.submit(double, y)`. It gets the value of `y.result` and **z** now holds a
future in state `PENDING`. Execution reaches the line with the `print`. It
blocks here because `z.result` is not available. The decider schedules the
task backs by the **z** future: `double(y)`. The workflow execution continues
so forth by evaluating the `BasicWorkflow.run` again from the start until
it finishes.
