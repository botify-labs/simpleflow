# Canvas Features

Simpleflow handles task grouping above the SWF level, using canvas
concepts from [Celery](https://docs.celeryq.dev/en/stable/userguide/canvas.html):
Chains and Groups. It also allows delayed function execution with FuncGroups.

## Chains

A Chain links tasks together sequentially.

```python
from simpleflow import activity, Workflow
from simpleflow.canvas import Chain


@activity.with_attributes(task_list="quickstart", version="example")
def task_a():
    return "Something"


@activity.with_attributes(task_list="quickstart", version="example")
def task_b(x):
    return "Something Else", x


class AWorkflow(Workflow):
    # ...
    def run(self, *args, **kwargs):
        futures = self.submit(Chain(task_a, (task_b, 42)))
        print(f"Results: {futures.result}")
```

The future’s result is the list of each chained task result once they
are finished.

The `Chain.__init__` method takes a series of submittable objects or
`(submittable, ...args)` tuples.

The tasks can also be added after creating the chain, using the `append`
method.

```python
from simpleflow import Workflow
from simpleflow.canvas import Chain


class AWorkflow(Workflow):
    # ...
    def run(self, *args, **kwargs):
        chain = Chain()
        chain.append(task_a)
        chain.append(task_b, x=42)
        futures = self.submit(chain)
        print(f"Results: {futures.result}")
```

This allows the use of named arguments. Finally, appended tasks can
directly be `ActivityTask` or `WorkflowTask` for maximum flexibility.

!!! warning
    These are obviously too many ways of specifying tasks.


### Sending Results

Each task in a chain can send its results to the next one, by using the
`send_results=True` argument. The result is then added to the succeeding
task’s `*args`.


### Error Handling

By default, an error in a task will break the chain and bubble the
exception up. This can be controlled with several arguments:

* `raises_on_failure` (default: True) — bubble-up on failure
* `break_on_failure` (default: True) — break on failure
* `bubbles_exception_on_failure` (default: False) — in a sub-chain,
  report the not-raised failure to the upper chain

See `examples.canvas.CanvasWorkflow` for what’s happening in the
different cases.

`raises_on_failure` is propagated to the group’s content if set. That is:
* `chain = Chain(raises_on_failure=False); chain.append(some_activity)`
  will propagate `raises_on_failure=False` to `some_activity`;
* `chain = Chain(); chain.append(some_activity); chain.raises_on_failure = False`
  will not.

The `break_on_failure=False` and `send_results=True` options are
currently incompatible.

## Groups

A Group represents independent tasks that are scheduled in parallel.

```python
from simpleflow import activity, Workflow
from simpleflow.canvas import Group


@activity.with_attributes(task_list="quickstart", version="example")
def task_a():
    return "Something"


@activity.with_attributes(task_list="quickstart", version="example")
def task_b(x):
    return "Something Else", x


class AWorkflow(Workflow):
    # ...
    def run(self, *args, **kwargs):
        futures = self.submit(Group(task_a, (task_b, 42)))
        print(f"Results: {futures.result}")
```

Defining a Group is similar to a Chain. The `raises_on_failure` and
`bubbles_exception_on_failure` arguments are the same. An extra argument
is `max_parallel`, which specifies how many tasks can be scheduled at a
given time.


## FuncGroup

A `FuncGroup` instance encapsulates a function called by the executor
and returning a Chain or Group to execute. It can be seen as a barrier:
in a Chain, the function will be called after the previous task.

!!! warning
    Untested code

```python
from simpleflow import activity, Workflow
from simpleflow.canvas import Chain, FuncGroup, Group


@activity.with_attributes(task_list="quickstart", version="example")
def partition_data(data_location):
    # Partition a list of things to do into parallelizable sub-parts
    pass


@activity.with_attributes(task_list="quickstart", version="example")
def execute_on_sub_part(sub_part):
    pass


class AWorkflow(Workflow):
    # ...
    def run(self, *args, **kwargs):
        chain = Chain(send_result=True)
        chain.append(partition_data, data_location="s3://my_bucket/foo")
        chain.append(FuncGroup(lambda parts: Group(*[(execute_on_sub_part, sub_part) for sub_part in parts])))
```

Here, `partition_data`’s result is passed to the `FuncGroup` lambda,
which returns a `Group` parallelizing its execution.


## Advanced Uses

### No-result `FuncGroup`

The function encapsulated in a `FuncGroup` must return a result; this is
inconvenient when its job is limited to the workflow state, and must thus
return an empty Group. Since this has been a long-standing policy, a new
`_allow_none` argument relaxes this constraint.

!!! warning
    This is a new experimental option: a better one might be to enforce
    that nothing is returned.
