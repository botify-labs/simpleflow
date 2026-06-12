# Task Lists

Task lists are often used to route different tasks to specific groups
of workers.
The decider and activity task lists are distinct, even if they have the same name.

For SWF activities, the task list is typically specified with `@activity.with_attributes`:

```python
from simpleflow import activity


@activity.with_attributes(task_list="quickstart", version="example")
def double(x):
    return x * 2
```

Dynamic task lists are possible:


```python
from simpleflow import activity, Workflow


def run_on(func, task_list, **kwargs):
    return activity.with_attributes(task_list=task_list, **kwargs)(func)


def double(x):
    return x * 2


class MyWorkflow(Workflow):
    ...

    def run(self, x, task_list, *args, **kwargs):
        result = self.submit(run_on(double, task_list), x).result
        print(f"Result: {result}")
```

```bash
[screen0]$ simpleflow decider.start examples.dyn_task_list.BasicWorkflow --task-list foo-decider
[screen1]$ simpleflow worker.start --task-list foo-worker
[screen2]$ simpleflow workflow.start examples.dyn_task_list.BasicWorkflow --task-list foo-decider --input '{"args": [3, "foo-worker"]}'
```

For SWF workflows, a static task list is usually defined as a class variable
in the `Workflow` subclass. Dynamic task lists are implemented by a
`get_task_list` class method:

```python
from simpleflow import Workflow


class MyWorkflow(Workflow):
    ...

    @classmethod
    def get_task_list(cls, task_list, *args, **kwargs):
        return task_list

    def run(self, x, task_list, *args, **kwargs):
        ...
```

In this example, `task_list` is a mandatory workflow argument; a more realistic
case would use a `kwarg`.

## Child workflows

By default, a child workflow is launched on the task list determined by its own
`task_list` class variable or `get_task_list` class method, just like a
top-level workflow. To override this on a per-submission basis — for instance to
make a child workflow inherit its parent's decision task list — pass
`workflow_task_list` when building the `WorkflowTask`:

```python
from simpleflow import Workflow
from simpleflow.task import WorkflowTask


class MyChildWorkflow(Workflow):
    ...


class MyWorkflow(Workflow):
    ...

    def run(self, *args, **kwargs):
        # Run the child workflow on this workflow's own decision task list.
        self.submit(
            WorkflowTask(
                self._executor,
                MyChildWorkflow,
                *args,
                workflow_task_list=self._executor.task_list,
            )
        )
```

`workflow_task_list` takes precedence over the child workflow's own
`task_list`/`get_task_list`. It is purely a decision-routing concern: it is
never forwarded as an argument to the child workflow's `run()`.

!!! warning
    `workflow_task_list` is a reserved keyword argument of `WorkflowTask`.
