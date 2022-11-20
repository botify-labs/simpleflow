# Steps

Steps allow to group activities at a logical level independently of the
workflows, with interdependencies, skipped and forced replay, and much more.

They are implemented as S3 objects and SWF markers.

A step is defined mainly with:
* a name
* a submittable to run if not already done
* an optional submittable to run otherwise
* an optional list of step names depending on this step being run

It can emit a signal (that happens whether the step runs or is skipped)
and have the same `bubbles_exception_on_failure` attribute as a group.

A step can be forced for several reasons:
* at submission time, as determined by business logic
* if it is a dependency of an executed step

A step can also be explicitly skipped, although the "force" logic takes
precedence over the "skip" one.

Step names are hierarchical, using a dotted notation: forcing "a" will
also force "a.b.c". "*" forces or skips everything.

## Using Steps

### Basic Example

A workflow using steps should derive from the `WorkflowStepMixin` mixin.
One then submits either `self.step` or a `Step`:

```python
from simpleflow import Workflow, activity, futures
from simpleflow.step.workflow import WorkflowStepMixin


@activity.with_attributes(task_list="example", version="example", idempotent=True)
def do_something():
    return "this is something"


class AWorkflow(Workflow, WorkflowStepMixin):
    # ...
    def run(self, **context):
        futures.wait(self.submit(self.step("something", do_something)))
```

The first time `AWorkflow` is executed, the step "something" is run;
next time the workflow is executed, the step is skipped.


### Creating a Step

The `Step.__init__` method is called with:
* `step_name: str`: step name
* `activities: Submittable | SubmittableContainer`: what to execute (generally a chain or a group, thus the plural)
* `force: bool=False`: whether to force a step even if previously played
* `activities_if_step_already_done: Submittable | SubmittableContainer | None=None`: what to execute if the step is skipped
* `emit_signal: bool=False`: whether to emit a `signal.{step_name}` signal sent after the step is played/skipped
* `force_steps_if_executed: list[str] | None=None`: dependent steps to play next if this one isn’t skipped
* `bubbles_exception_on_failure: bool=False`: flag applied to the chain encapsulating the step

The `WorkflowStepMixin.step` method delegates to `Step`.


### Internals

A submitted step is executed in different phases:
* get the list of done steps
* create a chain
* if we should run the step (not done or forced):
  * determine the step was force-run or not
  * add dependent steps to the forced steps list
  * add to the chain:
    * add a SWF marker logging the step as scheduled
    * run the step
    * mark the step as done
    * add a SWF marker logging the step as completed
* otherwise:
  * determine whether the step was force-skipped or not
  * add to the chain:
    * run the alternative submittable ("activities_if_step_already_done")
    * add a SWF marker logging the step as skipped
* if configured so, add to the chain a SWF signal named "step.{step_name}"

The chain is then submitted to the workflow.

Done steps are tracked by S3 objects. We only list them, their content is not used.

## Customization

The `WorkflowStepMixin` class supports several customizations; see for instance `examples.step.CustomizedStepWorkflow`.

* `get_step_bucket()`: return the bucket name, potentially with the region endpoint.
  Default: `{SIMPLEFLOW_S3_HOST}/{STEP_BUCKET}`
* `get_step_path_prefix()`: return the steps path prefix. Default: `{workflow_id}/steps`
* `get_step_activity_params()`: return supplemental activity parameters for `GetStepsDoneTask` and `MarkStepDoneTask`.
  A typical use is forcing a separate task list.
