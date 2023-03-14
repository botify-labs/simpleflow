# Tags

[Tags](https://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dev-adv-tags.html) are free-form strings associated with a workflow execution by SWF, useful for filtering.
They can be specified in several ways:

* when starting the workflow, with the `--tags` option of
    `simpleflow workflow.start` or `tag_list` argument of
    `workflow_type.start_execution`
* in the workflow class definition: either a `tag_list` class variable or
    a `get_tag_list(cls, *args, **kwargs)` class method

Reusing a workflow parent’s tag list is a common use case. The special
variable `Workflow.INHERIT_TAG_LIST` allows this.

```python
from simpleflow import Workflow


class MyChildWorkflow(Workflow):
    ...

    tag_list = Workflow.INHERIT_TAG_LIST
```
