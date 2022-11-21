Error Handling
==============

!!! warning
    This feature is in _beta_ mode and subject to changes. Any feedback is appreciated.

What follows applies to the SWF executor; the local one only handles `raises_on_failure`.

A workflow can declare a method to be called when a task or child workflow fails or times out.
By default, errors are handled as follows:

*  a`retry` attribute defines how many times the task or workflow is retried; it defaults to 0 (no retry)
* if too many retries were attempted, the `raises_on_failure` attribute is checked. If False, a future is returned with
    its `exception` set; if True, the workflow is aborted.

Groups and Chains have `raises_on_failure` too, along with `bubbles_exception_on_failure` and (for Chains)
`break_on_failure`, allowing for more fine-grained control.

If defined, `Workflow.on_task_failure` is called before the `Executor.default_failure_handling` default error handling.
It is passed a `TaskFailureContext` object with error details and can modify it or return a new `TaskFailureContext`
instance.

The `TaskFailureContext` currently have these members:

* a_task: failed `ActivityTask` or `WorkflowTask`
* task_name: activity or workflow name
* exception: raised exception (shortcut to future.exception)
* retry_count: current retry count (0 for first retry)
* task_error: for a TaskFailed exception, name of the inner exception if available
* future: failed future
* event: quite opaque dict, experimental
* history: History object, experimental
* decision: described below
* retry_wait_timeout: ditto

The `TaskFailureContext.decision` (nothing in common with SWF’s decisions) should be set by `on_task_failure`. It can be:

* none: default value, continue with default handling (potential retries then abort)
* abort: use default abort strategy
* ignore: discard the exception, consider the task finished; the future’s result may be user-modified
* cancel: mark the future as cancelled
* retry: schedule the task again; its args and kwargs may be user-modified (well, the whole task)
* retry_later: schedule the task after retry_wait_timeout seconds, with args and kwargs potentially altered
* handled: `on_task_failure` somewhat handled the failure; use the future and task it has possibly modified (one
    strategy here is for `on_task_failure` to call the executor’s `default_failure_handling` method, and make
    workflow-specific processing according to its return value)

The `Workflow.on_task_failure` method is guaranteed to be called only once on a given replay, but may be called
again with the same failure in subsequent replays: it must thus be idempotent.
