Error Handling
==============

!!! warning
    This feature is in _beta_ mode and subject to changes. Any feedback is appreciated.

What follows applies to the SWF executor; the local one only handles `raises_on_failure`.

A workflow can declare a method to be called on an activity or child workflow error, that is, either an
`ActivityTaskFailed`, `ActivityTaskTimedOut`, `ChildWorkflowExecutionFailed` or `ChildWorkflowExecutionTimedOut`.
In its absence, errors are handled as follow:

*  a`retry` attribute defines how many times the task is retried; it defaults to 0 (no retry)
* if too many retries were attempted, the `raises_on_failure` attribute is checked. If False, a future is returned with 
    its `exception` set; if True, the workflow is aborted. 

Groups and Chains have `raises_on_failure` too, along with `bubbles_exception_on_failure` and (for Chains)
`break_on_failure`, allowing for more fine-grained control.

If defined, `Workflow.on_task_failure` is called before the default error handling. It is passed a `TaskFailureContext` 
object with error details and can modify it or return a new `TaskFailureContext` instance.

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

The `TaskFailureContext.decision` (nothing in common with SWF's decisions) is set by `on_task_failure`. It can be:

* none: continue with default handling (potential retries then abort)
* abort: use default abort strategy
* ignore: discard the exception, consider the task finished; the future's result may be user-modified
* cancel: mark the future as cancelled
* retry: schedule the task again; its input may be user-modified (well, the whole task)
* retry_later: schedule the task after retry_wait_timeout seconds


