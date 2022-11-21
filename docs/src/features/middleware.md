# Middleware

!!! warning
    This feature is in _beta_ mode and subject to changes. Any feedback is appreciated.

## Presentation

Simpleflow allows for the execution of functions before and after the
execution of an Activity.

To do this, you may pass functions as paths when running in standalone
mode or when launching a worker (`worker.start` command):

```
simpleflow standalone
		--domain TestDomain \
		--nb-deciders 1 \
		--nb-workers 1 \
		--middleware-pre-execution module.path.pre.execution.function \
		--middleware-pre-execution module.path.pre.execution.second.function \
		--middleware-post-execution module.path.post.execution.function \
		myWorkflow \
		--input '{"args":[], "kwargs": {"task_list":"test"}}'
```

The above example will execute two functions before each Activity code,
and one after.

The `--middleware-pre-execution` and `--middleware-post-execution`
arguments are also accepted by the `workflow.start` command in
`--standalone` mode.

## Writing a middleware function

Middleware pre-execution functions receive the activity context.
Middleware post-execution functions receive the activity context and
result.

```
def my_pre_execution_func(activity_context, **kwargs):
  pass

def my_post_execution_func(activity_context, result, **kwargs):
  print("activity result:", "result")
```
