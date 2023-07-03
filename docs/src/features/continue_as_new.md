# Continue As New

In long-running workflow executions, the history can hit the 25,000-events hard SWF limit. This causes execution
termination. To prevent this, the workflow can itself close the current execution and start another one by submitting
`self.continue_as_new(*args, **kwargs)`: it is then restarted with a new run ID and an empty history.

See `examples/continue_as_new.py` for a demonstration of this pattern:

```shell
SWF_DOMAIN=TestDomain PYTHONPATH=$PWD simpleflow standalone \
 examples.continue_as_new.CANWorkflow \
 --nb-deciders 1 --nb-workers 1
```

In a real workflow, we would typically use [steps](steps.md) to determine which activities have been executed
and which ones need to run.
