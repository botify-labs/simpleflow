# Continue As New

In long-running workflow executions, the history can grow to more than 25,000 events. This causes execution termination.
To prevent this, the workflow can itself close the current execution and start another one by submitting
`self.continue_as_new(*args, **kwargs)`.

See `examples/continue_as_new.py` for a demonstration of this pattern:

```shell
SWF_DOMAIN=TestDomain PYTHONPATH=$PWD simpleflow standalone \
 examples.continue_as_new.CANWorkflow \
 --nb-deciders 1 --nb-workers 1
```
