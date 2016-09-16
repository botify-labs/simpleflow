# Extras for simpleflow

## ./demo

This script demonstrates a few features of simpleflow standalone. You should first have a
running decider:

    simpleflow worker.start --domain TestDomain --task-list test --nb-processes 1 examples.basic.BasicWorkflow

... and a running activity worker:

    simpleflow decider.start --domain TestDomain --task-list test --nb-processes 1 examples.basic.BasicWorkflow

... before using the script:

    INTERACTIVE=1 ./extras/demo

If you don't want an interactive usage, set `INTERACTIVE` to `0` or omit it.
