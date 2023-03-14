Command Line
============

Simpleflow comes with a `simpleflow` command-line utility that can be used to list workflows against SWF,
boot decider or activity workers (with multiprocessing), and a few other goodies.


List Workflow Executions
------------------------

    $ simpleflow workflow.list TestDomain
    basic-example-1438722273  basic  OPEN


Workflow Execution Status
-------------------------

    $ simpleflow --header workflow.info TestDomain basic-example-1438722273
    domain      workflow_type.name    workflow_type.version      task_list  workflow_id               run_id                                          tag_list      execution_time  input
    TestDomain  basic                 example                               basic-example-1438722273  22QFVi362TnCh6BdoFgkQFlocunh24zEOemo1L12Yl5Go=                          1.70  {'args': [1], 'kwargs': {}}


Tasks Status
------------

You can check the status of the workflow execution with:

    $ simpleflow --header workflow.tasks DOMAIN WORKFLOW_ID [RUN_ID] --nb-tasks 3
    $ simpleflow --header workflow.tasks TestDomain basic-example-1438722273
    Tasks                     Last State    Last State Time             Scheduled Time
    examples.basic.increment  scheduled     2015-08-04 23:04:34.510000  2015-08-04 23:04:34.510000
    $ simpleflow --header workflow.tasks TestDomain basic-example-1438722273
    Tasks                     Last State    Last State Time             Scheduled Time
    examples.basic.double     completed     2015-08-04 23:06:19.200000  2015-08-04 23:06:17.738000
    examples.basic.delay      completed     2015-08-04 23:08:18.402000  2015-08-04 23:06:17.738000
    examples.basic.increment  completed     2015-08-04 23:06:17.503000  2015-08-04 23:04:34.510000


Profiling
---------

You can profile the execution of the workflow with:

    $ simpleflow --header workflow.profile TestDomain basic-example-1438722273
    Task                                 Last State    Scheduled           Time Scheduled  Start               Time Running  End                 Percentage of total time
    activity-examples.basic.double-1     completed     2015-08-04 23:06              0.07  2015-08-04 23:06            1.39  2015-08-04 23:06                        1.15
    activity-examples.basic.increment-1  completed     2015-08-04 23:04            102.20  2015-08-04 23:06            0.79  2015-08-04 23:06                        0.65


Controlling SWF access
----------------------

The SWF region is controlled by the environment variable `AWS_DEFAULT_REGION`. This variable
comes from the legacy "simple-workflow" project. The option might be exposed through a
`--region` option in the future (if you want that, please open an issue).

The SWF domain is controlled by the `--domain` on most simpleflow commands. It can also
be set via the `SWF_DOMAIN` environment variable. In case both are supplied, the
command-line value takes precedence over the environment variable.

Note that some simpleflow commands expect the domain to be passed as a positional argument.
In that case the environment variable has no effect for now.

The number of retries for accessing SWF can be controlled via `SWF_CONNECTION_RETRIES`
(defaults to 5).

The identity of SWF activity workers and deciders can be controlled via `SIMPLEFLOW_IDENTITY`
which should be a JSON-serialized string representing `{ "key": "value" }` pairs that
adds up (or override) the basic identity provided by simpleflow. If some value is null in
this JSON map, then the key is removed from the final SWF identity.


Controlling log verbosity
-------------------------

You can control log verbosity via the `LOG_LEVEL` environment variable. Default is `INFO`. For instance,
the following command will start a decider with `DEBUG` logs:

    $ LOG_LEVEL=DEBUG simpleflow decider.start --domain TestDomain --task-list test examples.basic.BasicWorkflow
