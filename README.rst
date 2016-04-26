===============================
simpleflow
===============================

.. image:: https://badge.fury.io/py/simpleflow.png
    :target: http://badge.fury.io/py/simpleflow

.. image:: https://travis-ci.org/botify-labs/simpleflow.png?branch=master
        :target: https://travis-ci.org/botify-labs/simpleflow

.. image:: https://pypip.in/d/simpleflow/badge.png
        :target: https://crate.io/packages/simpleflow?version=latest


Simple Flow is a Python library that provides abstractions to write programs in
the `distributed dataflow paradigm
<https://en.wikipedia.org/wiki/Distributed_data_flow>`_. It relies on futures
to describe the dependencies between tasks. It coordinates the execution of
distributed tasks with Amazon `SWF <https://aws.amazon.com/swf/>`_.

A ``Future`` object models the asynchronous execution of a computation that may
end.

It tries to mimics the interface of the Python `concurrent.futures
<http://docs.python.org/3/library/concurrent.futures>`_ library.

Features
--------

- Provides a ``Future`` abstraction to define dependencies between tasks.
- Define asynchronous tasks from callables.
- Handle workflows with Amazon SWF.
- Implement replay behavior like the Amazon Flow framework.
- Handle retry of tasks that failed.
- Automatically register decorated tasks.
- Handle the completion of a decision with more than 100 tasks.
- Provides a local executor to check a workflow without Amazon SWF (see
  ``simpleflow --local`` command).
- Provides decider and activity worker process for execution with Amazon SWF.
- Ships with the `simpleflow` command. `simpleflow --help` for more information
  about the commands it supports.

Quickstart
----------

Let's take a simple example that computes the result of ``(x + 1) * 2``. You
will find this example in ``examples/basic.py``.

We need to declare the functions as activities to make them available:

.. code:: python

    from simpleflow import (
        activity,
        Workflow,
        futures,
    )

    @activity.with_attributes(task_list='quickstart', version='example')
    def increment(x):
        return x + 1

    @activity.with_attributes(task_list='quickstart', version='example')
    def double(x):
        return x * 2

    @activity.with_attributes(task_list='quickstart', version='example')
    def delay(t, x):
        time.sleep(t)
        return x

And then define the workflow itself in a ``example.py`` file:

.. code:: python

    class BasicWorkflow(Workflow):
        name = 'basic'
        version = 'example'
        task_list = 'example'

        def run(self, x, t=30):
            y = self.submit(increment, x)
            yy = self.submit(delay, t, y)
            z = self.submit(double, y)

            print '({x} + 1) * 2 = {result}'.format(
                x=x,
                result=z.result)
            futures.wait(yy, z)
            return z.result

Now check that the workflow works locally with an integer "x" and a wait value "t": ::

    $ simpleflow workflow.start --local examples.basic.BasicWorkflow --input '[1, 5]'
    (1 + 1) * 2 = 4

*input* is encoded in JSON format and can contain the list of *positional*
arguments such as ``'[1, 1]`` or a *dict* with the ``args`` and ``kwargs`` keys
such as ``{"args": [1], "kwargs": {}}``, ``{"kwargs": {"x": 1}}``, or
``'{"args": [1], "kwargs": {"t": 5}}'```.

Now that you are confident that the workflow should work, you can run it on
Amazon SWF with the ``standalone`` command : ::

   $ simpleflow standalone --domain TestDomain examples.basic.BasicWorkflow --input '[1, 5]'

The *standalone* command sets an unique task list and manage all the processes
that are needed to execute the workflow: decider, activity worker, and a client
that starts the workflow. It is very convenient for testing a workflow by
executing it with SWF during the development steps or integration tests.

Let's take a closer look to the workflow definition.

It is a *class* that inherits from :class:`simpleflow.Workflow`:

    .. code:: python

    class BasicWorkflow(Workflow):

It defines 3 class attributes:

- *name*, the name of the SWF workflow type.
- *version*, the version of the SWF workflow type. It is currently provided
  only for labeling a workflow.
- *task_list*, the default task list (see it as a dynamically created queue)
  where decision tasks for this workflow will be sent. Any *decider* that
  listens on this task list can handle this workflow. This value can be
  overrided by the simpleflow commands and objects.

It also implements the :meth:`run` method that takes two arguments: ``x`` and
``t=30`` (i.e. ``t`` is optional and has the default value ``30``). These
arguments are passed with the ``--input`` option. The :meth:`run` method
describes the workflow and how its tasks should execute.

Each time a decider takes a decision task, it executes again the :meth:`run`
from the start. When the workflow execution starts, it evaluates ``y =
self.submit(increment, x)`` for the first time. *y* holds a future in state
``PENDING``. The execution continues with the line ``yy = self.submit(delay, t,
y)``. *yy* holds another future in state ``PENDING``. This state means the task
has not been scheduled. Now execution still continue in the :meth:`run` method
with the line ``z = self.submit(double, y)``. Here it needs the value of the
*y* future to evaluate the :func:`double` activity. As the execution cannot
continues, the decider schedules the task :func:`increment`. *yy* is not a
dependency for any task so it is not scheduled.

Once the decider has scheduled the task for *y*, it sleeps and waits for an
event to be waken up. This happens when the :func:`increment` task completes.
SWF schedules a decision task. A decider takes it and executes the
:meth:`BasicWorkflow.run` method again from the start. It evalues the line ``y
= self.submit(increment, x)``. The task associated with the *y* future has
completed. Hence *y* is in state ``FINISHED`` and contains the value ``2`` in
``y.result``. The execution continues until it blocks. It goes by ``yy =
self.submit(delay, t, y)`` that stays the same. Then it reaches ``z =
self.submit(double, y)``. It gets the value of ``y.result`` and *z* now holds a
future in state ``PENDING``. Execution reaches the line with the ``print``. It
blocks here because ``z.result`` is not available. The decider schedules the
task backs by the *z* future: ``double(y)``. The workflow execution continues
so forth by evaluating the :meth:`BasicWorkflow.run` again from the start until
it finishes.

Commands
--------

Overview
~~~~~~~~

Please read and even run the `demo` script to have a quick glance of
`simpleflow` commands. To run the `demo` you will need to start decider and
activity worker processes.

Start a decider with: ::

    $ simpleflow decider.start --domain TestDomain --task-list test examples.basic.BasicWorkflow

Start an activity worker with: ::

    $ simpleflow worker.start --domain TestDomain --task-list quickstart examples.basic.BasicWorkflow

Then execute ``./demo``.

List Workflow Executions
~~~~~~~~~~~~~~~~~~~~~~~~

    $ simpleflow workflow.list TestDomain
    basic-example-1438722273  basic  OPEN

Workflow Execution Status
~~~~~~~~~~~~~~~~~~~~~~~~~

    $ simpleflow --header workflow.info TestDomain basic-example-1438722273
    domain      workflow_type.name    workflow_type.version      task_list  workflow_id               run_id                                          tag_list      execution_time  input
    TestDomain  basic                 example                               basic-example-1438722273  22QFVi362TnCh6BdoFgkQFlocunh24zEOemo1L12Yl5Go=                          1.70  {u'args': [1], u'kwargs': {}}

Tasks Status
~~~~~~~~~~~~

You can check the status of the workflow execution with: ::

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
~~~~~~~~~~~~

You can profile the execution of the workflow with: ::

    $ simpleflow --header workflow.profile TestDomain basic-example-1438722273
    Task                                 Last State    Scheduled           Time Scheduled  Start               Time Running  End                 Percentage of total time
    activity-examples.basic.double-1     completed     2015-08-04 23:06              0.07  2015-08-04 23:06            1.39  2015-08-04 23:06                        1.15
    activity-examples.basic.increment-1  completed     2015-08-04 23:04            102.20  2015-08-04 23:06            0.79  2015-08-04 23:06                        0.65


Controlling log verbosity
~~~~~~~~~~~~~~~~~~~~~~~~~

You can control log verbosity via the `LOG_LEVEL` environment variable. Default is `INFO`. For instance,
the following command will start a decider with `DEBUG` logs:

    $ LOG_LEVEL=DEBUG simpleflow decider.start --domain TestDomain --task-list test examples.basic.BasicWorkflow


Documentation
-------------

Full documentation (work-in-progress) is available at
https://simpleflow.readthedocs.org/.

Requirements
------------

- Python 2.6.x or 2.7.x
- Python 3.x compatibility is NOT guaranteed for now: https://github.com/botify-labs/simpleflow/issues/87


Release
-------

In order to release a new version, you'll need credentials on pypi.python.org for this
software, as long as write access to this repository. Ask via an issue if needed.
Rough process:

    git checkout master
    git pull --rebase
    v=0.10.0
    vi simpleflow/__init__.py
    git add . && git commit -m "Bump version to $v"
    git tag $v
    git push --tags
    python setup.py sdist upload -r pypi


License
-------

MIT licensed. See the bundled `LICENSE <https://github.com/botify-labs/simpleflow/blob/master/LICENSE>`_ file for more details.
