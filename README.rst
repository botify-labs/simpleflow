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

Quickstart
----------

Let's take a simple example that computes the result of ``(x + 1) * 2``. You
will find this example in ``examples/basic.py``.

We need to declare the functions as activities to make them available:

.. code::

    from simpleflow import activity

    @activity.with_attributes(task_list='quickstart')
    def increment(x):
        return x + 1

    @activity.with_attributes(task_list='quickstart')
    def double(x):
        return x * 2


And then define the workflow itself in a ``example.py`` file:

.. code::

    class BasicWorkflow(Workflow):
        def run(self, x):
            y = self.submit(increment, x)
            z = self.submit(double, y)

            print '({x} + 1) * 2 = {result}'.format(
                x=x,
                result=z.result)
            return z.result

Now check that the workflow works locally: ::

    $ simpleflow start --local examples.basic.BasicWorkflow <<< '{"args": [1]}'
    (1 + 1) * 2 = 4

The *input* can contain ``args`` or ``kwargs`` such as in:

.. code::

    {"args": [1],
     "kwargs": {}
    }

which is equivalent to:

.. code::

    {"kwargs": {"x": 1}}

You can, of course, pass values in both ``args`` and ``kwargs``.

Now that you are confident that the workflow should work, you can run it on
Amazon SWF by omitting the ``--local`` flag: ::

   $ simpleflow --domain test start examples.basic.BasicWorkflow <<< '{"args": [1]}'

.. note:: It requires at least a *decider* and a *worker* processes which are
   not currently provided by the simpleflow package.

You can check the status of the workflow execution with: ::

    $ simpleflow status DOMAIN WORKFLOW_ID [RUN_ID] --nb-tasks 3
    Workflow Execution WORKFLOW_ID
    Domain: DOMAIN
    Workflow Type: WORKFLOW_TYPE

    Total time = 100.599 seconds

    ## Tasks Status

    | Tasks | Last State   | at                         | Scheduled at               |
    |:------|:-------------|:---------------------------|:---------------------------|
    | Task3 | completed    | 2015-06-24 12:06:03.397000 |                            |
    | Task2 | completed    | 2015-06-24 12:05:24.103000 | 2015-06-24 12:05:23.459000 |
    | Task1 | completed    | 2015-06-24 12:05:23.337000 | 2015-06-24 12:05:22.312000 |

You can profile the execution of the workflow with: ::

    $ simpleflow profile DOMAIN WORKFLOW_ID [RUN_ID] --nb-tasks 3

    Workflow Execution WORKFLOW_ID
    Domain: DOMAIN
    Workflow Type: WORKFLOW_TYPE

    Total time = 100.599 seconds

    ## Start to close timings

    | Task  | Last State   | Scheduled        |    ->  | Start            |    ->  | End              |        % |
    |:------|:-------------|:-----------------|-------:|:-----------------|-------:|:-----------------|---------:|
    | task2 | completed    | 2015-06-23 22:27 |  0.09  | 2015-06-23 22:27 | 43.776 | 2015-06-23 22:28 | 43.5153  |
    | task1 | completed    | 2015-06-23 22:27 |  0.118 | 2015-06-23 22:27 | 28.246 | 2015-06-23 22:27 | 28.0778  |
    | task3 | completed    | 2015-06-23 22:26 |  0.068 | 2015-06-23 22:26 | 11.159 | 2015-06-23 22:26 | 11.0926  |

Documentation
-------------

Full documentation is available at https://simpleflow.readthedocs.org/.

Requirements
------------

- Python >= 2.6 or >= 3.3

License
-------

MIT licensed. See the bundled `LICENSE <https://github.com/botify-labs/simpleflow/blob/master/LICENSE>`_ file for more details.
