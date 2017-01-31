Signals
=======

[Prerelease: this is subject to change]

Signals are handled via two methods: ``Workflow.signal`` and ``Workflow.wait_signal``.
They are currently only implemented with SWF.


Signaling a workflow
--------------------

The ``Workflow.signal`` method sends a signal to one or several workflows.

.. code::

        def run(self):
            # Send to self, parent and children
            future = self.submit(self.signal('signal_name', *args, **kwargs))

            # Send to specific workflow
            future = self.submit(self.signal('signal_name', workflow_id, run_id, *args, **kwargs))


The future will be finished, its result being \*args and \*\*kwargs, as soon as at least one workflow has been signaled
(including oneself).


Waiting for a signal
--------------------

The ``Workflow.wait_signal`` returns a ``Future`` which result is the signal input.

.. code::

        def run(self):
            future = self.submit(self.wait_signal('signal_name'))
            result = future.result

Naturally, one isn't forced to wait on the future result:

.. code::

        def run(self):
            my_signal = self.submit(self.wait_signal('signal_name'))
            if my_signal.finished:
                # Something happened
                self.process(my_signal.result)


Limitations
-----------

* signals cannot be reset; they can be overwritten though (only the latest one count)
* derive from futures.Future to add the timestamp or counter and better names? This would bypass the "reset" issue too


Implementation
--------------

The ``swf.executor.signal`` method returns a ``swf.SignalTask`` instance. Its ``schedule`` method
returns an ``ExternalWorkflowExecutionDecision`` containing the given signal, sent either to the running workflow or
the specified one.

This decision results in a ``SignalExternalWorkflowExecutionInitiated`` followed (if all's well) by a
``SignalExternalWorkflowExecutionInitiated`` in the sender's history; from these events, we create first a running,
then a completed future. (It can also fail, for instance if the workflow doesn't exist.)

The receiver gets a ``WorkflowExecutionSignaled`` with the signal name, input and external (i.e. sender) information.
In this implementation, we want every known workflow to be signaled too, so the signal is propagated by default to the
parent and children of the workflow. This can be disabled by passing ``propagate=False`` to signal.

One limit here is that we propagate using ``SignalWorkflowExecution``, not a decision; this means the target doesn't
have the ``externalWorkflowExecution`` information. We try and fix this by passing ``__workflow_id`` and ``__run_id``
in the input.
