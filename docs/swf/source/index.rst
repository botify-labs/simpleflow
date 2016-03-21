:orphan:

Welcome to Python Simple Workflow
=================================

python-simple-workflow is a wrapper for `Amazon Simple Workflow <http://aws.amazon.com/en/swf/>`_ service.
It aims to provide some abstractions over the webservice concepts through `Boto <https://boto.readthedocs.org/en/latest/ref/swf.html>`_ library Swf api implementation.

It aims to provide:

* **Modelisation**: Swf entities and concepts are to be manipulated through `Models <http://test.com>`_ and `QuerySets <http://test.com>`_ (any ressemblance with the `Django <http://test.com>`_ api would not be a coincidence).
* **High-level Events, History**: A higher level of abstractions over Swf *events* and *history*. Events are implemented as stateful objects aware of their own state and possible transitions. History enhance the events flow description, and can be compiled to check it's integrity and the activities statuses transitions.
* **Decisions**: Stateful abstractions above the Swf decision making system.
* **Actors**: Swf actors base implementation such as a `Decider <http://test.com>`_ or an activity task processor `Worker <http://test.com>`_ from which the user can easily inherit to implement it's own decision/processing model.

It provides querysets and model objects over commonly used concepts: domains, workflow types, activity types, and so on.

It is under MIT license, and any ideas, features requests, patches, pull requests to improve it are of course welcome.

.. include:: contents.rst.inc
