.. _api:

===
API
===

Settings
========

.. automodule:: swf.settings
    :members:
    :inherited-members:

Models
======

Domain
------

.. automodule:: swf.models.domain
    :members:
    :inherited-members:

Workflow Type
-------------

.. autoclass:: swf.models.workflow.WorkflowType
    :members:
    :inherited-members:

Workflow Execution
------------------

.. autoclass:: swf.models.workflow.WorkflowExecution
    :members:
    :inherited-members:

Event
-----

.. autoclass:: swf.models.event.Event
    :members:
    :inherited-members:

Execution History
-----------------

.. autoclass:: swf.models.event.History
    :members:
    :inherited-members:

Activity Type
-------------

.. autoclass:: swf.models.activity.ActivityType
    :members:
    :inherited-members:

Activity Task
-------------

.. autoclass:: swf.models.activity.ActivityTask
    :members:
    :inherited-members:



Querysets
=========


DomainQuerySet
--------------

.. autoclass:: swf.querysets.domain.DomainQuerySet
  :members:

WorkflowTypeQuerySet
--------------------

.. autoclass:: swf.querysets.workflow.WorkflowTypeQuerySet
  :members:

WorkflowExecutionQuerySet
-------------------------

.. autoclass:: swf.querysets.workflow.WorkflowExecutionQuerySet
  :members:

ActivityTypeQuerySet
--------------------

.. autoclass:: swf.querysets.activity.ActivityTypeQuerySet
    :members:


Actors
======


Actor
-----

.. autoclass:: swf.actors.core.Actor
    :members:

ActivityWorker
--------------

.. autoclass:: swf.actors.worker.ActivityWorker
    :members:
    :inherited-members:
