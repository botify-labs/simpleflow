.. _swf_quickstart:

==============
SWF Quickstart
==============

swf is a wrapper for `Amazon Simple Workflow <http://aws.amazon.com/swf>`_ service. It aims to provide some abstractions over `Boto <http://boto.readthedocs.org>`_ library SWF API implementation, like querysets and objects over commonly used concepts: ``Domains``, ``Workflows``, ``Activities``, and so on.

.. _installation:


Settings
========

Mandatory:

- aws_access_key_id
- aws_secret_access_key

Optional:

- region


Settings are found respectively in:

A credential file
    a ``.swf`` file in the user's home directory:

    .. code-block:: ini

        [credentials]
        aws_access_key_id=<aws_access_key_id>
        aws_secret_access_key=<aws_secret_access_key>

        [defaults]
        region=us-east-1

The following environment variables
    - `AWS_ACCESS_KEY_ID`
    - `AWS_SECRET_ACCESS_KEY`
    - `region`

If neither of the previous methods were used, you can still set the AWS credentials with :meth:`swf.settings.set`:

.. code-block:: shell

        >>> import swf.settings
        >>> swf.settings.set(aws_access_key_id='MYAWSACCESSKEYID',
        ...                  aws_secret_access_key='MYAWSSECRETACCESSKEY',
        ...                  region='REGION')
        # And then you're good to go...
        >>> queryset = DomainQuery()
        >>> queryset.all()
        [Domain('test1'), Domain('test2')]


Batteries Included
==================

.. _models:

Models
------

Simple Workflow entities such as domains, workflow types, workflow executions and activity types are to be manipulated through swf using ``models``. They are immutable ``swf`` objects representations providing an interface to objects attributes, local/remote objects synchronization and changes watch between these local and remote objects.

.. code-block:: python

    # Models resides in swf.models module
    >>> from swf.models import Domain, WorkflowType, WorkflowExecution, ActivityType

    # Once imported you're ready to create a local model instance
    >>> D = Domain(
        "my-test-domain-name",
        description="my-test-domain-description",
        retention_period=60
    )

    # a Domain model local instance has been created, but nothing has been
    # sent to amazon. To do so, you have to save it.
    >>> D.save()

Now you have a local ``Domain`` model object, and if no errors were raised, the ``save`` method have saved amazon-side. But, sometimes, you won't be able to know if the model you're manipulating has an upstream version: whether you've acquired it through a queryset, or the remote object has been deleted for example. Fortunately, models are shipped with a set of functions to make sure your local objects keep synced and consistent.

.. code-block:: python

    # Exists method let's you know if you're model instance has an upstream version
    >>> D.exists
    True

    # What if changes have been made to the remote object?
    # synced  and changes methods help ensuring local and remote models
    #are still synced and which changes have been maid.
    >>> D.is_synced
    True
    >>> D.changes
    ModelDiff()


What if your local object is out of sync? Models ``upstream`` method will fetch the remote version of your object and will build a new model instance using it's attributes.

.. code-block:: python

    >>> D.is_synced
    False
    >>> D.changes
    ModelDiff(
        Difference('status', 'REGISTERED', 'DEPRECATED')
    )

    # Let's pull the upstream version
    >>> D = D.upstream()
    >>> D.is_synced
    True
    >>> D.changes
    ModelDiff()


.. _querysets:

QuerySets
---------

Models can be retrieved and instantiated via querysets. To continue over the django comparison,
they're behaving like django managers.

.. code-block:: python

    # As querying for models needs a valid connection to amazon service,
    # Queryset objects cannot act as classmethods proxy and have to be instantiated;
    # most of the time against a Domain model instance
    >>> from swf.querysets import DomainQuerySet, WorkflowTypeQuerySet

    # Domain querysets can be instantiated directly
    >>> domain_qs = DomainQuerySet()
    >>> workflow_domain = domain_qs.get("MyTestDomain")  # and specific model retieved via .get method
    >>> workflow_qs = WorkflowTypeQuerySet(workflow_domain)  # queryset built against model instance example

    >>> workflow_qs.all()
    [WorkflowType("TestType1"), WorkflowType("TestType2"),]

    >>> workflow_qs.filter(status=DEPRECATED)
    [WorkflowType("DeprecatedType1"),]

.. _events:

Events
------

(coming soon)

.. _history:

History
-------

(coming soon)

.. _decisions:

Decisions
---------

(coming soon)

.. _actors:

Actors
------

Swf workflows are based on a worker-decider pattern. Every actions in the flow is executed by a worker which runs supplied activity tasks. And every actions is the result of a decision taken by the decider reading the workflow events history and deciding what to do next. In order to ease the development of such workers and decider, swf exposes base classes for them located in ``swf.actors`` submodule.

* An ``Actor`` must basically implement a ``start`` and ``stop`` method and can actually inherits from whatever runtime implementation you need: thread, gevent, multiprocess...

.. code-block:: python

    class Actor(ConnectedSWFObject):
        def __init__(self, domain, task_list)
        def start(self):
        def stop(self):

* ``Decider`` base class implements the core functionality of a swf decider: polling for decisions tasks, and sending back a decision task copleted decision. Every other special needs implementations are left up to the user.

.. code-block:: python

    class Decider(Actor):
        def __init__(self, domain, task_list)
        def complete(self, task_token, decisions=None, execution_context=None)
        def poll(self, task_list=None, identity=None, maximum_page_size=None)

* ``Worker`` base class implements the core functionality of a swf worker whoes role is to process activity tasks. It is basically able to poll for new activity tasks to process, send back a heartbeat to swf service in order to let it know it hasn't failed or crashed, and to complete, fail or cancel the activity task it's processing.

.. code-block:: python

    class ActivityWorker(Actor):
        def __init__(self, domain, task_list)
        def cancel(self, task_token, details=None)
        def complete(self, task_token, result=None)
        def fail(self, task_token, details=None, reason=None)
        def heartbeat(self, task_token, details=None)
        def poll(self, task_list=None, **kwargs)
