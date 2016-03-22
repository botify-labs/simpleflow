# -*- coding:utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

import time
import json
import collections

from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError

from swf.constants import REGISTERED
from swf.utils import immutable
from swf.models import BaseModel, Domain
from swf.models.history import History
from swf.models.base import ModelDiff
from swf import exceptions
from swf.exceptions import (
    DoesNotExistError,
    AlreadyExistsError,
    ResponseError,
    raises,
)


_POLICIES = (
    'TERMINATE',       # child executions will be terminated
    'REQUEST_CANCEL',  # a request to cancel will be attempted for
                       # each child execution
    'ABANDON',         # no action will be taken
)

CHILD_POLICIES = collections.namedtuple('CHILD_POLICY',
                                        ' '.join(_POLICIES))(*_POLICIES)


class WorkflowTypeDoesNotExist(DoesNotExistError):
    pass


class WorkflowExecutionDoesNotExist(DoesNotExistError):
    pass


@immutable
class WorkflowType(BaseModel):
    """Simple Workflow Type wrapper

    :param  domain: Domain the workflow type should be registered in
    :type   domain: swf.models.Domain

    :param  name: name of the workflow type
    :type   name: String

    :param  version: workflow type version
    :type   version: String

    :param  status: workflow type status
    :type   status: swf.core.ConnectedSWFObject.{REGISTERED, DEPRECATED}

    :param   creation_date: creation date of the current WorkflowType
    :type    creation_date: float (timestamp)

    :param   deprecation_date: deprecation date of WorkflowType
    :type    deprecation_date: float (timestamp)

    :param  task_list: task list to use for scheduling decision tasks for executions
                       of this workflow type
    :type   task_list: String

    :param  child_policy: policy to use for the child workflow executions
                          when a workflow execution of this type is terminated
    :type   child_policy: CHILD_POLICIES.{TERMINATE |
                                          REQUEST_CANCEL |
                                          ABANDON}

    :param  execution_timeout: maximum duration for executions of this workflow type
    :type   execution_timeout: String

    :param  decision_tasks_timeout: maximum duration of decision tasks for this workflow type
    :type   decision_tasks_timeout: String

    :param  description: Textual description of the workflow type
    :type   description: String
    """
    __slots__ = [
        'domain',
        'name',
        'version',
        'status',
        'creation_date',
        'deprecation_date',
        'task_list',
        'child_policy',
        'execution_timeout',
        'decision_tasks_timeout',
        'description',
    ]

    def __init__(self, domain, name, version,
                 status=REGISTERED,
                 creation_date=0.0,
                 deprecation_date=0.0,
                 task_list=None,
                 child_policy=CHILD_POLICIES.TERMINATE,
                 execution_timeout='300',
                 decision_tasks_timeout='300',
                 description=None, *args, **kwargs):
        self.domain = domain
        self.name = name
        self.version = version
        self.status = status
        self.creation_date = creation_date
        self.deprecation_date = deprecation_date

        self.task_list = task_list
        self.execution_timeout = execution_timeout
        self.decision_tasks_timeout = decision_tasks_timeout
        self.description = description

        # Explicitly call child_policy setter
        # to validate input value
        self.set_child_policy(child_policy)

        # immutable decorator rebinds class name,
        # so have to use generice self.__class__
        super(self.__class__, self).__init__(*args, **kwargs)

    def set_child_policy(self, policy):
        if not policy in CHILD_POLICIES:
            raise ValueError("invalid child policy value: {}".format(policy))

        self.child_policy = policy

    def _diff(self):
        """Checks for differences between WorkflowType instance
        and upstream version

        :returns: A list of swf.models.base.ModelDiff namedtuple describing
                  differences
        :rtype: list
        """
        try:
            description = self.connection.describe_workflow_type(
                self.domain.name,
                self.name,
                self.version
            )
        except SWFResponseError as e:
            if e.error_code == 'UnknownResourceFault':
                raise DoesNotExistError("Remote Domain does not exist")

            raise ResponseError(e.body['message'])

        workflow_info = description['typeInfo']
        workflow_config = description['configuration']

        return ModelDiff(
            ('name', self.name, workflow_info['workflowType']['name']),
            ('version', self.version, workflow_info['workflowType']['version']),
            ('status', self.status, workflow_info['status']),
            ('creation_date', self.creation_date, workflow_info['creationDate']),
            ('deprecation_date', self.deprecation_date, workflow_info['deprecationDate']),
            ('task_list', self.task_list, workflow_config['defaultTaskList']['name']),
            ('child_policy', self.child_policy, workflow_config['defaultChildPolicy']),
            ('execution_timeout', self.execution_timeout, workflow_config['defaultExecutionStartToCloseTimeout']),
            ('decision_tasks_timout', self.decision_tasks_timeout, workflow_config['defaultTaskStartToCloseTimeout']),
            ('description', self.description, workflow_info['description']),
        )

    @property
    @exceptions.translate(SWFResponseError, to=ResponseError)
    @exceptions.is_not(WorkflowTypeDoesNotExist)
    @exceptions.when(SWFResponseError,
                     raises(WorkflowTypeDoesNotExist,
                            when=exceptions.is_unknown('WorkflowType'),
                            extract=exceptions.extract_resource))
    def exists(self):
        """Checks if the WorkflowType exists amazon-side

        :rtype: bool
        """
        self.connection.describe_workflow_type(
            self.domain.name,
            self.name,
            self.version
        )
        return True

    def save(self):
        """Creates the workflow type amazon side"""
        try:
            self.connection.register_workflow_type(
                self.domain.name,
                self.name,
                self.version,
                task_list=str(self.task_list),
                default_child_policy=str(self.child_policy),
                default_execution_start_to_close_timeout=str(self.execution_timeout),
                default_task_start_to_close_timeout=str(self.decision_tasks_timeout),
                description=self.description
            )
        except SWFTypeAlreadyExistsError:
            raise AlreadyExistsError("Workflow type %s already exists amazon-side" % self.name)
        except SWFResponseError as e:
            if e.error_code == 'UnknownResourceFault':
                raise DoesNotExistError(e.body['message'])

    def delete(self):
        """Deprecates the workflow type amazon-side"""
        try:
            self.connection.deprecate_workflow_type(self.domain.name, self.name, self.version)
        except SWFResponseError as e:
            if e.error_code in ['UnknownResourceFault', 'TypeDeprecatedFault']:
                raise DoesNotExistError(e.body['message'])

    def upstream(self):
        from swf.querysets.workflow import WorkflowTypeQuerySet
        qs = WorkflowTypeQuerySet(self.domain)
        return qs.get(self.name, self.version)

    def start_execution(self, workflow_id=None, task_list=None,
                        child_policy=None, execution_timeout=None,
                        input=None, tag_list=None, decision_tasks_timeout=None):
        """Starts a Workflow execution of current workflow type

        :param  workflow_id: The user defined identifier associated with the workflow execution
        :type   workflow_id: String

        :param  task_list: task list to use for scheduling decision tasks for execution
                        of this workflow
        :type   task_list: String

        :param  child_policy: policy to use for the child workflow executions
                              of this workflow execution.
        :type   child_policy: CHILD_POLICIES.{TERMINATE |
                                              REQUEST_CANCEL |
                                              ABANDON}

        :param  execution_timeout: maximum duration for the workflow execution
        :type   execution_timeout: String

        :param  input: Input of the workflow execution
        :type   input: dict

        :param  tag_list: Tags associated with the workflow execution
        :type   tag_list: String

        :param  decision_tasks_timeout: maximum duration of decision tasks
                                        for this workflow execution
        :type   decision_tasks_timeout: String
        """
        workflow_id = workflow_id or '%s-%s-%i' % (self.name, self.version, time.time())
        task_list = task_list or self.task_list
        child_policy = child_policy or self.child_policy
        input = json.dumps(input) or None

        run_id = self.connection.start_workflow_execution(
            self.domain.name,
            workflow_id,
            self.name,
            self.version,
            task_list=task_list,
            child_policy=child_policy,
            execution_start_to_close_timeout=execution_timeout,
            input=input,
            tag_list=tag_list,
            task_start_to_close_timeout=decision_tasks_timeout,
        )['runId']

        return WorkflowExecution(self.domain, workflow_id, run_id=run_id)

    def __repr__(self):
        return '<{} domain={} name={} version={} status={}>'.format(
               self.__class__.__name__,
               self.domain.name,
               self.name,
               self.version,
               self.status)


@immutable
class WorkflowExecution(BaseModel):
    """Simple Workflow execution wrapper

    :param  domain: Domain the workflow execution should be registered in
    :type   domain: swf.models.domain.Domain

    :param  workflow_type: The WorkflowType associated with the workflow execution
                           is associated with
    :type   workflow_type: String

    :param  workflow_id: The user defined identifier associated with the workflow execution
    :type   workflow_id: String

    :param  run_id: The Amazon defined identifier associated with the workflow execution
    :type   run_id: String

    :param  status: Whether the WorkflowExecution instance represents an opened or
                    closed execution
    :type   status: String constant

    :param  task_list: The task list to use for the decision tasks generated
                       for this workflow execution.
    :type   task_list: string

    :param  input: input data of the execution, which will be passed around using
                   serialized json
    :type   input: dict
    """
    STATUS_OPEN = "OPEN"
    STATUS_CLOSED = "CLOSED"

    CLOSE_STATUS_COMPLETED = "COMPLETED"
    CLOSE_STATUS_FAILED = "FAILED"
    CLOSE_STATUS_CANCELED = "CANCELED"
    CLOSE_STATUS_TERMINATED = "TERMINATED"
    CLOSE_STATUS_CONTINUED_AS_NEW = "CLOSE_STATUS_CONTINUED_AS_NEW"
    CLOSE_TIMED_OUT = "TIMED_OUT"

    kind = 'execution'

    __slots__ = [
        'domain',
        'workflow_id',
        'run_id',
        'status',
        'workflow_type',
        'task_list',
        'child_policy',
        'close_status',
        'execution_timeout',
        'input',
        'tag_list',
        'decision_tasks_timeout',
        'close_timestamp',
        'start_timestamp',
        'cancel_requested',
        'latest_execution_context',
        'latest_activity_task_timestamp',
        'open_counts',
        'parent',
    ]

    def __init__(self, domain, workflow_id, run_id=None,
                 status=STATUS_OPEN, workflow_type=None,
                 task_list=None, child_policy=None,
                 close_status=None, execution_timeout=None,
                 input=None, tag_list=None,
                 decision_tasks_timeout=None,
                 close_timestamp=None,
                 start_timestamp=None,
                 cancel_requested=None,
                 latest_execution_context=None,
                 latest_activity_task_timestamp=None,
                 open_counts=None,
                 parent=None,
                 *args, **kwargs):
        Domain.check(domain)
        self.domain = domain
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.status = status
        self.workflow_type = workflow_type
        self.task_list = task_list
        self.child_policy = child_policy
        self.close_status = close_status
        self.execution_timeout = execution_timeout
        self.input = input
        self.tag_list = tag_list or []
        self.decision_tasks_timeout = decision_tasks_timeout
        self.close_timestamp = close_timestamp
        self.start_timestamp = start_timestamp
        self.cancel_requested = cancel_requested
        self.latest_execution_context = latest_execution_context
        self.latest_activity_task_timestamp = latest_activity_task_timestamp
        self.open_counts = open_counts or {} # so we can query keys in any case
        self.parent = parent or {} # so we can query keys in any case

        # immutable decorator rebinds class name,
        # so have to use generice self.__class__
        super(self.__class__, self).__init__(*args, **kwargs)

    def _diff(self):
        """Checks for differences between WorkflowExecution instance
        and upstream version

        :returns: A list of swf.models.base.Diff namedtuple describing
                  differences
        :rtype: list
        """
        try:
            description = self.connection.describe_workflow_execution(
                self.domain.name,
                self.run_id,
                self.workflow_id
            )
        except SWFResponseError as e:
            if e.error_code == 'UnknownResourceFault':
                raise DoesNotExistError("Remote Domain does not exist")

            raise ResponseError(e.body['message'])

        execution_info = description['executionInfo']
        execution_config = description['executionConfiguration']

        return ModelDiff(
            ('workflow_id', self.workflow_id, execution_info['execution']['workflowId']),
            ('run_id', self.run_id, execution_info['execution']['runId']),
            ('status', self.status, execution_info['executionStatus']),
            ('task_list', self.task_list, execution_config['taskList']['name']),
            ('child_policy', self.child_policy, execution_config['childPolicy']),
            ('execution_timeout', self.execution_timeout, execution_config['executionStartToCloseTimeout']),
            ('tag_list', self.tag_list, execution_info.get('tagList')),
            ('decision_tasks_timeout', self.decision_tasks_timeout, execution_config['taskStartToCloseTimeout']),
        )

    @property
    @exceptions.translate(SWFResponseError, to=ResponseError)
    @exceptions.is_not(WorkflowExecutionDoesNotExist)
    @exceptions.when(SWFResponseError,
                     raises(WorkflowExecutionDoesNotExist,
                            when=exceptions.is_unknown('WorkflowExecution'),
                            extract=exceptions.extract_resource))
    def exists(self):
        """Checks if the WorkflowExecution exists amazon-side

        :rtype: bool
        """
        self.connection.describe_workflow_execution(
            self.domain.name,
            self.run_id,
            self.workflow_id
        )
        return True

    def upstream(self):
        from swf.querysets.workflow import WorkflowExecutionQuerySet
        qs = WorkflowExecutionQuerySet(self.domain)
        return qs.get(self.workflow_id, self.run_id)

    def history(self, *args, **kwargs):
        """Returns workflow execution history report

        :returns: The workflow execution complete events history
        :rtype: swf.models.event.History
        """
        domain = kwargs.pop('domain', self.domain)
        if not isinstance(domain, basestring):
            domain = domain.name

        response = self.connection.get_workflow_execution_history(
            self.domain.name,
            self.run_id,
            self.workflow_id,
            **kwargs
        )

        events = response['events']
        next_page = response.get('nextPageToken')
        while next_page is not None:
            response = self.connection.get_workflow_execution_history(
                self.domain.name,
                self.run_id,
                self.workflow_id,
                next_page_token=next_page,
                **kwargs
            )

            events.extend(response['events'])
            next_page = response.get('nextPageToken')

        return History.from_event_list(events)

    @exceptions.translate(SWFResponseError,
                          to=ResponseError)
    @exceptions.when(SWFResponseError,
                     raises(WorkflowExecutionDoesNotExist,
                            when=exceptions.is_unknown('WorkflowExecution'),
                            extract=exceptions.extract_resource))
    def signal(self, signal_name, input=None, *args, **kwargs):
        """Records a signal event in the workflow execution history and
        creates a decision task.

        The signal event is recorded with the specified user defined
        ``signal_name`` and ``input`` (if provided).

        :param  signal_name: The name of the signal. This name must be
                             meaningful to the target workflow.
        :type   signal_name: str

        :param  input: Data to attach to the WorkflowExecutionSignaled
                       event in the target workflow execution’s history.
        :type   input: dict
        """
        if input is None:
            input = {}
        self.connection.signal_workflow_execution(
            self.domain.name,
            signal_name,
            self.workflow_id,
            input=json.dumps(input),
            run_id=self.run_id)

    @exceptions.translate(SWFResponseError,
                          to=ResponseError)
    @exceptions.when(SWFResponseError,
                     raises(WorkflowExecutionDoesNotExist,
                            when=exceptions.is_unknown('domain'),
                            extract=exceptions.extract_resource))
    def request_cancel(self, *args, **kwargs):
        """Requests the workflow execution cancel"""
        self.connection.request_cancel_workflow_execution(
            self.domain.name,
            self.workflow_id,
            run_id=self.run_id)

    @exceptions.translate(SWFResponseError,
                          to=ResponseError)
    @exceptions.when(SWFResponseError,
                     raises(WorkflowExecutionDoesNotExist,
                            when=exceptions.is_unknown('domain'),
                            extract=exceptions.extract_resource))
    def terminate(self, *args, **kwargs):
        """Terminates the workflow execution"""
        self.connection.terminate_workflow_execution(
            self.domain.name,
            self.workflow_id,
            run_id=self.run_id
        )
