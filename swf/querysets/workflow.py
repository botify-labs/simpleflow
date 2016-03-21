# -*- coding: utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from boto.swf.exceptions import SWFResponseError

from swf.constants import REGISTERED
from swf.querysets.base import BaseQuerySet
from swf.models import Domain
from swf.models.workflow import (WorkflowType, WorkflowExecution,
                                 CHILD_POLICIES)
from swf.utils import datetime_timestamp, past_day, get_subkey
from swf.exceptions import (ResponseError, DoesNotExistError,
                            InvalidKeywordArgumentError, AlreadyExistsError)


class BaseWorkflowQuerySet(BaseQuerySet):
    """Base domain bounded workflow queryset objects

    Amazon workflows types and executions are always bounded
    to a specific domain: so any queryset which means to deal
    with workflows has to be built against a `domain`

    :param      domain: domain the inheriting queryset belongs to
    :type       domain: swf.model.domain.Domain
    """
    # Amazon response section corresponding
    # to current queryset informations
    _infos = 'typeInfo'
    _infos_plural = 'typeInfos'

    def __init__(self, domain, *args, **kwargs):
        super(BaseWorkflowQuerySet, self).__init__(*args, **kwargs)
        Domain.check(domain)
        self.domain = domain

    @property
    def domain(self):
        if not hasattr(self, '_domain'):
            self._domain = None
        return self._domain

    @domain.setter
    def domain(self, value):
        # Avoiding circular import
        from swf.models.domain import Domain

        if not isinstance(value, Domain):
            err = "domain property has to be of"\
                  "swf.model.domain.Domain type, not %r"\
                  % type(value)
            raise TypeError(err)
        self._domain = value

    def _list(self, *args, **kwargs):
        raise NotImplementedError

    def _list_items(self, *args, **kwargs):
        response = {'nextPageToken': None}
        while 'nextPageToken' in response:
            response = self._list(
                *args,
                next_page_token=response['nextPageToken'],
                **kwargs
            )

            for item in response[self._infos_plural]:
                yield item


class WorkflowTypeQuerySet(BaseWorkflowQuerySet):

    # Explicit is better than implicit, keep zen
    _infos = 'typeInfo'
    _infos_plural = 'typeInfos'

    def to_WorkflowType(self, domain, workflow_info, **kwargs):
        # Not using get_subkey in order for it to explictly
        # raise when workflowType name doesn't exist for example
        return WorkflowType(
            domain,
            workflow_info['workflowType']['name'],
            workflow_info['workflowType']['version'],
            status=workflow_info['status'],
            **kwargs
        )

    def get(self, name, version, *args, **kwargs):
        """Fetches the Workflow Type with `name` and `version`

        :param  name: name of the workflow type
        :type   name: String

        :param  version: workflow type version
        :type   version: String

        :returns: matched workflow type instance
        :rtype: swf.core.model.workflow.WorkflowType

        A typical Amazon response looks like:

        .. code-block:: json

            {
                "configuration": {
                    "defaultExecutionStartToCloseTimeout": "300",
                    "defaultTaskStartToCloseTimeout": "300",
                    "defaultTaskList": {
                        "name": "None"
                    },
                    "defaultChildPolicy": "TERMINATE"
                },
                "typeInfo": {
                    "status": "REGISTERED",
                    "creationDate": 1364492094.968,
                    "workflowType": {
                        "version": "1",
                        "name": "testW"
                    }
                }
            }
        """
        try:
            response = self.connection.describe_workflow_type(self.domain.name, name, version)
        except SWFResponseError as e:
            if e.error_code == 'UnknownResourceFault':
                raise DoesNotExistError(e.body['message'])

            raise ResponseError(e.body['message'])

        wt_info = response[self._infos]
        wt_config = response['configuration']

        task_list = kwargs.get('task_list')
        if task_list is None:
            task_list = get_subkey(wt_config, ['defaultTaskList', 'name'])

        child_policy = kwargs.get('child_policy')
        if child_policy is None:
            child_policy = wt_config.get('defaultChildPolicy')

        decision_task_timeout = kwargs.get('decision_task_timeout')
        if decision_task_timeout is None:
            decision_task_timeout = wt_config.get(
                'defaultTaskStartToCloseTimeout')

        execution_timeout = kwargs.get('execution_timeout')
        if execution_timeout is None:
            execution_timeout = wt_config.get(
                'defaultExecutionStartToCloseTimeout')

        decision_tasks_timeout = kwargs.get('decision_tasks_timeout')
        if decision_tasks_timeout is None:
            decision_tasks_timeout = wt_config.get(
                'defaultTaskStartToCloseTimeout')

        return self.to_WorkflowType(
            self.domain,
            wt_info,
            task_list=task_list,
            child_policy=child_policy,
            execution_timeout=execution_timeout,
            decision_tasks_timeout=decision_tasks_timeout,
        )

    def get_or_create(self, name, version,
                      status=REGISTERED,
                      creation_date=0.0,
                      deprecation_date=0.0,
                      task_list=None,
                      child_policy=CHILD_POLICIES.TERMINATE,
                      execution_timeout='300',
                      decision_tasks_timeout='300',
                      description=None,
                      *args, **kwargs):
        """Fetches, or creates the ActivityType with ``name`` and ``version``

        When fetching trying to fetch a matching workflow type, only
        name and version parameters are taken in account.
        Anyway, If you'd wanna make sure that in case the workflow type
        has to be created it is made with specific values, just provide it.

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

        :returns: Fetched or created WorkflowType model object
        :rtype: WorkflowType
        """
        try:
            return self.get(name,
                            version,
                            task_list=task_list,
                            child_policy=child_policy,
                            execution_timeout=execution_timeout,
                            decision_tasks_timeout=decision_tasks_timeout)

        except DoesNotExistError:
            try:
                return self.create(
                    name,
                    version,
                    status=status,
                    creation_date=creation_date,
                    deprecation_date=deprecation_date,
                    task_list=task_list,
                    child_policy=child_policy,
                    execution_timeout=execution_timeout,
                    decision_tasks_timeout=decision_tasks_timeout,
                    description=description,
                )
            # race conditon could happen if two workflows trying to register the same type
            except AlreadyExistsError:
                return self.get(name,
                            version,
                            task_list=task_list,
                            child_policy=child_policy,
                            execution_timeout=execution_timeout,
                            decision_tasks_timeout=decision_tasks_timeout)

    def _list(self, *args, **kwargs):
        return self.connection.list_workflow_types(*args, **kwargs)

    def filter(self, domain=None,
               registration_status=REGISTERED,
               name=None,
               *args, **kwargs):
        """Filters workflows based on the ``domain`` they belong to,
        their ``status``, and/or their ``name``

        :param      domain: domain the workflow type belongs to
        :type       domain: swf.models.domain.Domain

        :param      registration_status: workflow type registration status to match,
                                         Valid values are:
                                         * ``swf.constants.REGISTERED``
                                         * ``swf.constants.DEPRECATED``

        :type       registration_status: string

        :param      name: workflow type name to match
        :type       name: string

        :returns: list of matched WorkflowType models objects
        :rtype: list
        """
        # As WorkflowTypeQuery has to be built against a specific domain
        # name, domain filter is disposable, but not mandatory.
        domain = domain or self.domain
        return [self.to_WorkflowType(domain, wf) for wf in
                self._list_items(domain.name, registration_status, name=name)]

    def all(self, registration_status=REGISTERED, *args, **kwargs):
        """Retrieves every Workflow types

        :param      registration_status: workflow type registration status to match,
                                 Valid values are:
                                 * ``swf.constants.REGISTERED``
                                 * ``swf.constants.DEPRECATED``

        :type       registration_status: string

        A typical Amazon response looks like:

        .. code-block:: json

            {
                "typeInfos": [
                    {
                        "status": "REGISTERED",
                        "creationDate": 1364293450.67,
                        "description": "",
                        "workflowType": {
                            "version": "1",
                            "name": "Crawl"
                        }
                    },
                    {
                        "status": "REGISTERED",
                        "creationDate": 1364492094.968,
                        "workflowType": {
                            "version": "1",
                            "name": "testW"
                        }
                    }
                ]
            }
        """
        return self.filter(registration_status=registration_status)

    def create(self, name, version,
               status=REGISTERED,
               creation_date=0.0,
               deprecation_date=0.0,
               task_list=None,
               child_policy=CHILD_POLICIES.TERMINATE,
               execution_timeout='300',
               decision_tasks_timeout='300',
               description=None,
               *args, **kwargs):
        """Creates a new remote workflow type and returns the
        created WorkflowType model instance.

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
        workflow_type = WorkflowType(
            self.domain,
            name,
            version,
            status=status,
            creation_date=creation_date,
            deprecation_date=deprecation_date,
            task_list=task_list,
            child_policy=child_policy,
            execution_timeout=execution_timeout,
            decision_tasks_timeout=decision_tasks_timeout,
            description=description
        )
        workflow_type.save()

        return workflow_type


class WorkflowExecutionQuerySet(BaseWorkflowQuerySet):
    """Fetches Workflow executions"""

    _infos = 'executionInfo'
    _infos_plural = 'executionInfos'

    def _is_valid_status_param(self, status, param):
        statuses = {
            WorkflowExecution.STATUS_OPEN: set([
                'oldest_date',
                'latest_date'],
            ),
            WorkflowExecution.STATUS_CLOSED: set([
                'start_latest_date',
                'start_oldest_date',
                'close_latest_date',
                'close_oldest_date',
                'close_status'
            ]),
        }
        return param in statuses.get(status, set())

    def _validate_status_parameters(self, status, params):
        return [param for param in params if
                not self._is_valid_status_param(status, param)]

    def list_workflow_executions(self, status, *args, **kwargs):
        statuses = {
            WorkflowExecution.STATUS_OPEN: 'open',
            WorkflowExecution.STATUS_CLOSED: 'closed',
        }

        # boto.swf.list_closed_workflow_executions awaits a `start_oldest_date`
        # MANDATORY kwarg, when boto.swf.list_open_workflow_executions awaits a
        # `oldest_date` mandatory arg.
        if status == WorkflowExecution.STATUS_OPEN:
            kwargs['oldest_date'] = kwargs.pop('start_oldest_date')

        try:
            method = 'list_{}_workflow_executions'.format(statuses[status])
            return getattr(self.connection, method)(*args, **kwargs)
        except KeyError:
            raise ValueError("Unknown status provided: %s" % status)

    def get_workflow_type(self, execution_info):
        workflow_type = execution_info['workflowType']
        workflow_type_qs = WorkflowTypeQuerySet(self.domain)

        return workflow_type_qs.get(
            workflow_type['name'],
            workflow_type['version'],
        )

    def to_WorkflowExecution(self, domain, execution_info, **kwargs):
        workflow_type = WorkflowType(
            self.domain,
            execution_info['workflowType']['name'],
            execution_info['workflowType']['version']
        )

        return WorkflowExecution(
            domain,
            get_subkey(execution_info, ['execution', 'workflowId']),  # workflow_id
            run_id=get_subkey(execution_info, ['execution', 'runId']),
            workflow_type=workflow_type,
            status=execution_info.get('executionStatus'),
            close_status=execution_info.get('closeStatus'),
            tag_list=execution_info.get('tagList'),
            start_timestamp=execution_info.get('startTimestamp'),
            close_timestamp=execution_info.get('closeTimestamp'),
            cancel_requested=execution_info.get('cancelRequested'),
            parent=execution_info.get('parent'),
            **kwargs
        )

    def get(self, workflow_id, run_id, *args, **kwargs):
        """ """
        try:
            response = self.connection.describe_workflow_execution(
                self.domain.name,
                run_id,
                workflow_id)
        except SWFResponseError as e:
            if e.error_code == 'UnknownResourceFault':
                raise DoesNotExistError(e.body['message'])

            raise ResponseError(e.body['message'])

        execution_info = response[self._infos]
        execution_config = response['executionConfiguration']

        return self.to_WorkflowExecution(
            self.domain,
            execution_info,
            task_list=get_subkey(execution_config, ['taskList', 'name']),
            child_policy=execution_config.get('childPolicy'),
            execution_timeout=execution_config.get('executionStartToCloseTimeout'),
            decision_tasks_timeout=execution_config.get('taskStartToCloseTimeout'),
            latest_activity_task_timestamp=response.get('latestActivityTaskTimestamp'),
            latest_execution_context=response.get('latestExecutionContext'),
            open_counts=response['openCounts'],
        )

    def filter(self,
               status=WorkflowExecution.STATUS_OPEN, tag=None,
               workflow_id=None, workflow_type_name=None,
               workflow_type_version=None,
               *args, **kwargs):
        """Filters workflow executions based on kwargs provided criteras

        :param  status: workflow executions with provided status will be kept.
                        Valid values are:
                        * ``swf.models.WorkflowExecution.STATUS_OPEN``
                        * ``swf.models.WorkflowExecution.STATUS_CLOSED``
        :type   status: string

        :param  tag: workflow executions containing the tag will be kept
        :type   tag: String

        :param  workflow_id: workflow executions attached to the id will be kept
        :type   workflow_id: String

        :param  workflow_type_name: workflow executions attached to the workflow type
                                    with provided name will be kept
        :type   workflow_type_name: String

        :param  workflow_type_version: workflow executions attached to the workflow type
                                       of the provided version will be kept
        :type   workflow_type_version: String

        **Be aware that** querying over status allows the usage of statuses specific
        kwargs

        * STATUS_OPEN

            :param start_latest_date: latest start or close date and time to return (in days)
            :type  start_latest_date: int

        * STATUS_CLOSED

            :param  start_latest_date: workflow executions that meet the start time criteria
                                       of the filter are kept (in days)
            :type   start_latest_date: int

            :param  start_oldest_date: workflow executions that meet the start time criteria
                                       of the filter are kept (in days)
            :type   start_oldest_date: int

            :param  close_latest_date: workflow executions that meet the close time criteria
                                       of the filter are kept (in days)
            :type   close_latest_date: int

            :param  close_oldest_date: workflow executions that meet the close time criteria
                                       of the filter are kept (in days)
            :type   close_oldest_date: int

            :param  close_status: must match the close status of an execution for it
                                  to meet the criteria of this filter.
                                  Valid values are:
                                  * ``CLOSE_STATUS_COMPLETED``
                                  * ``CLOSE_STATUS_FAILED``
                                  * ``CLOSE_STATUS_CANCELED``
                                  * ``CLOSE_STATUS_TERMINATED``
                                  * ``CLOSE_STATUS_CONTINUED_AS_NEW``
                                  * ``CLOSE_TIMED_OUT``
            :type   close_status: string

            :returns: workflow executions objects list
            :rtype: list
        """
        # As WorkflowTypeQuery has to be built against a specific domain
        # name, domain filter is disposable, but not mandatory.
        invalid_kwargs = self._validate_status_parameters(status, kwargs)

        if invalid_kwargs:
            err_msg = 'Invalid keyword arguments supplied: {}'.format(
                      ', '.join(invalid_kwargs))
            raise InvalidKeywordArgumentError(err_msg)

        if status == WorkflowExecution.STATUS_OPEN:
            oldest_date = kwargs.pop('oldest_date', 30)
        else:
            # The SWF docs on ListClosedWorkflowExecutions state that:
            #
            #   "startTimeFilter and closeTimeFilter are mutually exclusive"
            #
            # so we must figure out if we have to add a default value for
            # start_oldest_date or not.
            if "close_latest_date" in kwargs or "close_oldest_date" in kwargs:
                default_oldest_date = None
            else:
                default_oldest_date = 30
            oldest_date = kwargs.pop('start_oldest_date', default_oldest_date)

        # Compute a timestamp from the delta in days we got from params
        # If oldest_date is blank at this point, it's because we didn't want
        # it, so let's leave it blank and assume the user provided an other
        # time filter.
        if oldest_date:
            start_oldest_date = int(datetime_timestamp(past_day(oldest_date)))
        else:
            start_oldest_date = None

        return [self.to_WorkflowExecution(self.domain, wfe) for wfe in
                self._list_items(
                    *args,
                    domain=self.domain.name,
                    status=status,
                    workflow_id=workflow_id,
                    workflow_name=workflow_type_name,
                    workflow_version=workflow_type_version,
                    start_oldest_date=start_oldest_date,
                    tag=tag,
                    **kwargs
                )]

    def _list(self, *args, **kwargs):
        return self.list_workflow_executions(*args, **kwargs)

    def all(self, status=WorkflowExecution.STATUS_OPEN,
            start_oldest_date=30,
            *args, **kwargs):
        """Fetch every workflow executions during the last `start_oldest_date`
        days, with `status`

        :param  status: Workflow executions status filter
        :type   status: swf.models.WorkflowExecution.{STATUS_OPEN, STATUS_CLOSED}

        :param  start_oldest_date: Specifies the oldest start/close date to return.
        :type   start_oldest_date: integer (days)

        :returns: workflow executions objects list
        :rtype: list

        A typical amazon response looks like:

        .. code-block:: json

            {
                "executionInfos": [
                    {
                        "cancelRequested": "boolean",
                        "closeStatus": "string",
                        "closeTimestamp": "number",
                        "execution": {
                            "runId": "string",
                            "workflowId": "string"
                        },
                        "executionStatus": "string",
                        "parent": {
                            "runId": "string",
                            "workflowId": "string"
                        },
                        "startTimestamp": "number",
                        "tagList": [
                            "string"
                        ],
                        "workflowType": {
                            "name": "string",
                            "version": "string"
                        }
                    }
                ],
                "nextPageToken": "string"
            }
        """
        start_oldest_date = datetime_timestamp(past_day(start_oldest_date))

        return [self.to_WorkflowExecution(self.domain, wfe) for wfe
                in self._list_items(
                    status,
                    self.domain.name,
                    start_oldest_date=int(start_oldest_date))]
