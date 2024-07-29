from __future__ import annotations

import collections
import time
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from simpleflow import format
from simpleflow.swf.mapper import exceptions
from simpleflow.swf.mapper.constants import REGISTERED
from simpleflow.swf.mapper.exceptions import (
    AlreadyExistsError,
    DoesNotExistError,
    ResponseError,
    WorkflowExecutionAlreadyStartedError,
    extract_error_code,
    extract_message,
    raises,
)
from simpleflow.swf.mapper.models.base import BaseModel, ModelDiff
from simpleflow.swf.mapper.models.domain import Domain
from simpleflow.swf.mapper.models.history.base import History
from simpleflow.swf.mapper.utils import immutable

if TYPE_CHECKING:
    from typing import Any


_POLICIES = (
    "TERMINATE",  # child executions will be terminated
    "REQUEST_CANCEL",  # a request to cancel will be attempted for each child execution
    "ABANDON",  # no action will be taken
)

CHILD_POLICIES = collections.namedtuple("CHILD_POLICY", " ".join(_POLICIES))(*_POLICIES)


class WorkflowTypeDoesNotExist(DoesNotExistError):
    pass


class WorkflowExecutionDoesNotExist(DoesNotExistError):
    pass


@immutable
class WorkflowType(BaseModel):
    """Simple Workflow Type wrapper

    :param  domain: Domain the workflow type should be registered in
    :param  name: name of the workflow type
    :param  version: workflow type version
    :param  status: workflow type status
    :type   status:  simpleflow.swf.mapper.core.ConnectedSWFObject.{REGISTERED, DEPRECATED}
    :param   creation_date: creation date of the current WorkflowType (timestamp)
    :param   deprecation_date: deprecation date of WorkflowType (timestamp)
    :param  task_list: task list to use for scheduling decision tasks for executions
                       of this workflow type
    :param  child_policy: policy to use for the child workflow executions
                          when a workflow execution of this type is terminated
    :param  execution_timeout: maximum duration for executions of this workflow type
    :param  decision_tasks_timeout: maximum duration of decision tasks for this workflow type
    :param  description: Textual description of the workflow type
    """

    __slots__ = [
        "domain",
        "name",
        "version",
        "status",
        "creation_date",
        "deprecation_date",
        "task_list",
        "child_policy",
        "execution_timeout",
        "decision_tasks_timeout",
        "description",
    ]

    def __init__(
        self,
        domain: Domain,
        name: str,
        version: str,
        status: str = REGISTERED,
        creation_date: float = 0.0,
        deprecation_date: float = 0.0,
        task_list: str | None = None,
        child_policy: CHILD_POLICIES = CHILD_POLICIES.TERMINATE,
        execution_timeout: str = "300",
        decision_tasks_timeout: str = "300",
        description: str | None = None,
        *args,
        **kwargs,
    ) -> None:
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

    def set_child_policy(self, policy: CHILD_POLICIES) -> None:
        if policy not in CHILD_POLICIES:
            raise ValueError(f"invalid child policy value: {policy}")

        self.child_policy = policy

    def _diff(self, ignore_fields: list[str] | None = None) -> ModelDiff:
        """Checks for differences between WorkflowType instance
        and upstream version

        :returns: A simpleflow.swf.mapper.models.base.ModelDiff describing
                  differences
        """
        try:
            description = self.describe_workflow_type(self.domain.name, self.name, self.version)
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError("Remote Domain does not exist") from e

            raise ResponseError(message, error_code=error_code) from e

        workflow_info = description["typeInfo"]
        workflow_config = description["configuration"]

        return ModelDiff(
            ("name", self.name, workflow_info["workflowType"]["name"]),
            ("version", self.version, workflow_info["workflowType"]["version"]),
            ("status", self.status, workflow_info["status"]),
            ("creation_date", self.creation_date, workflow_info["creationDate"]),
            (
                "deprecation_date",
                self.deprecation_date,
                workflow_info["deprecationDate"],
            ),
            ("task_list", self.task_list, workflow_config["defaultTaskList"]["name"]),
            ("child_policy", self.child_policy, workflow_config["defaultChildPolicy"]),
            (
                "execution_timeout",
                self.execution_timeout,
                workflow_config["defaultExecutionStartToCloseTimeout"],
            ),
            (
                "decision_tasks_timout",
                self.decision_tasks_timeout,
                workflow_config["defaultTaskStartToCloseTimeout"],
            ),
            ("description", self.description, workflow_info["description"]),
            ignore_fields=ignore_fields,
        )

    @property
    @exceptions.translate(ClientError, to=ResponseError)
    @exceptions.is_not(WorkflowTypeDoesNotExist)
    @exceptions.catch(
        ClientError,
        raises(
            WorkflowTypeDoesNotExist,
            when=exceptions.is_unknown("WorkflowType"),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def exists(self) -> bool:
        """Checks if the WorkflowType exists amazon-side"""
        self.describe_workflow_type(self.domain.name, self.name, self.version)
        return True

    def save(self) -> None:
        """Creates the workflow type amazon side"""
        try:
            self.register_workflow_type(
                self.domain.name,
                self.name,
                self.version,
                task_list=str(self.task_list),
                default_child_policy=str(self.child_policy),
                default_execution_start_to_close_timeout=str(self.execution_timeout),
                default_task_start_to_close_timeout=str(self.decision_tasks_timeout),
                description=self.description,
            )
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "TypeAlreadyExistsFault":
                raise AlreadyExistsError(f"Workflow type {self.name} already exists amazon-side") from e
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(message) from e
            raise

    def delete(self) -> None:
        """Deprecates the workflow type amazon-side"""
        try:
            self.deprecate_workflow_type(self.domain.name, self.name, self.version)
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code in ["UnknownResourceFault", "TypeDeprecatedFault"]:
                raise DoesNotExistError(message) from e

    def upstream(self) -> WorkflowType:
        from simpleflow.swf.mapper.querysets.workflow import WorkflowTypeQuerySet

        qs = WorkflowTypeQuerySet(self.domain)
        return qs.get(self.name, self.version)

    @exceptions.catch(
        ClientError,
        raises(
            WorkflowExecutionAlreadyStartedError,
            when=lambda error, *args, **kwargs: extract_error_code(error) == "WorkflowExecutionAlreadyStartedFault",
            extract=lambda *args, **kwargs: "",
        ),
    )
    def start_execution(
        self,
        workflow_id: str | None = None,
        task_list: str | None = None,
        child_policy: CHILD_POLICIES | None = None,
        execution_timeout: str | None = None,
        input: dict[str, Any] | None = None,
        tag_list: str | list[str] | None = None,
        decision_tasks_timeout: str | None = None,
    ) -> WorkflowExecution:
        """Starts a Workflow execution of current workflow type

        :param  workflow_id: The user defined identifier associated with the workflow execution
        :param  task_list: task list to use for scheduling decision tasks for execution
                        of this workflow
        :param  child_policy: policy to use for the child workflow executions
                              of this workflow execution.
        :param  execution_timeout: maximum duration for the workflow execution
        :param  input: Input of the workflow execution
        :param  tag_list: Tags associated with the workflow execution
        :param  decision_tasks_timeout: maximum duration of decision tasks
                                        for this workflow execution
        """
        workflow_id = workflow_id or "%s-%s-%i" % (self.name, self.version, time.time())
        task_list = task_list or self.task_list
        child_policy = child_policy or self.child_policy
        if child_policy not in CHILD_POLICIES:
            raise ValueError(f"invalid child policy value: {child_policy}")
        if input is None:
            input = {}
        if tag_list is not None and not isinstance(tag_list, list):
            tag_list = [tag_list]

        # checks
        if tag_list and len(tag_list) > 5:
            raise ValueError("You cannot have more than 5 tags in StartWorkflowExecution.")

        run_id = self.start_workflow_execution(
            self.domain.name,
            workflow_id,
            self.name,
            self.version,
            task_list=task_list,
            child_policy=child_policy,
            execution_start_to_close_timeout=execution_timeout,
            input=format.input(input),
            tag_list=tag_list,
            task_start_to_close_timeout=decision_tasks_timeout,
        )["runId"]

        return WorkflowExecution(self.domain, workflow_id, run_id=run_id)

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} domain={self.domain.name} name={self.name} version={self.version}"
            f" status={self.status}>"
        )


@immutable
class WorkflowExecution(BaseModel):
    """Simple Workflow execution wrapper

    :param  domain: Domain the workflow execution should be registered in
    :param  workflow_type: The WorkflowType associated with the workflow execution
                           is associated with
    :param  workflow_id: The user defined identifier associated with the workflow execution
    :param  run_id: The Amazon defined identifier associated with the workflow execution
    :param  status: Whether the WorkflowExecution instance represents an opened or
                    closed execution
    :param  task_list: The task list to use for the decision tasks generated
                       for this workflow execution.
    :param  input: input data of the execution, which will be passed around using
                   serialized json
    """

    STATUS_OPEN = "OPEN"
    STATUS_CLOSED = "CLOSED"

    CLOSE_STATUS_COMPLETED = "COMPLETED"
    CLOSE_STATUS_FAILED = "FAILED"
    CLOSE_STATUS_CANCELED = "CANCELED"
    CLOSE_STATUS_TERMINATED = "TERMINATED"
    CLOSE_STATUS_CONTINUED_AS_NEW = "CLOSE_STATUS_CONTINUED_AS_NEW"
    CLOSE_TIMED_OUT = "TIMED_OUT"

    kind = "execution"

    __slots__ = [
        "domain",
        "workflow_id",
        "run_id",
        "status",
        "workflow_type",
        "task_list",
        "child_policy",
        "close_status",
        "execution_timeout",
        "input",
        "tag_list",
        "decision_tasks_timeout",
        "close_timestamp",
        "start_timestamp",
        "cancel_requested",
        "latest_execution_context",
        "latest_activity_task_timestamp",
        "open_counts",
        "parent",
    ]

    def __init__(
        self,
        domain: Domain,
        workflow_id: str,
        run_id: str | None = None,
        status: str = STATUS_OPEN,
        workflow_type: WorkflowType | None = None,
        task_list: str | None = None,
        child_policy: str | None = None,
        close_status: str | None = None,
        execution_timeout: str | None = None,
        input: dict[str, Any] | None = None,
        tag_list: list[str] | None = None,
        decision_tasks_timeout: str | None = None,
        close_timestamp: float | None = None,
        start_timestamp: float | None = None,
        cancel_requested: bool | None = None,
        latest_execution_context: str | None = None,
        latest_activity_task_timestamp: float | None = None,
        open_counts: dict[str, int] | None = None,
        parent: dict | None = None,
        *args,
        **kwargs,
    ) -> None:
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
        self.open_counts = open_counts or {}  # so we can query keys in any case
        self.parent = parent or {}  # so we can query keys in any case

        # immutable decorator rebinds class name,
        # so have to use generice self.__class__
        super(self.__class__, self).__init__(*args, **kwargs)

    def _diff(self, ignore_fields: list[str] | None = None) -> ModelDiff:
        """Checks for differences between WorkflowExecution instance
        and upstream version

        :returns: A simpleflow.swf.mapper.models.base.ModelDiff describing
                  differences
        """
        try:
            description = self.describe_workflow_execution(self.domain.name, self.run_id, self.workflow_id)
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError("Remote Domain does not exist") from e

            raise ResponseError(message, error_code=error_code) from e

        execution_info = description["executionInfo"]
        execution_config = description["executionConfiguration"]

        return ModelDiff(
            (
                "workflow_id",
                self.workflow_id,
                execution_info["execution"]["workflowId"],
            ),
            ("run_id", self.run_id, execution_info["execution"]["runId"]),
            ("status", self.status, execution_info["executionStatus"]),
            ("task_list", self.task_list, execution_config["taskList"]["name"]),
            ("child_policy", self.child_policy, execution_config["childPolicy"]),
            (
                "execution_timeout",
                self.execution_timeout,
                execution_config["executionStartToCloseTimeout"],
            ),
            ("tag_list", self.tag_list, execution_info.get("tagList")),
            (
                "decision_tasks_timeout",
                self.decision_tasks_timeout,
                execution_config["taskStartToCloseTimeout"],
            ),
            ignore_fields=ignore_fields,
        )

    @property
    @exceptions.translate(ClientError, to=ResponseError)
    @exceptions.is_not(WorkflowExecutionDoesNotExist)
    @exceptions.catch(
        ClientError,
        raises(
            WorkflowExecutionDoesNotExist,
            when=exceptions.is_unknown(("WorkflowExecution", "workflowId")),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def exists(self) -> bool:
        """Checks if the WorkflowExecution exists amazon-side"""
        self.describe_workflow_execution(self.domain.name, self.run_id, self.workflow_id)
        return True

    def upstream(self) -> WorkflowExecution:
        from simpleflow.swf.mapper.querysets.workflow import WorkflowExecutionQuerySet

        qs = WorkflowExecutionQuerySet(self.domain)
        return qs.get(self.workflow_id, self.run_id)

    def history(self, *args, **kwargs) -> History:
        """Returns workflow execution history report

        :returns: The workflow execution complete events history
        """
        domain = kwargs.pop("domain", self.domain)
        if not isinstance(domain, str):
            domain = domain.name

        response = self.get_workflow_execution_history(domain, self.run_id, self.workflow_id, **kwargs)

        events: list[dict[str, Any]] = response["events"]
        next_page = response.get("nextPageToken")
        while next_page is not None:
            response = self.get_workflow_execution_history(
                domain,
                self.run_id,
                self.workflow_id,
                next_page_token=next_page,
                **kwargs,
            )

            events.extend(response["events"])
            next_page = response.get("nextPageToken")

        return History.from_event_list(events)

    @exceptions.translate(ClientError, to=ResponseError)
    @exceptions.catch(
        ClientError,
        raises(
            WorkflowExecutionDoesNotExist,
            when=exceptions.is_unknown(("WorkflowExecution", "workflowId")),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def signal(
        self,
        signal_name: str,
        input: dict[str, Any] | None = None,
        workflow_id: str | None = None,
        run_id: str | None = None,
        *args,
        **kwargs,
    ) -> None:
        """Records a signal event in the workflow execution history and
        creates a decision task.

        The signal event is recorded with the specified user defined
        ``signal_name`` and ``input`` (if provided).

        Default to send to oneself (for compatibility with the previous versions).

        :param  signal_name: The name of the signal. This name must be
                             meaningful to the target workflow.
        :param  input: Data to attach to the WorkflowExecutionSignaled
                       event in the target workflow execution’s history.
        :param  workflow_id: Workflow ID to send the signal to.
        :param  run_id: Run ID to send the signal to.
        """
        if input is None:
            input = {}
        self.signal_workflow_execution(
            self.domain.name,
            signal_name,
            workflow_id or self.workflow_id,
            input=format.input(input),
            run_id=run_id if workflow_id else self.run_id,
        )

    @exceptions.translate(ClientError, to=ResponseError)
    @exceptions.catch(
        ClientError,
        raises(
            WorkflowExecutionDoesNotExist,
            when=exceptions.is_unknown("domain"),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def request_cancel(self, *args, **kwargs) -> None:
        """Requests the workflow execution cancel"""
        self.request_cancel_workflow_execution(self.domain.name, self.workflow_id, run_id=self.run_id)

    @exceptions.translate(ClientError, to=ResponseError)
    @exceptions.catch(
        ClientError,
        raises(
            WorkflowExecutionDoesNotExist,
            when=exceptions.is_unknown(("WorkflowExecution", "workflowId")),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def terminate(
        self,
        child_policy: CHILD_POLICIES | None = None,
        details: str | None = None,
        reason: str | None = None,
    ) -> None:
        """Terminates the workflow execution"""
        self.terminate_workflow_execution(
            self.domain.name,
            self.workflow_id,
            run_id=self.run_id,
            child_policy=child_policy,
            details=format.details(details),
            reason=format.reason(reason),
        )
