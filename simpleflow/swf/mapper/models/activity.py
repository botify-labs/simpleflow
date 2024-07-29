from __future__ import annotations

from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from simpleflow.swf.mapper import exceptions
from simpleflow.swf.mapper.constants import REGISTERED
from simpleflow.swf.mapper.exceptions import (
    AlreadyExistsError,
    DoesNotExistError,
    ResponseError,
    extract_error_code,
    extract_message,
    raises,
)
from simpleflow.swf.mapper.models.base import BaseModel, ModelDiff
from simpleflow.swf.mapper.utils import immutable

if TYPE_CHECKING:
    from simpleflow.swf.mapper.models.domain import Domain
    from simpleflow.swf.mapper.models.workflow import WorkflowExecution


class ActivityTypeDoesNotExist(Exception):
    pass


@immutable
class ActivityType(BaseModel):
    """ActivityType wrapper

    :param  domain: Domain the workflow type should be registered in
    :param  name: name of the ActivityType
    :param  version: version of the ActivityType
    :param  status: ActivityType status
    :type   status:  simpleflow.swf.mapper.constants.{REGISTERED, DEPRECATED}
    :param  description: ActivityType description
    :param   creation_date: creation date of the current ActivityType
    :type    creation_date: float (timestamp)
    :param   deprecation_date: deprecation date of ActivityType
    :type    deprecation_date: float (timestamp)
    :param  task_list: specifies the default task list to use for scheduling
                       tasks of this activity type.
    :param  task_heartbeat_timeout: default maximum time before which a worker
                                    processing a task of this type must report
                                    progress by calling RecordActivityTaskHeartbeat.
    :param  task_schedule_to_close_timeout: default maximum duration for a task
                                            of this activity type.
    :param  task_schedule_to_start_timeout: default maximum duration that a
                                            task of this activity type can wait
                                            before being assigned to a worker.
    :param   task_start_to_close_timeout: default maximum duration that a
                                          worker can take to process tasks of
                                          this activity type.
    """

    kind = "type"

    __slots__ = [
        "domain",
        "name",
        "version",
        "status",
        "description",
        "creation_date",
        "deprecation_date",
        "task_list",
        "task_heartbeat_timeout",
        "task_schedule_to_close_timeout",
        "task_schedule_to_start_timeout",
        "task_start_to_close_timeout",
    ]

    def __init__(
        self,
        domain,
        name: str,
        version: str,
        status: str = REGISTERED,
        description: str | None = None,
        creation_date: float = 0.0,
        deprecation_date: float = 0.0,
        task_list: str | None = None,
        task_heartbeat_timeout: int = 0,
        task_schedule_to_close_timeout: int = 0,
        task_schedule_to_start_timeout: int = 0,
        task_start_to_close_timeout: int = 0,
        *args,
        **kwargs,
    ) -> None:
        self.domain = domain
        self.name = name
        self.version = version
        self.status = status
        self.description = description

        self.creation_date = creation_date
        self.deprecation_date = deprecation_date

        self.task_list = task_list
        self.task_heartbeat_timeout = task_heartbeat_timeout
        self.task_schedule_to_close_timeout = task_schedule_to_close_timeout
        self.task_schedule_to_start_timeout = task_schedule_to_start_timeout
        self.task_start_to_close_timeout = task_start_to_close_timeout

        # immutable decorator rebinds class name,
        # so have to use generic self.__class__
        super(self.__class__, self).__init__(*args, **kwargs)

    def _diff(self, ignore_fields: list[str] | None = None) -> ModelDiff:
        """Checks for differences between ActivityType instance
        and upstream version

        :returns: A list (simpleflow.swf.mapper.models.base.ModelDiff) namedtuple describing
                  differences
        """
        try:
            description = self.describe_activity_type(self.domain.name, self.name, self.version)
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError("Remote ActivityType does not exist") from e

            raise ResponseError(message, error_code=error_code) from e

        info = description["typeInfo"]
        config = description["configuration"]

        return ModelDiff(
            ("name", self.name, info["activityType"]["name"]),
            ("version", self.version, info["activityType"]["version"]),
            ("status", self.status, info["status"]),
            ("description", self.description, info["description"]),
            ("creation_date", self.creation_date, info["creationDate"]),
            ("deprecation_date", self.deprecation_date, info["deprecationDate"]),
            ("task_list", self.task_list, config["defaultTaskList"]["name"]),
            (
                "task_heartbeat_timeout",
                self.task_heartbeat_timeout,
                config["defaultTaskHeartbeatTimeout"],
            ),
            (
                "task_schedule_to_close_timeout",
                self.task_schedule_to_close_timeout,
                config["defaultTaskScheduleToCloseTimeout"],
            ),
            (
                "task_schedule_to_start_timeout",
                self.task_schedule_to_start_timeout,
                config["defaultTaskScheduleToStartTimeout"],
            ),
            (
                "task_start_to_close_timeout",
                self.task_start_to_close_timeout,
                config["defaultTaskStartToCloseTimeout"],
            ),
            ignore_fields=ignore_fields,
        )

    @property
    @exceptions.is_not(ActivityTypeDoesNotExist)
    @exceptions.catch(
        ClientError,
        raises(
            ActivityTypeDoesNotExist,
            when=exceptions.is_unknown("ActivityType"),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def exists(self) -> bool:
        """Checks if the ActivityType exists amazon-side"""
        self.describe_activity_type(self.domain.name, self.name, self.version)
        return True

    def save(self):
        """Creates the activity type amazon side"""
        try:
            self.register_activity_type(
                self.domain.name,
                self.name,
                self.version,
                task_list=str(self.task_list),
                default_task_heartbeat_timeout=str(self.task_heartbeat_timeout),
                default_task_schedule_to_close_timeout=str(self.task_schedule_to_close_timeout),
                default_task_schedule_to_start_timeout=str(self.task_schedule_to_start_timeout),
                default_task_start_to_close_timeout=str(self.task_start_to_close_timeout),
                description=self.description,
            )
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "TypeAlreadyExistsFault":
                raise AlreadyExistsError(f"{self} already exists") from e
            if error_code in ("UnknownResourceFault", "TypeDeprecatedFault"):
                raise DoesNotExistError(f"{error_code}: {message}") from e
            raise

    @exceptions.catch(
        ClientError,
        raises(
            ActivityTypeDoesNotExist,
            when=exceptions.is_unknown("ActivityType"),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def delete(self):
        """Deprecates the domain amazon side"""
        self.deprecate_activity_type(self.domain.name, self.name, self.version)

    def upstream(self):
        from simpleflow.swf.mapper.querysets.activity import ActivityTypeQuerySet

        qs = ActivityTypeQuerySet(self.domain)
        return qs.get(self.name, self.version)

    def __repr__(self):
        return (
            f"<{self.__class__.__name__} domain={self.domain.name} name={self.name} version={self.version}"
            f" status={self.status}>"
        )


@immutable
class ActivityTask(BaseModel):
    __slots__ = [
        "domain",
        "task_list",
        "task_token",
        "activity_type",
        "workflow_execution",
        "input",
        "activity_id",
        "started_event_id",
    ]

    # noinspection PyMissingConstructor
    def __init__(
        self,
        domain: Domain,
        task_list: str,
        task_token: str | None = None,
        activity_type: ActivityType | None = None,
        workflow_execution: WorkflowExecution | None = None,
        input: Any = None,
        activity_id: int | None = None,
        started_event_id: int | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        self.domain = domain
        self.task_list = task_list

        self.task_token = task_token
        self.activity_type = activity_type
        self.workflow_execution = workflow_execution
        self.input = input
        self.activity_id = activity_id
        self.started_event_id = started_event_id

        self.context = context

    @classmethod
    def from_poll(cls, domain: Domain, task_list: str, data: dict[str, Any]) -> ActivityTask:
        from .workflow import WorkflowExecution

        activity_type = ActivityType(domain, data["activityType"]["name"], data["activityType"]["version"])

        workflow_execution = WorkflowExecution(
            domain,
            data["workflowExecution"]["workflowId"],
            data["workflowExecution"]["runId"],
        )

        return cls(
            domain,
            task_list,
            task_token=data["taskToken"],
            activity_type=activity_type,
            workflow_execution=workflow_execution,
            input=data.get("input"),
            activity_id=data["activityId"],
            started_event_id=data["startedEventId"],
            context=data,
        )
