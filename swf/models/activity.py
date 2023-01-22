# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from boto.swf.exceptions import SWFResponseError, SWFTypeAlreadyExistsError

from swf import exceptions
from swf.constants import REGISTERED
from swf.exceptions import AlreadyExistsError, DoesNotExistError, ResponseError, raises
from swf.models import BaseModel
from swf.models.base import ModelDiff
from swf.utils import immutable


class ActivityTypeDoesNotExist(Exception):
    pass


@immutable
class ActivityType(BaseModel):
    """ActivityType wrapper

    :param  domain: Domain the workflow type should be registered in
    :type   domain: swf.models.Domain

    :param  name: name of the ActivityType
    :type   name: str

    :param  version: version of the ActivityType
    :type   version: str

    :param  status: ActivityType status
    :type   status: swf.constants.{REGISTERED, DEPRECATED}

    :param  description: ActivityType description
    :type   description: str | None

    :param   creation_date: creation date of the current ActivityType
    :type    creation_date: float (timestamp)

    :param   deprecation_date: deprecation date of ActivityType
    :type    deprecation_date: float (timestamp)

    :param  task_list: specifies the default task list to use for scheduling
                       tasks of this activity type.
    :type   task_list: str

    :param  task_heartbeat_timeout: default maximum time before which a worker
                                    processing a task of this type must report
                                    progress by calling RecordActivityTaskHeartbeat.
    :type   task_heartbeat_timeout: int

    :param  task_schedule_to_close_timeout: default maximum duration for a task
                                            of this activity type.
    :type   task_schedule_to_close_timeout: int

    :param  task_schedule_to_start_timeout: default maximum duration that a
                                            task of this activity type can wait
                                            before being assigned to a worker.
    :type   task_schedule_to_start_timeout: int

    :param   task_start_to_close_timeout: default maximum duration that a
                                          worker can take to process tasks of
                                          this activity type.
    :type    task_start_to_close_timeout: int
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
        name,
        version,
        status=REGISTERED,
        description=None,
        creation_date=0.0,
        deprecation_date=0.0,
        task_list=None,
        task_heartbeat_timeout=0,
        task_schedule_to_close_timeout=0,
        task_schedule_to_start_timeout=0,
        task_start_to_close_timeout=0,
        *args,
        **kwargs,
    ):

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

    def _diff(self):
        """Checks for differences between ActivityType instance
        and upstream version

        :returns: A list (swf.models.base.ModelDiff) namedtuple describing
                  differences
        :rtype: ModelDiff
        """
        try:
            description = self.connection.describe_activity_type(self.domain.name, self.name, self.version)
        except SWFResponseError as err:
            if err.error_code == "UnknownResourceFault":
                raise DoesNotExistError("Remote ActivityType does not exist")

            raise ResponseError(err.body["message"])

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
        )

    @property
    @exceptions.is_not(ActivityTypeDoesNotExist)
    @exceptions.catch(
        SWFResponseError,
        raises(
            ActivityTypeDoesNotExist,
            when=exceptions.is_unknown("ActivityType"),
            extract=exceptions.extract_resource,
        ),
    )
    def exists(self):
        """Checks if the ActivityType exists amazon-side

        :rtype: bool
        """
        self.connection.describe_activity_type(self.domain.name, self.name, self.version)
        return True

    def save(self):
        """Creates the activity type amazon side"""
        try:
            self.connection.register_activity_type(
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
        except SWFTypeAlreadyExistsError:
            raise AlreadyExistsError(f"{self} already exists")
        except SWFResponseError as err:
            if err.error_code in ["UnknownResourceFault", "TypeDeprecatedFault"]:
                raise DoesNotExistError(err.body["message"])
            raise

    @exceptions.catch(
        SWFResponseError,
        raises(
            ActivityTypeDoesNotExist,
            when=exceptions.is_unknown("ActivityType"),
            extract=exceptions.extract_resource,
        ),
    )
    def delete(self):
        """Deprecates the domain amazon side"""
        self.connection.deprecate_activity_type(self.domain.name, self.name, self.version)

    def upstream(self):
        from swf.querysets.activity import ActivityTypeQuerySet

        qs = ActivityTypeQuerySet(self.domain)
        return qs.get(self.name, self.version)

    def __repr__(self):
        return "<{} domain={} name={} version={} status={}>".format(
            self.__class__.__name__,
            self.domain.name,
            self.name,
            self.version,
            self.status,
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

    def __init__(
        self,
        domain,
        task_list,
        task_token=None,
        activity_type=None,
        workflow_execution=None,
        input=None,
        activity_id=None,
        started_event_id=None,
        context=None,
    ):
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
    def from_poll(cls, domain, task_list, data):
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
