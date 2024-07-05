from __future__ import annotations

from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from simpleflow.swf.mapper.constants import REGISTERED
from simpleflow.swf.mapper.exceptions import DoesNotExistError, ResponseError, extract_error_code, extract_message
from simpleflow.swf.mapper.models.activity import ActivityType
from simpleflow.swf.mapper.querysets.base import BaseQuerySet
from simpleflow.swf.mapper.utils import get_subkey

if TYPE_CHECKING:
    from simpleflow.swf.mapper.models.domain import Domain


class ActivityTypeQuerySet(BaseQuerySet):
    """Swf activity type queryset object

    Allows the user to interact with amazon's swf
    activity types through a django-queryset-like interface

    :param      domain: domain the activity type belongs to
    """

    # Explicit is better than implicit, keep zen
    _infos = "typeInfo"
    _infos_plural = "typeInfos"

    def __init__(self, domain: Domain, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain = domain

    @property
    def domain(self) -> Domain | None:
        if not hasattr(self, "_domain"):
            self._domain = None
        return self._domain

    @domain.setter
    def domain(self, value: Domain):
        # Avoiding circular import
        from simpleflow.swf.mapper.models.domain import Domain

        if not isinstance(value, Domain):
            err = "domain property has to be of" f"swf.model.domain.Domain type, not {type(value)!r}"
            raise TypeError(err)
        self._domain = value

    def to_ActivityType(self, domain: Domain, type_info: dict[str, Any], **kwargs) -> ActivityType:
        return ActivityType(
            domain,
            type_info["activityType"]["name"],
            type_info["activityType"]["version"],
            status=type_info.get("status"),
            description=type_info.get("description"),
            creation_date=type_info.get("creationDate"),
            deprecation_date=type_info.get("deprecationDate"),
            **kwargs,
        )

    def _list(self, *args, **kwargs):
        return self.list_activity_types(*args, **kwargs)["typeInfos"]

    def get(self, name: str, version: str, *args, **kwargs) -> ActivityType:
        """Fetches the activity type with provided ``name`` and ``version``

        :param      name: activity type name to fetch
        :param      version: activity version to fetch
        :returns: Matched activity type instance

        A typical Amazon response looks like:

        .. code-block:: json

            {
                "configuration": {
                    "defaultTaskHeartbeatTimeout": "string",
                    "defaultTaskList": {
                        "name": "string"
                    },
                    "defaultTaskScheduleToCloseTimeout": "string",
                    "defaultTaskScheduleToStartTimeout": "string",
                    "defaultTaskStartToCloseTimeout": "string"
                },
                "typeInfo": {
                    "activityType": {
                        "name": "string",
                        "version": "string"
                    },
                    "creationDate": "number",
                    "deprecationDate": "number",
                    "description": "string",
                    "status": "string"
                }
            }
        """
        try:
            response = self.describe_activity_type(self.domain.name, name, version)
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(message) from e

            raise ResponseError(message, error_code=error_code) from e

        activity_info = response[self._infos]
        activity_config = response["configuration"]

        task_list = kwargs.get("task_list")
        if task_list is None:
            task_list = get_subkey(activity_config, ["defaultTaskList", "name"])

        task_heartbeat_timeout = kwargs.get("task_heartbeat_timeout")
        if task_heartbeat_timeout is None:
            task_heartbeat_timeout = activity_config.get("defaultTaskHeartbeatTimeout")

        task_schedule_to_close_timeout = kwargs.get("task_schedule_to_close_timeout")
        if task_schedule_to_close_timeout is None:
            task_schedule_to_close_timeout = activity_config.get("defaultTaskScheduleToCloseTimeout")

        task_schedule_to_start_timeout = kwargs.get("task_schedule_to_start_timeout")
        if task_schedule_to_start_timeout is None:
            task_schedule_to_start_timeout = activity_config.get("defaultTaskScheduleToStartTimeout")

        task_start_to_close_timeout = kwargs.get("task_start_to_close_timeout")
        if task_start_to_close_timeout is None:
            task_start_to_close_timeout = activity_config.get("defaultTaskStartToCloseTimeout")

        return self.to_ActivityType(
            self.domain,
            activity_info,
            task_list=task_list,
            task_heartbeat_timeout=task_heartbeat_timeout,
            task_schedule_to_close_timeout=task_schedule_to_close_timeout,
            task_schedule_to_start_timeout=task_schedule_to_start_timeout,
            task_start_to_close_timeout=task_start_to_close_timeout,
        )

    def get_or_create(
        self,
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
    ) -> ActivityType:
        """Fetches, or creates the ActivityType with ``name`` and ``version``

        When fetching trying to fetch a matching activity type, only
        name and version parameters are taken in account.
        Anyway, If you'd wanna make sure that in case the activity type
        has to be created it is made with specific values, just provide it.

        :param  name: name of the ActivityType
        :param  version: version of the ActivityType
        :param  status: ActivityType status
        :type   status:  simpleflow.swf.mapper.constants.{REGISTERED, DEPRECATED}
        :param  description: ActivityType description
        :param   creation_date: creation date of the current ActivityType
        :param   deprecation_date: deprecation date of ActivityType
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
        :returns: Fetched or created ActivityType model object
        """
        try:
            return self.get(
                name,
                version,
                task_list=task_list,
                task_heartbeat_timeout=task_heartbeat_timeout,
                task_schedule_to_close_timeout=task_schedule_to_close_timeout,
                task_schedule_to_start_timeout=task_schedule_to_start_timeout,
                task_start_to_close_timeout=task_start_to_close_timeout,
                *args,
                **kwargs,
            )
        except DoesNotExistError:
            return self.create(
                name,
                version,
                status=status,
                description=description,
                creation_date=creation_date,
                deprecation_date=deprecation_date,
                task_list=task_list,
                task_heartbeat_timeout=task_heartbeat_timeout,
                task_schedule_to_close_timeout=task_schedule_to_close_timeout,
                task_schedule_to_start_timeout=task_schedule_to_start_timeout,
                task_start_to_close_timeout=task_start_to_close_timeout,
            )

    def filter(
        self,
        domain: Domain | None = None,
        registration_status: str = REGISTERED,
        name: str | None = None,
        *args,
        **kwargs,
    ) -> list[ActivityType]:
        """Filters activity types based on their status, and/or name

        :param      domain: domain the activity type belongs to
        :param      registration_status: activity type registration status to match,
                                         Valid values are:
                                         * `` simpleflow.swf.mapper.constants.REGISTERED``
                                         * `` simpleflow.swf.mapper.constants.DEPRECATED``

        :param      name: activity type name to match
        :returns: list of matched ActivityType models objects
        """
        # name, domain filter is disposable, but not mandatory.
        domain = domain or self.domain
        return [
            self.to_ActivityType(domain, type_info)
            for type_info in self._list(domain.name, registration_status, name=name)
        ]

    def all(self, registration_status: str = REGISTERED, *args, **kwargs) -> list[ActivityType]:
        """Retrieves every activity types

        :param      registration_status: activity type registration status to match,
                                         Valid values are:
                                         * `` simpleflow.swf.mapper.constants.REGISTERED``
                                         * `` simpleflow.swf.mapper.constants.DEPRECATED``
        :returns: list of matched ActivityType models objects

        A typical Amazon response looks like:

        .. code-block:: json

            {
                "nextPageToken": "string",
                "typeInfos": [
                    {
                        "activityType": {
                            "name": "string",
                            "version": "string"
                        },
                        "creationDate": "number",
                        "deprecationDate": "number",
                        "description": "string",
                        "status": "string"
                    }
                ]
            }
        """

        def get_activity_types():
            response = {"nextPageToken": None}
            while "nextPageToken" in response:
                response = self.list_activity_types(
                    self.domain.name,
                    registration_status,
                    next_page_token=response["nextPageToken"],
                )

                yield from response[self._infos_plural]

        return [self.to_ActivityType(self.domain, activity_info) for activity_info in get_activity_types()]

    def create(
        self,
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
    ) -> ActivityType:
        """Creates a new remote activity type and returns the
        created ActivityType model instance.

        :param  name: name of the ActivityType
        :param  version: version of the ActivityType
        :param  status: ActivityType status
        :type   status:  simpleflow.swf.mapper.constants.{REGISTERED, DEPRECATED}
        :param  description: ActivityType description
        :param   creation_date: creation date of the current ActivityType
        :param   deprecation_date: deprecation date of ActivityType
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
        activity_type = ActivityType(
            self.domain,
            name,
            version,
            status=status,
            description=description,
            creation_date=creation_date,
            deprecation_date=deprecation_date,
            task_list=task_list,
            task_heartbeat_timeout=task_heartbeat_timeout,
            task_schedule_to_close_timeout=task_schedule_to_close_timeout,
            task_schedule_to_start_timeout=task_schedule_to_start_timeout,
            task_start_to_close_timeout=task_start_to_close_timeout,
        )
        activity_type.save()

        return activity_type
