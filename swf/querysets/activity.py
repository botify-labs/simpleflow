# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from typing import TYPE_CHECKING

from boto.swf.exceptions import SWFResponseError

from swf.constants import REGISTERED
from swf.exceptions import DoesNotExistError, ResponseError
from swf.models.activity import ActivityType
from swf.querysets.base import BaseQuerySet
from swf.utils import get_subkey

if TYPE_CHECKING:
    from swf.models.domain import Domain


class ActivityTypeQuerySet(BaseQuerySet):
    """Swf activity type queryset object

    Allows the user to interact with amazon's swf
    activity types through a django-queryset-like interface

    :param      domain: domain the activity type belongs to
    :type       domain: swf.models.domain.Domain
    """

    # Explicit is better than implicit, keep zen
    _infos = "typeInfo"
    _infos_plural = "typeInfos"

    def __init__(self, domain, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain = domain

    @property
    def domain(self) -> Domain | None:
        if not hasattr(self, "_domain"):
            self._domain = None
        return self._domain

    @domain.setter
    def domain(self, value):
        # Avoiding circular import
        from swf.models.domain import Domain

        if not isinstance(value, Domain):
            err = "domain property has to be of" "swf.model.domain.Domain type, not %r" % type(value)
            raise TypeError(err)
        self._domain = value

    def to_ActivityType(self, domain, type_info, **kwargs):
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
        return self.connection.list_activity_types(*args, **kwargs)["typeInfos"]

    def get(self, name, version, *args, **kwargs):
        """Fetches the activity type with provided ``name`` and ``version``

        :param      name: activity type name to fetch
        :type       name: String

        :param      version: activity version to fetch
        :type       version: String

        :returns: Matched activity type instance
        :rtype: swf.models.activity.ActivityType

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
            response = self.connection.describe_activity_type(self.domain.name, name, version)
        except SWFResponseError as e:
            if e.error_code == "UnknownResourceFault":
                raise DoesNotExistError(e.error_message)

            raise ResponseError(e.error_message)

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
        """Fetches, or creates the ActivityType with ``name`` and ``version``

        When fetching trying to fetch a matching activity type, only
        name and version parameters are taken in account.
        Anyway, If you'd wanna make sure that in case the activity type
        has to be created it is made with specific values, just provide it.

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

        :returns: Fetched or created ActivityType model object
        :rtype: ActivityType
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

    def filter(self, domain=None, registration_status=REGISTERED, name=None, *args, **kwargs):
        """Filters activity types based on their status, and/or name

        :param      domain: domain the activity type belongs to
        :type       domain: swf.models.domain.Domain

        :param      registration_status: activity type registration status to match,
                                         Valid values are:
                                         * ``swf.constants.REGISTERED``
                                         * ``swf.constants.DEPRECATED``

        :type       registration_status: string

        :param      name: activity type name to match
        :type       name: string

        :returns: list of matched ActivityType models objects
        :rtype: list
        """
        # name, domain filter is disposable, but not mandatory.
        domain = domain or self.domain
        return [
            self.to_ActivityType(domain, type_info)
            for type_info in self._list(domain.name, registration_status, name=name)
        ]

    def all(self, registration_status=REGISTERED, *args, **kwargs):
        """Retrieves every activity types

        :param      registration_status: activity type registration status to match,
                                         Valid values are:
                                         * ``swf.constants.REGISTERED``
                                         * ``swf.constants.DEPRECATED``

        :type       registration_status: string

        :returns: list of matched ActivityType models objects
        :rtype: list

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
                response = self.connection.list_activity_types(
                    self.domain.name,
                    registration_status,
                    next_page_token=response["nextPageToken"],
                )

                yield from response[self._infos_plural]

        return [self.to_ActivityType(self.domain, activity_info) for activity_info in get_activity_types()]

    def create(
        self,
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
        """Creates a new remote activity type and returns the
        created ActivityType model instance.

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
