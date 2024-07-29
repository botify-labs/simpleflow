from __future__ import annotations

from typing import TYPE_CHECKING

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
    from simpleflow.swf.mapper.models.activity import ActivityType
    from simpleflow.swf.mapper.models.workflow import WorkflowType


class DomainDoesNotExist(DoesNotExistError):
    pass


@immutable
class Domain(BaseModel):
    """Simple Workflow Domain wrapper

    :param      name: Name of the domain to register (unique)
    :param      retention_period: Domain's workflow executions records retention in days
    :param      status: Specifies the registration status of the
                        workflow types to list. Valid values are:
                        *  simpleflow.swf.mapper.constants.REGISTERED
                        *  simpleflow.swf.mapper.constants.DEPRECATED
    :param      description: Textual description of the domain
    """

    __slots__ = [
        "name",
        "status",
        "description",
        "retention_period",
    ]

    def __init__(
        self,
        name: str,
        status: str = REGISTERED,
        description: str | None = None,
        retention_period: int = 30,
        *args,
        **kwargs,
    ) -> None:
        self.name = name
        self.status = status
        self.description = description
        self.retention_period = retention_period

        # immutable decorator rebinds class name,
        # so have to use generic self.__class__
        super(self.__class__, self).__init__(*args, **kwargs)

    @classmethod
    def check(cls, domain: Domain) -> None:
        """
        Ensures *domain* is a :class:`Domain` object and has a *name* attribute
        as a string.

        """
        if not isinstance(domain, cls) or not hasattr(domain, "name") or not isinstance(domain.name, str):
            raise TypeError(f"invalid type {type(domain)} for domain")

    def _diff(self, ignore_fields: list[str] | None = None) -> ModelDiff:
        """Checks for differences between Domain instance
        and upstream version

        :returns: A simpleflow.swf.mapper.models.base.ModelDiff describing
                  differences
        """
        try:
            description = self.describe_domain(self.name)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError("Remote Domain does not exist") from e

            raise ResponseError(e.args[0], error_code=error_code) from e

        domain_info = description["domainInfo"]
        domain_config = description["configuration"]

        return ModelDiff(
            ("name", self.name, domain_info["name"]),
            ("status", self.status, domain_info["status"]),
            ("description", self.description, domain_info.get("description")),
            (
                "retention_period",
                self.retention_period,
                domain_config["workflowExecutionRetentionPeriodInDays"],
            ),
            ignore_fields=ignore_fields,
        )

    @property
    @exceptions.translate(ClientError, to=ResponseError)
    @exceptions.is_not(DomainDoesNotExist)
    @exceptions.catch(
        ClientError,
        raises(
            DomainDoesNotExist,
            when=exceptions.is_unknown("domain"),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def exists(self) -> bool:
        """Checks if the Domain exists amazon-side

        :rtype: bool
        """
        try:
            self.describe_domain(self.name)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "UnknownResourceFault":
                return False
        return True

    def save(self) -> None:
        """Creates the domain amazon side"""
        try:
            self.register_domain(self.name, str(self.retention_period), self.description)
        except ClientError as e:
            error_code = extract_error_code(e)
            message = extract_message(e)
            if error_code == "DomainAlreadyExistsFault":
                raise AlreadyExistsError(f"Domain {self.name} already exists amazon-side") from e
            raise ResponseError(message, error_code=error_code) from e

    @exceptions.translate(ClientError, to=ResponseError)
    @exceptions.catch(
        ClientError,
        raises(
            DomainDoesNotExist,
            when=exceptions.is_unknown("domain"),
            extract=exceptions.generate_resource_not_found_message,
        ),
    )
    def delete(self) -> None:
        """Deprecates the domain amazon side"""
        self.deprecate_domain(self.name)

    def upstream(self) -> Domain:
        from simpleflow.swf.mapper.querysets.domain import DomainQuerySet

        qs = DomainQuerySet()
        return qs.get(self.name)

    def workflows(self, status=REGISTERED) -> list[WorkflowType]:
        """Lists the current domain's workflow types

        :param      status: Specifies the registration status of the
                            workflow types to list. Valid values are:
                            *  simpleflow.swf.mapper.constants.REGISTERED
                            *  simpleflow.swf.mapper.constants.DEPRECATED
        :type       status: string
        """
        from simpleflow.swf.mapper.querysets.workflow import WorkflowTypeQuerySet

        qs = WorkflowTypeQuerySet(self)
        return qs.all(registration_status=status)

    def activities(self, status: str = REGISTERED) -> list[ActivityType]:
        """Lists the current domain's activity types

        :param      status: Specifies the registration status of the
                            workflow types to list. Valid values are:
                            *  simpleflow.swf.mapper.constants.REGISTERED
                            *  simpleflow.swf.mapper.constants.DEPRECATED
        :type       status: string
        """
        from simpleflow.swf.mapper.querysets.activity import ActivityTypeQuerySet

        qs = ActivityTypeQuerySet(self)
        return qs.all(registration_status=status)

    @property
    def executions(self):
        return []

    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.name} status={self.status}>"
