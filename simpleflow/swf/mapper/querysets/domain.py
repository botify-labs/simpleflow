from __future__ import annotations

from botocore.exceptions import ClientError

from simpleflow.swf.mapper.constants import REGISTERED
from simpleflow.swf.mapper.exceptions import DoesNotExistError, ResponseError
from simpleflow.swf.mapper.models.domain import Domain
from simpleflow.swf.mapper.querysets.base import BaseQuerySet


class DomainQuerySet(BaseQuerySet):
    """Swf domain queryset object

    Allows the user to interact with amazon's swf domains
    through a django-queryset like interface
    """

    def get(self, name: str, *args, **kwargs) -> Domain:
        """Fetches the Domain with `name`

        :param      name:  name of the domain to fetch

        A typical Amazon response looks like:

        .. code-block:: json

            {
                "configuration": {
                    "workflowExecutionRetentionPeriodInDays": "7",
                },
                "domainInfo": {
                    "status": "REGISTERED",
                    "name": "CrawlTest",
                }
            }
        """
        try:
            response = self.describe_domain(name)
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "UnknownResourceFault":
                raise DoesNotExistError(f"No such domain: {name}") from e
            # Any other errors should raise
            raise ResponseError(e.args[0], error_code=error_code) from e

        domain_info = response["domainInfo"]
        domain_config = response["configuration"]

        return Domain(
            domain_info["name"],
            status=domain_info["status"],
            retention_period=domain_config["workflowExecutionRetentionPeriodInDays"],
        )

    def get_or_create(
        self,
        name: str,
        status: str = REGISTERED,
        description: str | None = None,
        retention_period: int = 30,
        *args,
        **kwargs,
    ) -> Domain:
        """Fetches, or creates the Domain with `name`

        When fetching trying to fetch a matching domain, only
        name parameter is taken in account. Anyway, If you'd wanna
        make sure that in case the domain has to be created it is
        made with specific values, just provide it.

        :param      name:  name of the domain to fetch or create
        :param      retention_period: Domain's workflow executions records retention in days
        :param      status: Specifies the registration status of the
                            workflow types to list. Valid values are:
                            * `` simpleflow.swf.mapper.constants.REGISTERED``
                            * `` simpleflow.swf.mapper.constants.DEPRECATED``
        :param      description: Textual description of the domain

        :returns: Fetched or created Domain model object
        """
        try:
            return self.get(name)
        except DoesNotExistError:
            return self.create(name, status, description, retention_period)

    def all(self, registration_status: str = REGISTERED, *args, **kwargs) -> list[Domain]:
        """Retrieves all domains

        :param      registration_status: domain registration status to match,
                                         Valid values are:
                                         * `` simpleflow.swf.mapper.constants.REGISTERED``
                                         * `` simpleflow.swf.mapper.constants.DEPRECATED``

        A typical Amazon response looks like:

        .. code-block:: json

            {
                "domainInfos": [
                    {
                        "name": "Crawl"
                        "status": "REGISTERED",
                        "description": "",
                    },
                ]
            }
        """

        def get_domains():
            response = {"nextPageToken": None}
            while "nextPageToken" in response:
                response = self.list_domains(registration_status, next_page_token=response["nextPageToken"])

                yield from response["domainInfos"]

        return [Domain(d["name"], d["status"], d.get("description")) for d in get_domains()]

    def create(
        self,
        name: str,
        status: str = REGISTERED,
        description: str | None = None,
        retention_period: int = 30,
        *args,
        **kwargs,
    ) -> Domain:
        """Creates a new remote domain and returns the Domain model instance

        :param      name: Name of the domain to register (unique)
        :param      retention_period: Domain's workflow executions records retention in days
        :param      status: Specifies the registration status of the
                            workflow types to list. Valid values are:
                            * `` simpleflow.swf.mapper.constants.REGISTERED``
                            * `` simpleflow.swf.mapper.constants.DEPRECATED``
        :param      description: Textual description of the domain
        """

        domain = Domain(
            name,
            status=status,
            description=description,
            retention_period=retention_period,
        )
        domain.save()

        return domain
