from __future__ import annotations

from typing import TYPE_CHECKING

from swf.core import ConnectedSWFObject
from swf.models import Domain

if TYPE_CHECKING:
    from boto.exception import SWFResponseError


class Actor(ConnectedSWFObject):
    """SWF Actor base class.

    Actor is running through a thread in order for its polling
    operations not to be blocking. Many actors might be run in
    the same process.

    Usage example: implementing an activity worker or a decider
    using an actor is the typical usage.

    :ivar  domain: Domain the Actor should interact with
    :ivar  task_list: task list the Actor should watch for tasks on
    """

    def __init__(self, domain: Domain, task_list: str) -> None:
        super().__init__()

        self._set_domain(domain)
        self.task_list = task_list

    def _set_domain(self, domain: Domain):
        if not isinstance(domain, Domain):
            raise TypeError("domain arg must be a swf.models.Domain instance")
        self.domain = domain

    def start(self):
        """Launch the actor.

        A class overriding the Actor class should set this
        method to update the Actor status to Actor.STATES.RUNNING
        """
        raise NotImplementedError

    def stop(self):
        """Stop the actor.

        Set actor's status to Actor.STATES.STOPPED, and
        wait for the last polling operation to end before
        shutting down.
        """
        raise NotImplementedError

    def get_error_message(self, e: SWFResponseError) -> str:
        message = e.error_message
        if not message:
            if e.body:
                # Expected 'message', got 'Message' ¯\_(ツ)_/¯
                message = e.body.get("Message")
        return message
