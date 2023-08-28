from __future__ import annotations

from typing import TYPE_CHECKING

from simpleflow.swf.mapper.models.history.base import History
from simpleflow.swf.mapper.querysets.base import BaseQuerySet

if TYPE_CHECKING:
    from simpleflow.swf.mapper.models.domain import Domain


class HistoryQuerySet(BaseQuerySet):
    """WorkflowExecution history queryset"""

    def __init__(self, domain: Domain, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.domain = domain

    def get(
        self,
        run_id: str,
        workflow_id: str,
        max_results: int | None = None,
        page_size: int | None = 100,
        reverse: bool = False,
    ) -> History:
        """Retrieves a WorkflowExecution history

        :param  run_id: unique identifier of the workflow execution
        :param  workflow_id: The user defined identifier associated with the workflow execution
        :param  max_results: Max output history size. Retrieved history will be shrinked
                             if it's size is greater than max_results.
        :param  page_size: Swf api response page size: controls how many history events
                           will be returned at each requests. Keep in mind that until
                           max_results history size is reached, next pages will be
                           requested.
        :param  reverse: Should the history events be retrieved in reverse order.
        """
        max_results = max_results or page_size

        if max_results < page_size:
            page_size = max_results

        response = self.get_workflow_execution_history(
            self.domain.name,
            run_id,
            workflow_id,
            maximum_page_size=page_size,
            reverse_order=reverse,
        )
        events = response["events"]

        next_page = response.get("nextPageToken")
        while next_page is not None and len(events) < max_results:
            response = self.get_workflow_execution_history(
                self.domain.name,
                run_id,
                workflow_id,
                maximum_page_size=page_size,
                next_page_token=next_page,
                reverse_order=reverse,
            )
            events.extend(response["events"])
            next_page = response.get("nextPageToken")

        return History.from_event_list(events)
