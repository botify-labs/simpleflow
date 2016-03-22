from swf.models import History
from swf.querysets.base import BaseQuerySet


class HistoryQuerySet(BaseQuerySet):
    """WorkflowExecution history queryset"""
    def __init__(self, domain, *args, **kwargs):
        super(HistoryQuerySet, self).__init__(*args, **kwargs)
        self.domain = domain

    def get(self, run_id, workflow_id, max_results=None, page_size=100, reverse=False):
        """Retrieves a WorkflowExecution history

        :param  run_id: unique identifier of the workflow execution
        :type   run_id: string

        :param  workflow_id: The user defined identifier associated with the workflow execution
        :type   workflow_id: string

        :param  max_results: Max output history size. Retrieved history will be shrinked
                             if it's size is greater than max_results.
        :type   max_results: int

        :param  page_size: Swf api response page size: controls how many history events
                           will be returned at each requests. Keep in mind that until
                           max_results history size is reached, next pages will be
                           requested.
        :type   page_size: int

        :param  reverse: Should the history events be retrieved in reverse order.
        :type   reverse: bool
        """
        events = []
        max_results = max_results or page_size

        if max_results < page_size:
            page_size = max_results

        response = self.connection.get_workflow_execution_history(
            self.domain.name,
            run_id,
            workflow_id,
            maximum_page_size=page_size,
            reverse_order=reverse)
        events = response['events']

        next_page = response.get('nextPageToken')
        while next_page is not None and len(events) < max_results:
            response = self.connection.get_workflow_execution_history(
                self.domain.name,
                run_id,
                workflow_id,
                maximum_page_size=page_size,
                next_page_token=next_page,
                reverse_order=reverse
            )
            events.extend(response['events'])
            next_page = response.get('nextPageToken')

        return History.from_event_list(events)
