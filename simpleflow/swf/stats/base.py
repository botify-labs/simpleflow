from itertools import chain

from future.utils import iteritems


def get_start_to_close_timing(event):
    last_state = event['state']
    scheduled = event.get('scheduled_timestamp')
    start = event.get('started_timestamp')
    if start is None:
        end = None
        duration = None
    else:
        end = event['{}_timestamp'.format(last_state)]
        duration = (end - start).total_seconds()

    return last_state, scheduled, start, end, duration


class WorkflowStats(object):
    def __init__(self, history):
        self._history = history

    def total_time(self):
        """
        Returns the total time of the workflow execution in seconds.

        :returns:
            :rtype: ``float``.

        """
        history = self._history
        start = history.events[0].timestamp
        end = history.events[-1].timestamp
        return (end - start).total_seconds()

    def get_timings(self):
        """
        Returns the time in seconds spent in the execution of a task, i.e.
        between the start and the close events, by task ID.

        :returns:
            :rtype: ``[(str, str, datetime.datetime, datetime.datetime, datetime.datetime, float)]``.

        Example: ::

            [(activity-module.func-1', 'completed', scheduled, start, end, 37.22),
             ('activity-module.otherfunc-1', 'completed', scheduled, start, end, 13.37)]

        """
        history = self._history
        history.parse()

        events = chain(
            iteritems(history._activities),
            iteritems(history._child_workflows),
        )
        return [
            (name,) + get_start_to_close_timing(attributes) for
            name, attributes in events
        ]

    def get_timings_with_percentage(self):
        """
        Returns the time in seconds and its percentage against the total time
        spent in the execution of a task, i.e. between the start and the close
        events, by task ID.

        :returns:
            :rtype: ``[(str, str, datetime.datetime, datetime.datetime, datetime.datetime, float, float)]``.

        Example: ::

            [(activity-module.func-1', 'completed', scheduled, start, end, 37.22, 12.4),
             ('activity-module.otherfunc-1', 'completed', scheduled, start, end, 13.37, 3.8)]

        """
        timing = -1
        total_time = self.total_time()

        return [
            (vals + ((vals[timing] / total_time) * 100.,) if vals[timing] else None)
            for vals in self.get_timings()
        ]
