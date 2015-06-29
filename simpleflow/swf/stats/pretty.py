import sys
import operator
from functools import partial, wraps
from datetime import datetime

from . import WorkflowStats
from simpleflow.history import History


TEMPLATE = '''
Workflow Execution {workflow_id}
Domain: {workflow_type.domain.name}
Workflow Type: {workflow_type.name}

{tag_list}

Total time = {total_time} seconds
'''


TIME_FORMAT = '%Y-%m-%d %H:%M'


def _show_tag_list(tag_list):
    return '\n'.join(
        '{}:\t{}'.format(key.strip(), value.strip()) for key, value in (
            keyval.split('=') for keyval in tag_list
        )
    )


def _to_timestamp(date):
    return (date - datetime(1970, 1, 1)).total_seconds()


def tabular(values, headers, tablefmt, floatfmt):
    from tabulate import tabulate

    return tabulate(
        values,
        headers=headers,
        tablefmt=tablefmt,
        floatfmt=floatfmt,
    )


def csv(values, headers, delimiter=','):
    import csv

    return csv.writer(sys.stdout, delimiter=delimiter).writerows(values)


DEFAULT_FORMAT = partial(tabular, tablefmt='plain', floatfmt='.2f')
FORMATS = {
    'csv': csv,
    'tsv': partial(csv, delimiter='\t'),
    'tabular': DEFAULT_FORMAT,
}


def get_timestamps(task):
    last_state = task['state']
    timestamp = task[last_state + '_timestamp']
    scheduled_timestamp = task.get('scheduled_timestamp', '')

    return last_state, timestamp, scheduled_timestamp


def info(workflow_execution):
    history = History(workflow_execution.history())
    history.parse()

    first_event = history._tasks[0]
    last_event = history._tasks[-1]
    execution_time = (
        last_event[last_event['state'] + '_timestamp'] -
        first_event[first_event['state'] + '_timestamp']
    ).total_seconds()

    header = (
        'domain',
        'workflow_type.name',
        'workflow_type.version',
        'workflow_id',
        'tag_list',
        'execution_time',
    )
    ex = workflow_execution
    rows = [(
        ex.domain.name,
        ex.workflow_type.name,
        ex.workflow_type.version,
        ex.workflow_id,
        ','.join(ex.tag_list),
        execution_time,
    )]
    return header, rows


def profile(workflow_execution, nb_tasks=None):
    stats = WorkflowStats(History(workflow_execution.history()))

    header = (
        'Task',
        'Last State',
        'Scheduled',
        'Time Scheduled',
        'Start',
        'Time Running',
        'End',
        'Percentage of total time',
    )

    values = (
        (task,
         last_state,
         scheduled.strftime(TIME_FORMAT) if scheduled else None,
         (start - scheduled).total_seconds() if scheduled else None,
         start.strftime(TIME_FORMAT) if start else None,
         (end - start).total_seconds() if start else None,
         end.strftime(TIME_FORMAT) if end else None,
         percent) for task, last_state, scheduled, start, end, timing, percent in
        (row for row in stats.get_timings_with_percentage() if row is not None)
    )
    rows = sorted(
        values,
        key=operator.itemgetter(5),
        reverse=True,
    )

    if nb_tasks:
        rows = rows[:nb_tasks]

    return header, rows


def status(workflow_execution, nb_tasks=None):
    history = History(workflow_execution.history())
    history.parse()

    header = 'Tasks', 'Last State', 'Last State Time', 'Scheduled Time'
    rows = [
        (task['name'],) + get_timestamps(task) for task in
        history._tasks[::-1]
    ]
    if nb_tasks:
        rows = rows[:nb_tasks]

    return header, rows


def formatted(with_info=False, with_header=False, fmt=DEFAULT_FORMAT):
    def formatter(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            header, rows = func(*args, **kwargs)
            return fmt(
                rows,
                headers=header if with_header else [],
            )
        wrapped.__wrapped__ = wrapped
        return wrapped

    if isinstance(fmt, basestring):
        fmt = FORMATS[fmt]

    return formatter


def list(workflow_executions):
    header = 'Workflow ID', 'Workflow Type', 'Status'
    rows = ((
        execution.workflow_id,
        execution.workflow_type.name,
        execution.status,
    ) for execution in workflow_executions)

    return header, rows
