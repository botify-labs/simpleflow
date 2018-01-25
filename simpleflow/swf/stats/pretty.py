import operator
from datetime import datetime
from functools import partial, wraps
from itertools import chain

from future.utils import iteritems

from simpleflow import compat
from simpleflow.history import History
from simpleflow.utils import json_dumps
from tabulate import tabulate

from . import WorkflowStats

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
    return tabulate(
        values,
        headers=headers,
        tablefmt=tablefmt,
        floatfmt=floatfmt,
    )


def csv(values, headers, delimiter=','):
    import csv
    from io import BytesIO

    data = BytesIO()

    csv.writer(data, delimiter=delimiter).writerows(values)

    return data.getvalue()


def human(values, headers):
    return tabulate(
        [(str(k), str(v)) for k, v in zip(headers, values[0])],
        tablefmt='plain',
    )


def jsonify(values, headers):
    if headers:
        return json_dumps(
            [dict(zip(headers, value)) for value in values]
        )
    else:
        return json_dumps(values)


DEFAULT_FORMAT = partial(tabular, tablefmt='plain', floatfmt='.2f')
FORMATS = {
    'csv': csv,
    'tsv': partial(csv, delimiter='\t'),
    'tabular': DEFAULT_FORMAT,
    'human': human,
    'json': jsonify,
}


def get_timestamps(task):
    last_state = task['state']
    timestamp = task[last_state + '_timestamp']
    scheduled_timestamp = task.get('scheduled_timestamp', '')

    return last_state, timestamp, scheduled_timestamp


def info(workflow_execution):
    history = History(workflow_execution.history())
    history.parse()

    if history.tasks:
        first_event = history.tasks[0]
        first_timestamp = first_event[first_event['state'] + '_timestamp']
        last_event = history.tasks[-1]
        last_timestamp = last_event.get('timestamp') or last_event[last_event['state'] + '_timestamp']
        workflow_input = first_event['input']
    else:
        first_event = history.events[0]
        first_timestamp = first_event.timestamp
        last_event = history.events[-1]
        last_timestamp = last_event.timestamp
        workflow_input = first_event.input

    execution_time = (last_timestamp - first_timestamp).total_seconds()

    header = (
        'domain',
        'workflow_type.name',
        'workflow_type.version',
        'task_list',
        'workflow_id',
        'run_id',
        'tag_list',
        'execution_time',
        'input',
    )
    ex = workflow_execution
    rows = [(
        ex.domain.name,
        ex.workflow_type.name,
        ex.workflow_type.version,
        ex.task_list,
        ex.workflow_id,
        ex.run_id,
        ','.join(ex.tag_list),
        execution_time,
        workflow_input,
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
        history.tasks[::-1]
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
                headers=header if (with_header or fmt == human) else [],
            )

        wrapped.__wrapped__ = wrapped
        return wrapped

    if isinstance(fmt, compat.basestring):
        fmt = FORMATS[fmt]

    return formatter


def list_executions(workflow_executions):
    header = 'Workflow ID', 'Workflow Type', 'Status'
    rows = ((
                execution.workflow_id,
                execution.workflow_type.name,
                execution.status,
            ) for execution in workflow_executions)

    return header, rows


def list_details(workflow_executions):
    header = (
        'Workflow ID', 'Workflow Type', 'Workflow Version', 'Run ID', 'Status', 'Task List', 'Child Policy',
        'Close Status', 'Execution Timeout', 'Input', 'Tags', 'Decision Tasks Timeout'
    )
    rows = ((
                execution.workflow_id,
                execution.workflow_type.name,
                execution.workflow_type.version,
                execution.run_id,
                execution.status,
                execution.task_list,
                execution.child_policy,
                execution.close_status,
                execution.execution_timeout,
                execution.input,
                execution.tag_list,
                execution.decision_tasks_timeout,

            ) for execution in workflow_executions)

    return header, rows


def get_task(workflow_execution, task_id, details=False):
    history = History(workflow_execution.history())
    history.parse()
    task = history.activities[task_id]
    header = ['type', 'id', 'name', 'version', 'state', 'timestamp', 'input', 'result', 'reason']
    # TODO...
    if details:
        header.append('details')
    # print >>sys.stderr, task
    state = task['state']
    rows = \
        [
            [
                task['type'],
                task['id'],
                task['name'],
                task['version'],
                state,
                task[state + '_timestamp'],
                task['input'],
                task.get('result'),  # Absent for failed tasks
                task.get('reason'),
            ]]
    if details:
        rows[0].append(task.get('details'))
    return header, rows


def dump_history_to_json(history):
    history.parse()
    events = list(chain(
        iteritems(history.activities),
        iteritems(history.child_workflows),
    ))
    return jsonify(events, headers=None)
