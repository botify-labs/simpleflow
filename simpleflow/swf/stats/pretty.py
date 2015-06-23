import operator
from datetime import datetime

from tabulate import tabulate

from . import WorkflowStats
from simpleflow.history import History


TEMPLATE = '''
Workflow Execution {workflow_id}
Domain: {workflow_type.domain.name}
Workflow Type: {workflow_type.name}

{tag_list}

Total time = {total_time} seconds

## {label}

{values}

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


def show(workflow_execution, nb_tasks=None):
    stats = WorkflowStats(History(workflow_execution.history()))

    start_to_close_values = (
        (task,
         last_state,
         scheduled.strftime(TIME_FORMAT) if scheduled else None,
         (start - scheduled).total_seconds() if scheduled else None,
         start.strftime(TIME_FORMAT) if start else None,
         (end - start).total_seconds() if start else None,
         end.strftime(TIME_FORMAT) if end else None,
         percent) for task, last_state, scheduled, start, end, timing, percent in
        stats.get_timings_with_percentage()
    )
    start_to_close_values = sorted(
        start_to_close_values,
        key=operator.itemgetter(5),
        reverse=True,
    )
    if nb_tasks:
        start_to_close_values = start_to_close_values[:nb_tasks]

    start_to_close_contents = tabulate(
        start_to_close_values,
        headers=(
            'Task',
            'Last State',
            'Scheduled',
            ' -> ',
            'Start',
            ' -> ',
            'End',
            '%',
        ),
        tablefmt='pipe',  # Markdown-compatible.
    )

    contents = TEMPLATE.format(
        workflow_id=workflow_execution.workflow_id,
        workflow_type=workflow_execution.workflow_type,
        tag_list=_show_tag_list(workflow_execution.tag_list),
        total_time=stats.total_time(),
        label='Start to close timings',
        values=start_to_close_contents,
    )

    return contents


def get_timestamps(task):
    last_state = task['state']
    timestamp = task[last_state + '_timestamp']
    scheduled_timestamp = task.get('scheduled_timestamp', '')

    return last_state, timestamp, scheduled_timestamp


def status(workflow_execution, nb_tasks=None):
    history = History(workflow_execution.history())
    history.parse()

    values = [
        (task['name'],) + get_timestamps(task) for task in
        history._tasks[::-1]
    ]
    if nb_tasks:
        values = values[:nb_tasks]

    status_contents = tabulate(
        values,
        headers=(
            'Tasks',
            'Last State',
            'at',
            'Scheduled at',
        ),
        tablefmt='pipe',  # Markdown-compatible.
    )
    first_event = history._tasks[0]
    last_event = history._tasks[-1]
    total_time = (
        last_event[last_event['state'] + '_timestamp'] -
        first_event[first_event['state'] + '_timestamp']
    ).total_seconds()
    contents = TEMPLATE.format(
        workflow_id=workflow_execution.workflow_id,
        workflow_type=workflow_execution.workflow_type,
        tag_list=_show_tag_list(workflow_execution.tag_list),
        total_time=total_time,
        label='Tasks Status',
        values=status_contents,
    )

    return contents
