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

Start to close timings
----------------------
{start_to_close_timings}

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


def show(workflow_execution):
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
    start_to_close_contents = tabulate(
        sorted(
            start_to_close_values,
            key=operator.itemgetter(5),
            reverse=True,
        ),
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
        start_to_close_timings=start_to_close_contents,
    )

    return contents


def chart(workflow_execution):
    """
    Charts like Developer Tools one http://stackoverflow.com/a/21323364
    """
    pass
