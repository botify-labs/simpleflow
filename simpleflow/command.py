# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json
import logging
import multiprocessing
import os
import signal
import sys
import time
from uuid import uuid4

import click

import swf.models
import swf.querysets

from simpleflow.swf.stats import pretty
from simpleflow.swf import helpers
from simpleflow.swf.process import decider
from simpleflow.swf.process import worker
from simpleflow import __version__


logger = logging.getLogger(__name__)


def get_workflow(clspath):
    modname, clsname = clspath.rsplit('.', 1)
    module = __import__(modname, fromlist=['*'])
    cls = getattr(module, clsname)
    return cls


@click.group()
@click.option('--format')
@click.option('--header/--no-header', default=False)
@click.version_option(version=__version__)
@click.pass_context
def cli(ctx, header, format):
    ctx.params['format'] = format
    ctx.params['header'] = header


def get_workflow_type(domain_name, workflow):
    domain = swf.models.Domain(domain_name)
    query = swf.querysets.WorkflowTypeQuerySet(domain)
    return query.get_or_create(workflow.name, workflow.version)


def load_input(input_fp):
    if input_fp is None:
        input_fp = sys.stdin
    input = json.load(input_fp)
    return transform_input(input)


def get_input(wf_input):
    if not wf_input:
        wf_input = sys.stdin.read()
    wf_input = json.loads(wf_input)
    return transform_input(wf_input)


def transform_input(wf_input):
    if isinstance(wf_input, list):
        wf_input = {
            'args': wf_input,
            'kwargs': {},
        }
    return wf_input


@click.option('--local', default=False, is_flag=True,
              required=False,
              help='Run the workflow locally without calling Amazon SWF.')
@click.option('--input', '-i',
              required=False,
              help='JSON input of the workflow.')
@click.option('--input-file',
              required=False, type=click.File(),
              help='JSON file with the input of the workflow.')
@click.option('--tags',
              required=False,
              help='Tags for the workflow execution.')
@click.option('--decision-tasks-timeout',
              required=False,
              help='Timeout for the decision tasks.')
@click.option('--execution-timeout',
              required=False,
              help='Timeout for the whole workflow execution.')
@click.option('--task-list',
              required=False,
              help='Task list for decision tasks.')
@click.option('--workflow-id',
              required=False,
              help='ID of the workflow execution.')
@click.option('--domain',
              required=False,
              help='Amazon SWF Domain.')
@click.argument('workflow')
@cli.command('workflow.start', help='Start the workflow defined in the WORKFLOW module.')
def start_workflow(workflow,
                   domain,
                   workflow_id,
                   task_list,
                   execution_timeout,
                   tags,
                   decision_tasks_timeout,
                   input,
                   input_file,
                   local):
    workflow_definition = get_workflow(workflow)

    if input_file:
        wf_input = load_input(input_file)
    else:
        wf_input = get_input(input)
    if local:
        from .local import Executor

        Executor(workflow_definition).run(wf_input)

        return

    if not domain:
        raise ValueError('*domain* must be set when not running in local mode')

    workflow_type = get_workflow_type(domain, workflow_definition)
    execution = workflow_type.start_execution(
        workflow_id=workflow_id,
        task_list=task_list,
        execution_timeout=execution_timeout,
        input=wf_input,
        tag_list=tags,
        decision_tasks_timeout=decision_tasks_timeout,
    )
    print '{workflow_id} {run_id}'.format(
        workflow_id=execution.workflow_id,
        run_id=execution.run_id,
    )
    return execution


@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command(
    'workflow.terminate',
    help='Workflow associated with WORKFLOW and optionally RUN_ID.')
def terminate_workflow(domain, workflow_id, run_id):
    ex = helpers.get_workflow_execution(domain, workflow_id, run_id)
    ex.terminate()


@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command(
    'workflow.restart',
    help='Workflow associated with WORKFLOW_ID and optionally RUN_ID.')
def restart_workflow(domain, workflow_id, run_id):
    ex = helpers.get_workflow_execution(domain, workflow_id, run_id)
    history = ex.history()
    ex.terminate()
    new_ex = ex.workflow_type.start_execution(
        ex.workflow_id,
        task_list=ex.task_list,
        execution_timeout=ex.execution_timeout,
        input=history.events[0].input,
        tag_list=ex.tag_list,
        decision_tasks_timeout=ex.decision_tasks_timeout,
    )
    print '{workflow_id} {run_id}'.format(
        workflow_id=new_ex.workflow_id,
        run_id=new_ex.run_id,
    )


def with_format(ctx):
    return pretty.formatted(
        with_header=ctx.parent.params['header'],
        fmt=ctx.parent.params['format'] or pretty.DEFAULT_FORMAT,
    )


@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command('workflow.info', help='Info about a workflow execution.')
@click.pass_context
def info(ctx, domain, workflow_id, run_id):
    print(with_format(ctx)(helpers.show_workflow_info)(
        domain,
        workflow_id,
        run_id,
    ))


@click.option('--nb-tasks', '-n', default=None, type=int,
              help='Maximum number of tasks to display.')
@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command('workflow.profile', help='Profile of a workflow.')
@click.pass_context
def profile(ctx, domain, workflow_id, run_id, nb_tasks):
    print(with_format(ctx)(helpers.show_workflow_profile)(
        domain,
        workflow_id,
        run_id,
        nb_tasks,
    ))


@click.option('--nb-tasks', '-n', default=None, type=int,
              help='Maximum number of tasks to display.')
@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command('workflow.tasks', help='Tasks of a workflow execution.')
@click.pass_context
def status(ctx, domain, workflow_id, run_id, nb_tasks):
    print(with_format(ctx)(helpers.show_workflow_status)(
        domain,
        workflow_id,
        run_id,
        nb_tasks,
    ))


@click.argument('domain')
@cli.command('workflow.list', help='Active workflow executions.')
@click.option('--status', '-s', default='open', show_default=True, type=click.Choice(['open', 'closed']),
              help='Open/Closed')
@click.option('--started-since', '-d', default=30, show_default=True, help='Started since N days.')
@click.pass_context
def list_workflows(ctx, domain, status, started_since):
    print(with_format(ctx)(helpers.list_workflow_executions)(domain, status=status.upper(),
                                                             start_oldest_date=started_since))


@click.argument('domain')
@cli.command('workflow.filter', help='Filter workflow executions.')
@click.option('--status', '-s', default='open', show_default=True, type=click.Choice(['open', 'closed']),
              help='Open/Closed')
@click.option('--tag', default=None, help='Tag (multiple option).'  # , multiple=True
              )
@click.option('--workflow-id', default=None, help='Workflow ID.')
@click.option('--workflow-type-name', default=None, help='Workflow Name.')
@click.option('--workflow-type-version', default=None, help='Workflow Version (name needed).')
@click.option('--started-since', '-d', default=30, show_default=True, help='Started since N days.')
@click.pass_context
def filter_workflows(ctx, domain, status, tag,
                     workflow_id, workflow_type_name,
                     workflow_type_version, started_since):
    status = status.upper()
    kwargs = {}
    if status == swf.models.workflow.WorkflowExecution.STATUS_OPEN:
        kwargs['oldest_date'] = started_since
    else:
        kwargs['start_oldest_date'] = started_since
    print(with_format(ctx)(helpers.filter_workflow_executions)(domain, status=status.upper(),
                                                               tag=tag,
                                                               workflow_id=workflow_id,
                                                               workflow_type_name=workflow_type_name,
                                                               workflow_type_version=workflow_type_version,
                                                               **kwargs
                                                               ))


@click.argument('task_id')
@click.argument('workflow_id')
@click.argument('domain')
@click.option('--details/--no-details',
              default=False,
              help='Display details.'
              )
@cli.command('task.info', help='Informations on a task.')
@click.pass_context
def task_info(ctx, domain, workflow_id, task_id, details):
    print(with_format(ctx)(helpers.get_task)(domain, workflow_id, task_id, details))


@click.option('--nb-processes', '-N', type=int)
@click.option('--log-level', '-l')
@click.option('--task-list')
@click.option('--domain', '-d', required=True, help='SWF Domain')
@click.argument('workflows', nargs=-1, required=True)
@cli.command('decider.start', help='Start a decider process to manage workflow executions.')
def start_decider(workflows, domain, task_list, log_level, nb_processes):
    if log_level:
        logger.warning(
            "Deprecated: --log-level will be removed, use LOG_LEVEL environment variable instead"
        )
    decider.command.start(
        workflows,
        domain,
        task_list,
        None,
        nb_processes,
    )


@click.option('--heartbeat',
              type=int,
              required=False,
              help='interval in seconds')
@click.option('--nb-processes', '-N', type=int)
@click.option('--log-level', '-l')
@click.option('--task-list')
@click.option('--domain', '-d', required=True, help='SWF Domain')
@click.argument('workflow')
@cli.command('worker.start', help='Start a worker process to handle activity tasks.')
def start_worker(workflow, domain, task_list, log_level, nb_processes, heartbeat):
    if log_level:
        logger.warning(
            "Deprecated: --log-level will be removed, use LOG_LEVEL environment variable instead"
        )
    worker.command.start(
        workflow,
        domain,
        task_list,
        nb_processes,
        heartbeat,
    )


def get_task_list(workflow_id=''):
    task_list_id = '-' + uuid4().hex
    overflow = 256 - len(task_list_id) - len(workflow_id)
    if overflow < 0:
        truncated = workflow_id[:overflow]
        task_list = truncated + task_list_id
    else:
        task_list = workflow_id + task_list_id
    return task_list


@click.option('--heartbeat',
              type=int,
              required=False,
              help='Heartbeat interval in seconds.')
@click.option('--nb-workers',
              type=int,
              required=False,
              help='Number of parallel processes handling activity tasks.')
@click.option('--input', '-i',
              required=False,
              help='JSON input of the workflow.')
@click.option('--input-file',
              required=False, type=click.File(),
              help='JSON file with the input of the workflow.')
@click.option('--tags',
              required=False,
              help='Tags identifying the workflow execution.')
@click.option('--decision-tasks-timeout',
              required=False,
              help='Decision tasks timeout.')
@click.option('--execution-timeout',
              required=False,
              help='Timeout for the whole workflow execution.')
@click.option('--workflow-id',
              required=False,
              help='ID of the workflow execution.')
@click.option('--domain', '-d',
              required=True,
              help='SWF Domain.')
@click.option('--display-status',
              type=bool, required=False,
              help='Display execution status.'
              )
@click.argument('workflow')
@cli.command('standalone', help='Execute a workflow with a single process.')
@click.pass_context
def standalone(context,
               workflow,
               domain,
               workflow_id,
               execution_timeout,
               tags,
               decision_tasks_timeout,
               input,
               input_file,
               nb_workers,
               heartbeat,
               display_status,
               ):
    """
    This command spawn a decider and an activity worker to execute a workflow
    with a single main process.

    """
    if not workflow_id:
        workflow_id = get_workflow(workflow).name

    task_list = get_task_list(workflow_id)
    logger.info('using task list {}'.format(task_list))
    decider_proc = multiprocessing.Process(
        target=decider.command.start,
        args=(
            [workflow],
            domain,
            task_list,
        )
    )
    decider_proc.start()

    worker_proc = multiprocessing.Process(
        target=worker.command.start,
        args=(
            workflow,
            domain,
            task_list,
            nb_workers,
            heartbeat,
        )
    )
    worker_proc.start()

    print >> sys.stderr, 'starting workflow {}'.format(workflow)
    ex = start_workflow.callback(
        workflow,
        domain,
        workflow_id,
        task_list,
        execution_timeout,
        tags,
        decision_tasks_timeout,
        input,
        input_file,
        local=False,
    )
    while True:
        time.sleep(2)
        ex = helpers.get_workflow_execution(
            domain,
            ex.workflow_id,
            ex.run_id,
        )
        if display_status:
            print >> sys.stderr, 'status: {}'.format(ex.status)
        if ex.status == ex.STATUS_CLOSED:
            print >> sys.stderr, 'execution {} finished'.format(ex.workflow_id)
            break

    os.kill(worker_proc.pid, signal.SIGTERM)
    worker_proc.join()
    os.kill(decider_proc.pid, signal.SIGTERM)
    decider_proc.join()
