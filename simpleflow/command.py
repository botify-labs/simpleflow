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


__all__ = ['start', 'info', 'profile', 'status', 'list']


logger = logging.getLogger(__name__)


def get_workflow(clspath):
    modname, clsname = clspath.rsplit('.', 1)
    module = __import__(modname, fromlist=['*'])
    cls = getattr(module, clsname)
    return cls


@click.group()
@click.option('--format')
@click.option('--header/--no-header', default=False)
@click.pass_context
def cli(ctx, header,  format):
    ctx.params['format'] = format
    ctx.params['header'] = header


def get_workflow_type(domain_name, workflow):
    domain = swf.models.Domain(domain_name)
    query = swf.querysets.WorkflowTypeQuerySet(domain)
    return query.get_or_create(workflow.name, workflow.version)


@click.option('--local', default=False, is_flag=True,
              required=False,
              help='Run the workflow locally without calling Amazon SWF')
@click.option('--input', '-i',
              required=False,
              help='Path to a JSON file that contains the input of the workflow')
@click.option('--tags',
              required=False,
              help='that identifies the workflow execution')
@click.option('--decision-tasks-timeout',
              required=False,)
@click.option('--execution-timeout',
              required=False,
              help='for the whole workflow execution')
@click.option('--task-list',
              required=False,
              help='for decision tasks')
@click.option('--workflow-id',
              required=False,
              help='of the workflow execution')
@click.option('--domain',
              required=False,
              help='Amazon SWF Domain')
@click.argument('workflow')
@cli.command('workflow.start', help='the workflow defined in the WORKFLOW module')
def start_workflow(workflow,
          domain,
          workflow_id,
          task_list,
          execution_timeout,
          tags,
          decision_tasks_timeout,
          input,
          local):
    workflow_definition = get_workflow(workflow)
    if not input:
        input = json.loads(sys.stdin.read())
    else:
        input = json.loads(input)

    if local:
        from .local import Executor

        Executor(workflow_definition).run(input)

        return

    if not domain:
        raise ValueError('*domain* must be set when not running in local mode')

    workflow_type = get_workflow_type(domain, workflow_definition)
    execution = workflow_type.start_execution(
        workflow_id=workflow_id,
        task_list=task_list,
        execution_timeout=execution_timeout,
        input=input,
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
    help='the workflow associated with WORKFLOW and optionally RUN_ID')
def terminate_workflow(domain, workflow_id, run_id):
    ex = helpers.get_workflow_execution(domain, workflow_id, run_id)
    ex.terminate()


@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command(
    'workflow.restart',
    help='the workflow associated with WORKFLOW_ID and optionally RUN_ID')
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
@cli.command('workflow.info', help='about a workflow execution')
@click.pass_context
def info(ctx, domain, workflow_id, run_id):
    print(with_format(ctx)(helpers.show_workflow_info)(
        domain,
        workflow_id,
        run_id,
    ))


@click.option('--nb-tasks', '-n', default=None, type=click.INT,
              help='Maximum number of tasks to display')
@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command('workflow.profile', help='the tasks of a workflow')
@click.pass_context
def profile(ctx, domain, workflow_id, run_id, nb_tasks):
    print(with_format(ctx)(helpers.show_workflow_profile)(
        domain,
        workflow_id,
        run_id,
        nb_tasks,
    ))


@click.option('--nb-tasks', '-n', default=None, type=click.INT,
              help='Maximum number of tasks to display')
@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command('workflow.tasks', help='of a workflow execution')
@click.pass_context
def status(ctx, domain, workflow_id, run_id, nb_tasks):
    print(with_format(ctx)(helpers.show_workflow_status)(
        domain,
        workflow_id,
        run_id,
        nb_tasks,
    ))


@click.argument('domain')
@cli.command('workflow.list', help='active workflow executions')
@click.pass_context
def list_workflows(ctx, domain):
    print(with_format(ctx)(helpers.list_workflow_executions)(domain))


@click.argument('task_id')
@click.argument('workflow_id')
@click.argument('domain')
@cli.command('task.info')
@click.pass_context
def task_info(ctx, domain, workflow_id, task_id):
    print(with_format(ctx)(helpers.get_task)(domain, workflow_id, task_id))


@click.option('--nb-processes', '-N', type=int)
@click.option('--log-level', '-l')
@click.option('--task-list')
@click.option('--domain', '-d', required=True, help='SWF Domain')
@click.argument('workflows', nargs=-1, required=True)
@cli.command('decider.start', help='start a decider process to manage workflow executions')
def start_decider(workflows, domain, task_list, log_level, nb_processes):
    decider.command.start(
        workflows,
        domain,
        task_list,
        log_level,
        nb_processes,
    )


@click.option('--nb-processes', '-N', type=int)
@click.option('--log-level', '-l')
@click.option('--task-list')
@click.option('--domain', '-d', required=True, help='SWF Domain')
@click.argument('workflow')
@cli.command('worker.start', help='a worker process to handle activity tasks')
def start_worker(workflow, domain, task_list, log_level, nb_processes):
    worker.command.start(
        workflow,
        domain,
        task_list,
        nb_processes,
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


@click.option('--input', '-i',
              required=False,
              help='Path to a JSON file that contains the input of the workflow')
@click.option('--tags',
              required=False,
              help='that identifies the workflow execution')
@click.option('--decision-tasks-timeout',
              required=False,)
@click.option('--execution-timeout',
              required=False,
              help='for the whole workflow execution')
@click.option('--workflow-id',
              required=False,
              help='of the workflow execution')
@click.option('--domain', '-d', required=True, help='SWF Domain')
@click.argument('workflow')
@cli.command('standalone', help='execute a workflow with a single process')
@click.pass_context
def standalone(context,
        workflow,
        domain,
        workflow_id,
        execution_timeout,
        tags,
        decision_tasks_timeout,
        input):
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
        )
    )
    worker_proc.start()

    print >> sys.stderr, 'starting workflow {}'.format(workflow)
    ex = context.forward(start_workflow, local=False, task_list=task_list)
    while True:
        time.sleep(2)
        ex = helpers.get_workflow_execution(
            domain,
            ex.workflow_id,
            ex.run_id,
        )
        if ex.status == ex.STATUS_CLOSED:
            print >> sys.stderr, 'execution {} finished'.format(
                ex.workflow_id,
            )
            break

    os.kill(worker_proc.pid, signal.SIGTERM)
    worker_proc.join()
    os.kill(decider_proc.pid, signal.SIGTERM)
    decider_proc.join()
