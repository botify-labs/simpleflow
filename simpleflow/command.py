# -*- coding: utf-8 -*-
from __future__ import absolute_import

import sys
import json

import click

import swf.models
import swf.querysets


__all__ = ['start', 'profile']


def get_workflow(clspath):
    modname, clsname = clspath.rsplit('.', 1)
    module = __import__(modname, fromlist=['*'])
    cls = getattr(module, clsname)
    return cls


@click.group()
def cli():
    pass


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
@cli.command(help='the workflow defined in the WORKFLOW module')
def start(workflow,
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
        input = json.load(open(input, 'rb'))

    if local:
        from .local import Executor

        Executor(workflow_definition).run(input)

        return

    if not domain:
        raise ValueError('*domain* must be set when not running in local mode')

    workflow_type = get_workflow_type(domain, workflow_definition)
    workflow_type.start_execution(
        workflow_id=workflow_id,
        task_list=task_list,
        execution_timeout=execution_timeout,
        input=input,
        tag_list=tags,
        decision_tasks_timeout=decision_tasks_timeout,
    )


@click.option('--nb-tasks', '-n', default=None, type=click.INT,
              help='Maximum number of tasks to display')
@click.argument('run_id', required=False)
@click.argument('workflow_id')
@click.argument('domain')
@cli.command('profile', help='the tasks of a workflow')
def profile(domain, workflow_id, run_id, nb_tasks):
    from simpleflow.swf import stats

    print(stats.helpers.show_workflow_stats(
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
@cli.command('status', help='show the status of a workflow execution')
def status(domain, workflow_id, run_id, nb_tasks):
    from simpleflow.swf import stats

    print(stats.helpers.show_workflow_status(
        domain,
        workflow_id,
        run_id,
        nb_tasks,
    ))
