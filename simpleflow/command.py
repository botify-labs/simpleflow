# -*- coding: utf-8 -*-

import sys
import json

import click


__all__ = ['start']


def get_workflow(clspath):
    modname, clsname = clspath.rsplit('.', 1)
    module = __import__(modname, fromlist=['*'])
    cls = getattr(module, clsname)
    return cls


@click.group()
def cli():
    pass


@click.option('--local', default=False, is_flag=True,
              required=False,
              help='Run the workflow locally without calling Amazon SWF')
@click.option('--input', '-i',
              required=False,
              help='Path to a JSON file that contains the input of the workflow')
@click.argument('workflow')
@cli.command(help='the workflow defined in the WORKFLOW module')
def start(local, workflow, input):
    workflow_definition = get_workflow(workflow)
    if not input:
        input = json.loads(sys.stdin.read())
    else:
        input = json.load(open(input, 'rb'))

    if local:
        from .local import Executor

        Executor(workflow_definition).run(input)
