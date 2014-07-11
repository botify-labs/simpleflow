# -*- coding: utf-8 -*-

import sys
import argparse
import json


def get_argument_parser():
    parser = argparse.ArgumentParser(
        description='Command line tools to use simpleflow')

    parser.add_argument(
        '--local',
        action='store_true',
        default=False,
        help='Run the workflow locally without calling Amazon SWF'
    )

    parser.add_argument(
        '-w', '--workflow',
        action='store',
        required=True,
        help='Module that contains the workflow to perform'
    )

    parser.add_argument(
        '-i', '--input',
        action='store',
        help='Path to a JSON file that contains the input of the workflow'
    )

    return parser


def get_workflow(clspath):
    modname, clsname = clspath.rsplit('.', 1)
    module = __import__(modname, fromlist=['*'])
    cls = getattr(module, clsname)
    return cls


def main():
    arguments = get_argument_parser().parse_args(sys.argv[1:])
    workflow = get_workflow(arguments.workflow)
    if not arguments.input:
        input = json.loads(sys.stdin.read())
    else:
        input = json.load(open(arguments.input, 'rb'))

    if arguments.local:
        from . import local

        local.Executor(workflow).run(input)
