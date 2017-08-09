# -*- coding: utf-8 -*-
import os
from invoke import task, run

@task
def test():
    run('python setup.py test', pty=True)

@task
def clean():
    run("rm -rf build")
    run("rm -rf dist")
    run("rm -rf simpleflow.egg-info")
    print("Cleaned up.")
