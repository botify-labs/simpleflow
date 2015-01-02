##!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup
import botify.common


NAME = 'botify-common'
VERSION = botify.common.__version__
AUTHOR = 'Botify'


setup(
    name=NAME,
    version=VERSION,
    description='Botify common utilities',
    author=AUTHOR,
    author_email='tech@botify.com',
    packages=[
        'botify',
        'botify.common'
    ],
    namespace_packages=[
        'botify',
        'botify.common'
    ],
)
