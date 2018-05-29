#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import subprocess
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
from io import open

REQUIRES = [

]
PUBLISH_CMD = "python setup.py sdist bdist_wheel upload"
TEST_PUBLISH_CMD = 'python setup.py sdist bdist_wheel upload -r test'

PY2 = int(sys.version[0]) == 2


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


def find_version(fname):
    '''Attempts to find the version number in the file names fname.
    Raises RuntimeError if not found.
    '''
    version = ''
    with open(fname, 'r') as fp:
        reg = re.compile(r'__version__ = [\'"]([^\'"]*)[\'"]')
        for line in fp:
            m = reg.match(line)
            if m:
                version = m.group(1)
                break
    if not version:
        raise RuntimeError('Cannot find version information')
    return version


__version__ = find_version("simpleflow/__init__.py")

if 'publish' in sys.argv:
    try:
        __import__('wheel')
    except ImportError:
        print("wheel required. Run `pip install wheel`.")
        sys.exit(1)
    status = subprocess.call(PUBLISH_CMD, shell=True)
    sys.exit(status)

if 'publish_test' in sys.argv:
    try:
        __import__('wheel')
    except ImportError:
        print("wheel required. Run `pip install wheel`.")
        sys.exit(1)
    status = subprocess.call(TEST_PUBLISH_CMD, shell=True)
    sys.exit()


def read(fname):
    with open(fname, encoding='utf8') as fp:
        content = fp.read()
    return content


DEPS = [
    'future',
    'boto>=2.38.0',
    'diskcache==2.4.1',
    'Jinja2>=2.8',
    'kubernetes==3.0.0',
    'lazy_object_proxy',
    'lockfile>=0.9.1',
    'tabulate>=0.7.3,<0.8.0',
    'setproctitle',
    'click',
    'psutil>=3.2.1',
    'pytz',
    'typing',
    'PyYAML',
]
if PY2:
    DEPS += [
        'enum34',
        'subprocess32',
    ]

setup(
    name='simpleflow',
    version=__version__,
    description='Python library for dataflow programming with Amazon SWF',
    long_description=(read("README.md") + '\n\n' +
                      read("CHANGELOG.md")),
    long_description_content_type='text/markdown',
    author='Greg Leclercq',
    author_email='tech@botify.com',
    url='https://github.com/botify-labs/simpleflow',
    packages=find_packages(exclude=("test*",)),
    package_dir={
        'simpleflow': 'simpleflow',
        'swf': 'swf',
    },
    include_package_data=True,
    install_requires=DEPS,
    license=read("LICENSE"),
    zip_safe=False,
    keywords='simpleflow amazon swf simple workflow',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=[
        'pytest',
        'moto==0.4.31',
    ],
    cmdclass={'test': PyTest},
    entry_points={
        'console_scripts': [
            'simpleflow = simpleflow.command:cli',
        ]
    }
)
