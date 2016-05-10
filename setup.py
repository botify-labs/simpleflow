# -*- coding: utf-8 -*-
import platform
import re
import sys
import subprocess
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


REQUIRES = [

]
PUBLISH_CMD = "python setup.py register sdist bdist_wheel upload"
TEST_PUBLISH_CMD = 'python setup.py register -r test sdist bdist_wheel upload -r test'


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
    with open(fname) as fp:
        content = fp.read()
    return content

DEPS = [
    'boto>=2.38.0',
    'tabulate==0.7.3',
    'setproctitle',
    'subprocess32',
    'click',
    'psutil',
    'pytz',
]

setup(
    name='simpleflow',
    version=__version__,
    description='Python library for dataflow programming with Amazon SWF',
    long_description=(read("README.rst") + '\n\n' +
                      read("README_SWF.rst") + '\n\n' +
                      read("HISTORY.rst")),
    author='Greg Leclercq',
    author_email='greg@botify.com',
    url='https://github.com/botify-labs/simpleflow',
    packages=find_packages(exclude=("test*", )),
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
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ],
    test_suite='tests',
    tests_require=[
        'pytest',
        'moto>=0.4.19',
    ],
    cmdclass={'test': PyTest},
    entry_points={
        'console_scripts': [
            'simpleflow = simpleflow.command:cli',
        ]
    }
)
