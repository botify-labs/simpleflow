#!/usr/bin/env python3
import os
import re
import subprocess
import sys

from setuptools import find_packages, setup

REQUIRES = []
PUBLISH_CMD = ["python", "setup.py", "sdist", "bdist_wheel", "upload"]


def find_version(fname):
    """Attempts to find the version number in the file names fname.
    Raises RuntimeError if not found.
    """
    version = ""
    with open(fname) as fp:
        reg = re.compile(r'__version__ = [\'"]([^\'"]*)[\'"]')
        for line in fp:
            m = reg.match(line)
            if m:
                version = m.group(1)
                break
    if not version:
        raise RuntimeError("Cannot find version information")
    return version


__version__ = find_version("simpleflow/__init__.py")

if "publish" in sys.argv:
    try:
        __import__("wheel")
    except ImportError:
        print("wheel required. Run `pip install wheel`.")
        sys.exit(1)
    status = subprocess.call(PUBLISH_CMD)  # nosec
    sys.exit(status)


def read(fname):
    with open(fname, encoding="utf8") as fp:
        content = fp.read()
    return content


DEPS = [
    "attrs",
    "boto>=2.49.0",
    "diskcache>=4.1.0",
    "Jinja2>=2.11.1",
    "markupsafe>=2.1",
    "kubernetes>=3.0.0",
    "lazy_object_proxy",
    "lockfile>=0.9.1",
    "tabulate>=0.8.2,<1.0.0",
    "setproctitle",
    "click",
    "psutil",
    "pytz",
]

tests_require = []
try:
    for line in open(os.path.join(os.path.dirname(__file__), "requirements-dev.txt")):
        line = re.sub(r"(?: +|^)#.*$", "", line).strip()
        if line:
            tests_require.append(line)
except OSError:
    pass  # absent from distribution

setup(
    name="simpleflow",
    version=__version__,
    description="Python library for dataflow programming with Amazon SWF",
    long_description=(read("README.md") + "\n\n" + read("CHANGELOG.md")),
    long_description_content_type="text/markdown",
    author="Greg Leclercq",
    author_email="tech@botify.com",
    url="https://github.com/botify-labs/simpleflow",
    packages=find_packages(exclude=("test*",)),
    package_dir={
        "simpleflow": "simpleflow",
        "swf": "swf",
    },
    include_package_data=True,
    install_requires=DEPS,
    license="MIT License",
    zip_safe=False,
    keywords="simpleflow amazon swf simple workflow",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    entry_points={
        "console_scripts": [
            "simpleflow = simpleflow.command:cli",
        ]
    },
)
