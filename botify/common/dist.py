"""
This module wraps the :func:`setuptools.setup` function and enforces common
conventions of Botify's projects.

"""

import setuptools
from ConfigParser import ConfigParser

from botify.common.release import Release
from botify.common import io


__all__ = ['setup']


DEFAULT_AUTHOR = 'Botify'
DEFAULT_AUTHOR_EMAIL = 'tech@botify.com'


def get_release_config():
    config = ConfigParser()
    config.readfp(open('setup.cfg'))
    return {
        'package': config.get('release', 'package'),
        'url': config.get('release', 'url'),
    }


def get_version(config):
    return __import__(config['package']).__version__


def get_dependencies(directory='packaging'):
    try:
        install_deps = io.read_lines('packaging/python.deps')
    except IOError as err:
        if '[Errno 2] No such file or directory' in str(err):
            install_deps = []

    return {
        'install': install_deps,
    }


def setup(**kwargs):
    config = get_release_config()
    dependencies = get_dependencies()

    cmdclass = {
        'release': Release,
    }
    if 'cmdclass' in kwargs:
        cmdclass.update(kwargs.pop('cmdclass'))

    return setuptools.setup(
        version=get_version(config),
        url=config['url'],
        author=kwargs.pop('author', DEFAULT_AUTHOR),
        author_email=kwargs.pop('author_email', DEFAULT_AUTHOR_EMAIL),

        install_requires=dependencies['install'],
        packages=setuptools.find_packages(),
        cmdclass=cmdclass,
        **kwargs
    )
