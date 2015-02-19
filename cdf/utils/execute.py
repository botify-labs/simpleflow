from __future__ import absolute_import

from distutils.spawn import find_executable
import logging

from simpleflow import execute

import cdf.settings


logger = logging.getLogger(__name__)


def with_pypy(fallback_interpreter='python', enable=True):
    def wrapped(func):
        if not enable:
            return func

        py_interpreter = interpreter
        if not find_executable(py_interpreter):
            msg = 'will not be able to execute {}: '
            'intepreter {} not found'.format(
                func.func_name,
                py_interpreter,
            )

            if not fallback_interpreter:
                raise RuntimeError(msg)

            logger.error(msg + ' (continuing...)')
            py_interpreter = fallback_interpreter

        return execute.python(py_interpreter)(func)

    interpreter = cdf.settings.PYPY_PATH

    return wrapped
