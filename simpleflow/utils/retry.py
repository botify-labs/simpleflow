from __future__ import annotations

import functools
import time
from collections.abc import Sequence

from simpleflow import logger


def _to_tuple(exceptions):
    if not isinstance(exceptions, Sequence):
        return tuple([exceptions])
    elif not isinstance(exceptions, tuple):
        return tuple(exceptions)
    return exceptions


def constant(value):
    @functools.wraps(constant)
    def call(*args, **kwargs):
        return value

    return call


def exponential(value):
    """
    Set retry time exponentially; per the "+ 1," begin at a minimum of one second.
    """
    import random

    return random.random() * (2**value) + 1  # nosec


def with_delay(
    nb_times=1,
    delay=constant(1),
    on_exceptions=Exception,
    except_on=None,
    log_with=None,
):
    """
    Retry the *decorated* function *nb_times* with a *delay*.

    :param nb_times: number of times to retry.
    :type  nb_times: int

    :param delay: to wait before the next retry (also called back-off).
    :type  delay: callable(value: int) -> int

    :param on_exceptions: retry only when these exceptions raise.
    :type  on_exceptions: Exception | Sequence([Exception])

    :param except_on: don't retry on these exceptions.
    :type  except_on: Sequence([Exception])

    :param log_with: logger instance to use.
    """
    if log_with is None:
        log_with = logger.info
    if except_on is None:
        except_on = ()  # Can't "except None" in py3

    def decorate(func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            nb_retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except except_on:
                    raise
                except on_exceptions as error:
                    wait_delay = delay(nb_retries)
                    log_with(
                        'error "%r": retrying in %.2f seconds',
                        error,
                        wait_delay,
                    )
                    time.sleep(wait_delay)
                    nb_retries += 1
                    if nb_times - nb_retries <= 0:
                        raise

        return decorated

    on_exceptions = _to_tuple(on_exceptions)
    except_on = _to_tuple(except_on)

    return decorate
