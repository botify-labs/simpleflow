import time
import collections
import functools
import logging


def constant(value):
    @functools.wraps(constant)
    def call(*args, **kwargs):
        return value

    return call


def exponential(value):
    import random

    return random.random() * (2 ** value)


def with_delay(
        nb_times=1,
        delay=constant(1),
        on_exceptions=Exception,
        log_with=None):
    """
    Retry the *decorated* function *nb_times* with a *delay*.

    :param nb_times: number of times to retry.
    :type  nb_times: int

    :param delay: to wait before the next retry (also called back-off).
    :type  delay: callable(value: int) -> int

    :param on_exceptions: retry only when these exceptions raise.
    :type  on_exceptions: Sequence([Exception])

    """
    if log_with is None:
        log_with = logging.getLogger(__name__).info

    def decorate(func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            nb_retries = 0
            while nb_times - nb_retries:
                try:
                    return func(*args, **kwargs)
                except on_exceptions as error:
                    wait_delay = delay(nb_retries)
                    log_with(
                        'error "%s": retrying in %d seconds',
                        error,
                        wait_delay,
                    )
                    time.sleep(wait_delay)
                    nb_retries += 1
            raise
        return decorated

    if not isinstance(on_exceptions, collections.Sequence):
        on_exceptions = tuple([on_exceptions])
    elif not isinstance(on_exceptions, tuple):
        on_exceptions = tuple(on_exceptions)

    return decorate
