# -*- coding:utf-8 -*-

import threading


class Every(object):
    def __init__(self, nseconds, call, *args, **kwargs):
        """
        Every *nseconds* call ``call(*args, **kwargs)``.

        Execute *call* in a thread.

        """
        self.nseconds = nseconds
        self._call = call
        self._args = args
        self._kwargs = kwargs
        self._is_stopped = threading.Event()
        self._thread = None

    def __call__(self):
        def repeat():
            while not self._is_stopped.wait(self.nseconds):
                if self._is_stopped.is_set():
                    return

                self._call(*self._args, **self._kwargs)

        self._thread = threading.Thread(target=repeat)
        self._thread.daemon = True
        self._thread.start()
        return self

    def stop(self):
        self._is_stopped.set()


def meanwhile(calling_this, call_that, *args, **kwargs):
    """

    :param calling_this: function that executes in a thread or subprocess in
    parallel of *call_that*.
    :type calling_this: callable without argument.

    :param call_that: main function to execute
    :type call_that: callable

    """
    calling_this()
    error_happened = False
    try:
        result = call_that(*args, **kwargs)
    except:
        error_happened = True
    finally:
        calling_this.stop()

    if error_happened:
        raise

    return result
