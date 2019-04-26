import multiprocessing
import os
import time

import swf.exceptions
from simpleflow import logger
from simpleflow._decorators import deprecated
from simpleflow.utils import retry


__all__ = ['Heartbeater', 'HeartbeatProcess']


@deprecated
class HeartbeatProcess(object):  # Are people using it?
    def __init__(self, heartbeat_callable, interval):
        if not isinstance(interval, int) and not isinstance(interval, float):
            raise ValueError('heartbeat interval must be an integer or a float')
        if interval <= 0:
            raise ValueError('heartbeat interval must be > 0')
        self._interval = interval
        self._heartbeat = heartbeat_callable

    @retry.with_delay(nb_times=10,
                      delay=retry.exponential,
                      on_exceptions=swf.exceptions.ResponseError,
                      except_on=swf.exceptions.DoesNotExistError)
    def send_heartbeat(self, token):
        return self._heartbeat(token)

    def run(self, token, task):
        ppid = os.getppid()

        while True:
            time.sleep(self._interval)

            if os.getppid() != ppid:
                os._exit(1)

            try:
                logger.info('heartbeat {} for task {}'.format(
                    time.time(),
                    task.activity_type.name))
            except Exception:
                # Do not crash for debug
                pass

            try:
                response = self.send_heartbeat(token)
            except swf.exceptions.DoesNotExistError:
                # Either the task or the workflow execution no longer exists.
                logger.warning(
                    'task {} no longer exists. Stopping heartbeat'.format(
                        task.activity_type.name))
                return
            except Exception as error:
                # Let's crash if it cannot notify the heartbeat failed.
                logger.error('cannot send heartbeat for task {}: {}'.format(
                    task.activity_type.name,
                    error))
                raise

            if response and response.get('cancelRequested'):
                return


@deprecated
class Heartbeater(object):  # Are people using it?
    """Manages the heartbeat in a subprocess.

    """
    def __init__(self, heartbeat, interval, on_exit=None):
        """

        :param heartbeat: callable that sends a heartbeat.
        :type  heartbeat: callable(token: str): dict

        :param interval: of heartbeating in seconds.
        :type  interval: int.

        :param on_exit: signal handler called if the heartbeat subprocess
        exits. It is not called when calling ``Heartbeater.stop()``.
        :type  on_exit: callable(signal_number: int, frame)

        """
        self._heartbeat = heartbeat
        self._interval = interval
        self._on_exit = on_exit
        self._signal_handlers = {}
        self._heartbeater = None

    def start(self, token, task):
        """Start heartbeating in a child process.

        The only child is the heartbeat process.
        If it stops, SWF will trigger a heartbeat timeout.

        :param token: used in the heartbeat message.
        :type  token: str

        :task  task: SWF activity task.
        :type  task: ``swf.models.ActivityTask``
        """
        self._heartbeater = multiprocessing.Process(
            target=HeartbeatProcess(self._heartbeat, self._interval).run,
            args=(token, task)
        )
        self._heartbeater.start()
        return self

    def stop(self):
        assert self._heartbeater is not None

        self._heartbeater.terminate()

        return self
