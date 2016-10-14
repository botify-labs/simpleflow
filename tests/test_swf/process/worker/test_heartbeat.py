import unittest
import os
import signal
import time
from uuid import uuid4
import multiprocessing as mp

import swf.exceptions

from simpleflow.swf.process.worker.heartbeat import (
    HeartbeatProcess,
    Heartbeater,
)


class FakeHeartbeat(object):
    def __init__(self, max_count=1):
        self._token = None
        self._max_count = max_count
        self._count = 0

    def __call__(self, token):
        self._token = token
        self._count += 1
        if self._count == self._max_count:
            raise StopIteration

        return {}


class FakeHeartbeatCancel(FakeHeartbeat):
    def __call__(self, token):
        super(FakeHeartbeatCancel, self).__call__(token)
        return {'cancelRequested': True}


class FakeHeartbeatRaises(FakeHeartbeat):
    def __init__(self, raises, max_count=1):
        super(FakeHeartbeatRaises, self).__init__(max_count)
        self._raises = raises

    def __call__(self, token):
        super(FakeHeartbeatRaises, self).__call__(token)
        raise self._raises


class FakeActivityType(object):
    def __init__(self, name):
        self.name = name


class FakeTask(object):
    def __init__(self, name):
        self.activity_type = FakeActivityType(name)


class FakeTaskHandler(object):
    def __init__(self, callable, args, kwargs, result_queue):
        self._callable = callable
        self._args = args
        self._kwargs = kwargs
        self._result_queue = result_queue

    def __call__(self):
        process = mp.Process(target=self._callable,
                             args=self._args,
                             kwargs=self._kwargs)

        process.start()
        self._result_queue.put(process.pid)


class TestHeartbeatProcess(unittest.TestCase):
    def test_heartbeat_0(self):
        with self.assertRaises(ValueError):
            HeartbeatProcess(lambda *args: None, interval=0)

    def test_heartbeat_non_int(self):
        with self.assertRaises(ValueError):
            HeartbeatProcess(lambda *args: None, interval='NOT_AN_INTEGER')

    def test_fake_heartbeat_count(self):
        heartbeat = FakeHeartbeat(max_count=10)
        count = 3
        for _ in range(count):
            heartbeat(None)

        self.assertEquals(heartbeat._count, count)

    def test_fake_heartbeat_max_count(self):
        max_count = 7
        heartbeat = FakeHeartbeat(max_count)
        for _ in range(max_count - 1):
            heartbeat(None)

        with self.assertRaises(StopIteration):
            heartbeat(None)

        self.assertEquals(heartbeat._count, max_count)

    def test_heartbeat_ok(self):
        heartbeat = FakeHeartbeat(max_count=1)
        heartbeater = HeartbeatProcess(heartbeat, interval=0.1)
        token = uuid4()
        try:
            heartbeater.run(token, FakeTask('test_task'))
        except StopIteration:
            pass
        self.assertEquals(heartbeat._token,
                          token)

    def test_heartbeat_cancel(self):
        heartbeat = FakeHeartbeatCancel(max_count=10)
        heartbeater = HeartbeatProcess(heartbeat, interval=0.1)
        token = uuid4()

        heartbeater.run(token, FakeTask('test_task'))
        self.assertEquals(heartbeat._token,
                          token)

    def test_heartbeat_doesnotexist_error(self):
        error_type = 'Unknown execution: blah'
        heartbeat = FakeHeartbeatRaises(
            swf.exceptions.DoesNotExistError('error', error_type),
            10)
        heartbeater = HeartbeatProcess(heartbeat, interval=0.1)
        token = uuid4()

        heartbeater.run(token, FakeTask('test_task'))
        self.assertEquals(heartbeat._token,
                          token)

    def test_heartbeat_running(self):
        max_count = 3
        heartbeat = FakeHeartbeat(max_count)
        heartbeater = HeartbeatProcess(heartbeat, interval=0.1)
        token = uuid4()
        try:
            heartbeater.run(token, FakeTask('test_task'))
        except StopIteration:
            pass
        self.assertEquals(heartbeat._max_count, max_count)
        self.assertEquals(heartbeat._token,
                          token)

    # TODO: fix test not working in containers
    @unittest.skip("Doesn't work in containers for now")
    def test_heartbeat_process_kill_parent(self):
        heartbeat = FakeHeartbeat(1000)
        heartbeater = HeartbeatProcess(heartbeat, interval=0.1)

        token = uuid4()
        task = FakeTask('test_task')

        result_queue = mp.Queue()
        handler = FakeTaskHandler(heartbeater.run,
                                  (token, task),
                                  {},
                                  result_queue)
        handler_process = mp.Process(target=handler)

        handler_process.start()
        time.sleep(2)
        heartbeat_pid = result_queue.get()
        handler_process.terminate()
        os.kill(handler_process.pid, signal.SIGQUIT)
        handler_process.join()
        time.sleep(2)
        with self.assertRaises(OSError):
            os.kill(heartbeat_pid, signal.SIGKILL)


class Toggler(object):
    def __init__(self, value=True):
        self.value = value

    def __call__(self, *args):
        self.value = not self.value
        return self.value


class TestHeartbeater(unittest.TestCase):
    def test_heartbeater_start(self):
        heartbeater = Heartbeater(lambda *args: None, interval=0.1)
        heartbeater.start(uuid4(), FakeTask('test_task'))
        heartbeater.stop()
        heartbeater._heartbeater.join()
        self.assertFalse(heartbeater._heartbeater.is_alive())

    def test_heartbeater_stop(self):
        toggler = Toggler(True)
        heartbeater = Heartbeater(lambda *args: None,
                                  interval=0.1,
                                  on_exit=toggler)
        heartbeater.start(uuid4(), FakeTask('test_task'))
        heartbeater.stop()
        heartbeater._heartbeater.join()
        self.assertTrue(toggler.value)
