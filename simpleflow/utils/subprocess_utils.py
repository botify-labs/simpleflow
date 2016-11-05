"""
Utilities methods for subprocess.Popen (or subprocess32.Popen) objects.
"""

from __future__ import absolute_import, print_function
import sys

import errno

import os
from io import StringIO

from simpleflow.compat import unicode

mswindows = (sys.platform == "win32")
if mswindows:
    _has_poll = False
else:
    import select
    _has_poll = hasattr(select, 'poll')
    _PIPE_BUF = getattr(select, 'PIPE_BUF', 512)


def communicate_with_limits(proc, input=None, stdout_encoding='utf-8', stderr_encoding='utf-8', stdout_limit=None,
                            stderr_limit=None):
    """
    Like Popen.communicate but with bounds on stdout/stderr returned data. Only keep the last *_limit characters.

    Also, return unicode (py2) or str (py3) data.

    The "natural" implementation (for us) uses select.poll and should work on all current Unix variants. A fallback
    version is used on Windows or strange Unices, with unbounded memory use.

    (The poll-using version is not especially time-efficient; this is not meant for high-output children.)

    TODO: encode input?

    :param proc:
    :type proc: subprocess.Popen
    :param input:
    :param stdout_encoding:
    :param stderr_encoding:
    :param stdout_limit:
    :param stderr_limit:
    :return: (str, str)
    """
    if not _has_poll:
        communicate = communicate_with_limits_naive
    else:
        communicate = communicate_with_limits_poll
    stdout, stderr = communicate(proc, input, stdout_encoding, stderr_encoding, stdout_limit, stderr_limit)
    return stdout, stderr


def communicate_with_limits_naive(proc, input, stdout_encoding, stderr_encoding, stdout_limit, stderr_limit):
    """

    :param stdout_encoding:
    :param stderr_encoding:
    :param proc:
    :type proc: subprocess.Popen
    :param input:
    :param stdout_limit:
    :param stderr_limit:
    :return: (str, str)
    """
    stdout, stderr = proc.communicate(input)
    if stdout is not None and not isinstance(stdout, unicode):
        stdout = stdout.decode(stdout_encoding, errors='replace')
    if stderr is not None and not isinstance(stderr, unicode):
        stderr = stderr.decode(stderr_encoding, errors='replace')
    if stdout_limit is not None and stdout:
        stdout = stdout[-stdout_limit:]
    if stderr_limit is not None and stderr:
        stderr = stderr[-stderr_limit:]
    return stdout, stderr


def communicate_with_limits_poll(proc, input, stdout_encoding, stderr_encoding, stdout_limit, stderr_limit):
    """

    :param stdout_encoding:
    :param stderr_encoding:
    :param proc:
    :type proc: subprocess.Popen
    :param input:
    :param stdout_limit:
    :param stderr_limit:
    :return: (str, str)
    """
    if proc.stdin:
        # Flush stdio buffer.  This might block, if the user has
        # been writing to .stdin in an uncontrolled fashion.
        proc.stdin.flush()
        if not input:
            proc.stdin.close()

    stdout = None  # Return
    stderr = None  # Return
    fd2file = {}
    fd2data = {}

    poller = select.poll()

    def register_and_append(file_obj, eventmask):
        poller.register(file_obj.fileno(), eventmask)
        fd2file[file_obj.fileno()] = file_obj

    def close_unregister_and_remove(fd):
        poller.unregister(fd)
        fd2file[fd].close()
        fd2file.pop(fd)

    if proc.stdin and input:
        register_and_append(proc.stdin, select.POLLOUT)

    select_POLLIN_POLLPRI = select.POLLIN | select.POLLPRI
    if proc.stdout:
        register_and_append(proc.stdout, select_POLLIN_POLLPRI)
        stdout = StringIO()
        fd2data[proc.stdout.fileno()] = (stdout, stdout_encoding, stdout_limit)
    if proc.stderr:
        register_and_append(proc.stderr, select_POLLIN_POLLPRI)
        stderr = StringIO()
        fd2data[proc.stderr.fileno()] = (stderr, stderr_encoding, stderr_limit)

    input_offset = 0
    while fd2file:
        try:
            ready = poller.poll()
        except select.error as e:
            if e.args[0] == errno.EINTR:
                continue
            raise

        for fd, mode in ready:
            if mode & select.POLLOUT:
                chunk = input[input_offset: input_offset + _PIPE_BUF]
                try:
                    input_offset += os.write(fd, chunk)
                except OSError as e:
                    if e.errno == errno.EPIPE:
                        close_unregister_and_remove(fd)
                    else:
                        raise
                else:
                    if input_offset >= len(input):
                        close_unregister_and_remove(fd)
            elif mode & select_POLLIN_POLLPRI:
                data = os.read(fd, 4096)
                if not data:
                    close_unregister_and_remove(fd)
                file_obj, encoding, limit = fd2data[fd]
                if not isinstance(data, unicode):
                    data = data.decode(encoding, 'replace')
                file_obj.write(data)
                if limit is not None and file_obj.tell() > limit:
                    file_obj.seek(file_obj.tell() - limit)
                    kept = file_obj.read()
                    file_obj.seek(0)
                    file_obj.write(kept)
                    file_obj.truncate()
            else:
                # Ignore hang up or errors.
                close_unregister_and_remove(fd)

    proc.wait()
    return stdout.getvalue() if stdout else None, stderr.getvalue() if stderr else None
