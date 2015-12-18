from datetime import datetime
import logging
import sys


RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
END = '\033[0m'


def colorize(level, message):
    # if not in a tty, we're likely redirected or piped
    if not sys.stdout.isatty():
        return message

    # color mappings
    colors = {
        "CRITICAL": RED,
        "ERROR": RED,
        "WARNING": YELLOW,
        "INFO": GREEN,
        "DEBUG": BLUE,
        "NOTSET": END,
    }

    # colored string
    return "".join([colors[level], message, END])


class SimpleflowFormatter(logging.Formatter):
    # Example of record dict:
    # {
    #     'threadName': 'MainThread',
    #     'name': 'simpleflow.swf.process.actor',
    #     'thread': 140735241315072,
    #     'created': 1450436645.513802,
    #     'process': 84828,
    #     'processName': 'MainProcess',
    #     'args': (),
    #     'module': 'actor',,
    #     'filename': 'actor.py',
    #     'levelno': 20,
    #     'exc_text': None,
    #     'pathname': '/path/to/simpleflow/simpleflow/swf/process/actor.py',
    #     'lineno': 76,
    #     'msg': 'starting <bound method ActivityPoller.start of <simpleflow.swf.process.worker.base.ActivityPoller>',
    #     'exc_info': None,
    #     'funcName': 'start',
    #     'relativeCreated': 235.57710647583008,
    #     'levelname': 'INFO',
    #     'msecs': 513.8020515441895,
    # }
    def format(self, record):
        # self.formatTime() is documented as using ISO8601 format,
        # but in fact not, so we roll our own formatting
        # NB: we strip microseconds out so things are readable
        date = datetime.fromtimestamp(record.created).replace(microsecond=0)
        record.isodate = date.isoformat()
        record.message = record.msg % record.args
        record.coloredlevel = colorize(record.levelname, record.levelname)
        return "%(isodate)s %(coloredlevel)s [process=%(processName)s, pid=%(process)s]: %(message)s" % record.__dict__
