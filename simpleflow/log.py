from datetime import datetime
import logging.config
import sys

from . import logging_context, settings


RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
ORANGE = "\033[38;5;214m"
END = '\033[0m'


class ColorModes(object):
    AUTO = 'auto'
    ALWAYS = 'always'
    NEVER = 'never'


color_mode = 'auto'


def colorize(level, message):
    # if not in a tty, we're likely redirected or piped
    if color_mode == ColorModes.NEVER or (color_mode == ColorModes.AUTO and not sys.stdout.isatty()):
        return message

    # color mappings
    colors = {
        "CRITICAL": RED,
        "ERROR": RED,
        "WARNING": YELLOW,
        "INFO": GREEN,
        "DEBUG": BLUE,
        "NOTSET": END,
        "RED": RED,
        "GREEN": GREEN,
        "YELLOW": YELLOW,
        "BLUE": BLUE,
    }

    # colored string
    return "".join([colors[level], message, END])


class SimpleflowFormatter(logging.Formatter):
    # Example of record dict:
    # {
    #     'threadName': 'MainThread',
    #     'name': 'simpleflow.swf.process.poller',
    #     'thread': 140735241315072,
    #     'created': 1450436645.513802,
    #     'process': 84828,
    #     'processName': 'MainProcess',
    #     'args': (),
    #     'module': 'poller',,
    #     'filename': 'poller.py',
    #     'levelno': 20,
    #     'exc_text': None,
    #     'pathname': '/path/to/simpleflow/simpleflow/swf/process/poller.py',
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

        # don't risk bad interpolation if args is empty (in most cases it's because the
        # logged string is already formatted)
        if record.args:
            record.message = record.msg % record.args
        else:
            record.message = record.msg

        record.coloredlevel = colorize(record.levelname, record.levelname)
        s = "%(isodate)s %(coloredlevel)s [process=%(processName)s, pid=%(process)s]: %(message)s" % record.__dict__

        # C&P from logging.Formatter#format
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            try:
                s = s + record.exc_text
            except UnicodeError:
                # Sometimes filenames have non-ASCII chars, which can lead
                # to errors when s is Unicode and record.exc_text is str
                # See issue 8924.
                # We also use replace for when there are multiple
                # encodings, e.g. UTF-8 for the filesystem and latin-1
                # for a script. See issue 13232.
                s = s + record.exc_text.decode(sys.getfilesystemencoding(),
                                               'replace')

        return s


class SyslogFormatter(logging.Formatter):
    # Example of record dict:
    # {
    #     'threadName': 'MainThread',
    #     'name': 'simpleflow.swf.process.poller',
    #     'thread': 140735241315072,
    #     'created': 1450436645.513802,
    #     'process': 84828,
    #     'processName': 'MainProcess',
    #     'args': (),
    #     'module': 'poller',,
    #     'filename': 'poller.py',
    #     'levelno': 20,
    #     'exc_text': None,
    #     'pathname': '/path/to/simpleflow/simpleflow/swf/process/poller.py',
    #     'lineno': 76,
    #     'msg': 'starting <bound method ActivityPoller.start of <simpleflow.swf.process.worker.base.ActivityPoller>',
    #     'exc_info': None,
    #     'funcName': 'start',
    #     'relativeCreated': 235.57710647583008,
    #     'levelname': 'INFO',
    #     'msecs': 513.8020515441895,
    # }
    def format(self, record):
        msg = []
        workflow_id = logging_context.get("workflow_id")[0:64]
        if workflow_id:
            msg.append(workflow_id + ":")
            msg.append("{}#{}".format(logging_context.get("task_type"), logging_context.get("event_id")))

        msg.append(record.levelname)
        msg.append("pid={}".format(record.process))
        msg.append(record.message)
        return " ".join(msg)


def setup_logging():
    base_settings = settings.base.load()
    config = base_settings["LOGGING"]

    syslog_target = base_settings.get("SIMPLEFLOW_SYSLOG_TARGET")
    if syslog_target:
        host, port = syslog_target.rsplit(":", 1)
        config = setup_syslog_logging(config, host, int(port))

    logging.config.dictConfig(config)


def setup_syslog_logging(config, host, port):
    config["loggers"]["simpleflow"]["handlers"].append("syslog")
    config["handlers"]["syslog"] = {
        "class": "logging.handlers.SysLogHandler",
        "address": (host, port),
        "level": "DEBUG",
        "formatter": "syslog_formatter",
    }
    config["formatters"]["syslog_formatter"] = {
        "()": "simpleflow.log.SyslogFormatter",
        "format": "%(message)s",
    }
    return config
