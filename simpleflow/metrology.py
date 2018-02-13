import abc
import json
import os
import re
import time
from collections import OrderedDict
try:
    from urllib.parse import quote_plus  # py 3.x
except ImportError:
    from urllib import quote_plus  # py 2.x

from . import storage, settings
from .swf.stats.pretty import dump_history_to_json
from .workflow import Workflow

ACTIVITY_KEY_RE = re.compile(r'activity\.(.+)\.json')


class StepIO(object):
    def __init__(self):
        self.bytes = 0
        self.records = 0
        self.sampled = False

    def get_stats(self, time_total):
        mb_s = None
        rec_s = None
        if self.bytes:
            mb_s = round(float(self.bytes) / (1024 * 1024) / time_total, 2)
        if self.records:
            rec_s = int(self.records / time_total)
        return OrderedDict([
            ('bytes', self.bytes),
            ('records', self.records),
            ('mb_s', mb_s),
            ('rec_s', rec_s),
            ('sampled', self.sampled)
        ])


class Step(object):
    def __init__(self, name, task):
        self.name = name
        self.task = task
        self.read = StepIO()
        self.write = StepIO()
        self.time_started = time.time()
        self.time_finished = None
        self.time_total = None
        self.metadata = {}

    def done(self):
        self.time_finished = time.time()
        self.time_total = self.time_finished - self.time_started

    def get_stats(self):
        stats = OrderedDict([
            ('name', self.name),
            ('metadata', self.metadata),
            ('time_started', self.time_started),
            ('time_finished', self.time_finished),
            ('time_total', self.time_total),
            ('read', self.read.get_stats(self.time_total)),
            ('write', self.write.get_stats(self.time_total)),
        ])
        return stats

    def mset_metadata(self, kvs):
        for (k, v) in kvs:
            self.metadata[k] = v


class StepExecution(object):
    def __init__(self, step):
        self.step = step

    def __enter__(self):
        return self.step

    def __exit__(self, type, value, traceback):
        self.step.done()


class MetrologyTask(object):

    def can_upload(self):
        return all(c in self.context for c in ('workflow_id', 'run_id', 'activity_id'))

    @property
    def metrology_path(self):
        path = []
        if settings.METROLOGY_PATH_PREFIX is not None:
            path.append(settings.METROLOGY_PATH_PREFIX)
        path.append(self.context["workflow_id"])
        path.append(quote_plus(self.context["run_id"]))
        path.append("activity.{}.json".format(self.context["activity_id"]))
        return str(os.path.join(*path))

    def step(self, name):
        """
        To be called in a `with` execution
        Ex :
        > with self.step('My step') as step:
        >     step.records = 5
        """
        step = Step(name, self)
        step_exec = StepExecution(step)
        if not hasattr(self, 'steps'):
            self.steps = []
        self.steps.append(step)
        return step_exec

    def upload_stats(self):
        if not self.can_upload():
            return

        content = {"steps": [], "meta": getattr(self, 'meta', None)}

        for step in getattr(self, 'steps', []):
            content["steps"].append(step.get_stats())

        storage.push_content(
            settings.METROLOGY_BUCKET,
            self.metrology_path,
            json.dumps(content, indent=2),
            content_type="application/json")

    @abc.abstractmethod
    def execute(self):
        pass

    def post_execute(self):
        self.upload_stats()


class MetrologyWorkflow(Workflow):

    def after_closed(self, history):
        super(MetrologyWorkflow, self).after_closed(history)
        return self.push_metrology(history)

    @property
    def metrology_path(self):
        path = []
        if settings.METROLOGY_PATH_PREFIX:
            path.append(settings.METROLOGY_PATH_PREFIX)

        context = self.get_run_context()
        path.append(quote_plus(context["workflow_id"]))
        path.append(quote_plus(context["run_id"]))
        return str(os.path.join(*path))

    def push_metrology(self, history):
        """
        Fetch workflow history and merge it with metrology
        """
        activity_keys = [obj for obj in storage.list_keys(
            settings.METROLOGY_BUCKET,
            self.metrology_path)]
        history_dumped = dump_history_to_json(history)
        history = json.loads(history_dumped)

        for key in activity_keys:
            if not key.key.startswith(os.path.join(self.metrology_path, 'activity.')):
                continue
            contents = key.get_contents_as_string(encoding='utf-8')
            result = json.loads(contents)
            search = ACTIVITY_KEY_RE.search(key.name)
            name = search.group(1)
            for h in history:
                if h[0] == name:
                    h[1]["metrology"] = result

        storage.push_content(
            settings.METROLOGY_BUCKET,
            os.path.join(self.metrology_path, 'metrology.json'),
            json.dumps(history, indent=2),
            content_type="application/json"
        )
