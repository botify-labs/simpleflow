import functools

from setproctitle import setproctitle

from simpleflow import logger


def with_state(state):
    """
    Decorator used to change the process name when changing state.
    :param state: new state
    :type  state: str
    """
    def wrapper(method):
        @functools.wraps(method)
        def wrapped(self, *args, **kwargs):
            logger.debug("entering state {}: {}(args={}, kwargs={})".format(
                state, method.__name__, args, kwargs))
            self.state = state
            return method(self, *args, **kwargs)

        wrapped.__wrapped__ = method
        return wrapped
    return wrapper


class NamedMixin(object):
    """
    NamedMixin in conjunction with the "with_state()" decorator allows to change
    the process name depending on the worker state.

    To do that, you need to:
    1- include the "NamedMixin" as a parent class (and call its __init__()
        method explicitly if not the first parent)
    2- decorate your methods with "@with_state("my_state")"

    You can optionnally expose some other attributes of your worker by defining
    the "_named_mixin_properties" attribute to a list or tuple of fields you want
    to include in your process title. For instance:

        self._named_mixin_properties = ["task_list"]

    ... will resul in a process named like this:

        simpleflow YourClass(task_list=<value>)[running]

    """
    def __init__(self, *args, **kwargs):
        self.state = kwargs.get("state", "initializing")

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value
        self.set_process_name()

    def set_process_name(self):
        klass = self.__class__.__name__
        properties = []
        for prop in getattr(self, "_named_mixin_properties", []):
            properties.append("{}={}".format(prop, getattr(self, prop)))
        name = "{}({})".format(klass, ", ".join(properties))
        setproctitle("simpleflow {}[{}]".format(name, self.state))
