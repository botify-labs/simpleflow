# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING

from swf.utils import decapitalize, underscore_to_camel

if TYPE_CHECKING:
    from typing import Any


def decision_action(fn):
    """Ensures the decorated method class instance is bootstraped
    with decision type, attributes_key, and body
    """

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        self._fill_from_action(fn.__name__)
        return fn(self, *args, **kwargs)

    return wrapper


class Decision(dict):
    """Base decision message wrapper

    subclasses dictionary and provides attributes and methods
    to build a suitable Decision message that Amazon service will
    understand.

    It is meant to be subclassed, and does not intend to be instantiated
    as is.

    :param  action: Decision action type
    :type   action: string
    """

    _attributes_key_suffix = "DecisionAttributes"
    _base_type = None

    def __init__(self, action=None, *args, **kwargs):
        super().__init__()

        if action and hasattr(self, action):
            action_method = getattr(self, action)
            if callable(action_method):
                action_method(*args, **kwargs)

    def _fill_from_action(self, action):
        self.type = underscore_to_camel(action) + self._base_type
        self.attributes_key = decapitalize(self.type + self._attributes_key_suffix)

        self["decisionType"] = self.type
        self[self.attributes_key] = {}

    def update_attributes(self, data: dict[str, Any]):
        """Updates Decision instance attributes_key dictionary
        with provided data which values is not None

        :param  data:
        :type   data:
        """
        if not hasattr(self, "attributes_key"):
            raise AttributeError("Can't update unset attributes_key" "decision attribute")

        for key, value in data.items():
            if value:
                self[self.attributes_key].update({key: value})
