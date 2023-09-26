from __future__ import annotations

from functools import wraps
from typing import Any

from simpleflow.swf.mapper.utils import decapitalize, underscore_to_camel


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
    """

    _attributes_key_suffix = "DecisionAttributes"
    _base_type: str | None = None

    def __init__(self, action: str | None = None, *args, **kwargs) -> None:
        super().__init__()

        if action and hasattr(self, action):
            action_method = getattr(self, action)
            if callable(action_method):
                action_method(*args, **kwargs)

    def _fill_from_action(self, action: str) -> None:
        self.type = underscore_to_camel(action) + self._base_type
        self.attributes_key = decapitalize(self.type + self._attributes_key_suffix)

        self["decisionType"] = self.type
        self[self.attributes_key] = {}

    def update_attributes(self, data: dict[str, Any]) -> None:
        """Updates Decision instance attributes_key dictionary
        with provided data which values is not None
        """
        if not hasattr(self, "attributes_key"):
            raise AttributeError("Can't update unset attributes_key")

        for key, value in data.items():
            if value:
                self[self.attributes_key].update({key: value})
