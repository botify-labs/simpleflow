if False:
    from typing import Any, List, Dict  # NOQA
    from swf.models.decision.base import Decision  # NOQA


class DecisionsAndContext(object):
    """
    Encapsulate decisions and execution context.
    The execution context contains keys with either plain values, lists or sets.
    """
    def __init__(self, decisions=None, execution_context=None):
        self.decisions = decisions or []  # type: List[Decision]
        self.execution_context = execution_context  # type: Dict[str, Any]

    def __repr__(self):
        return '<{} decisions={}, execution_context={}>'.format(
            self.__class__.__name__, self.decisions, self.execution_context
        )

    def append_decision(self, decision):
        # type: (Decision) -> None
        """
        Append a decision.
        """
        self.decisions.append(decision)

    def extend_decision(self, decisions):
        # type: (List[Decision]) -> None
        """
        Append a list of decisions.
        """
        self.decisions += decisions

    def append_kv_to_context(self, key, value):
        # type: (str, Any) -> None
        """
        Set a (key, value) in the execution context.
        """
        if self.execution_context is None:
            self.execution_context = {}
        self.execution_context[key] = value

    def append_kv_to_list_context(self, key, value):
        # type: (str, Any) -> None
        """
        Append a value to a list in the execution context.
        """
        if self.execution_context is None:
            self.execution_context = {}
        if key not in self.execution_context:
            self.execution_context[key] = []
        self.execution_context[key].append(value)

    def append_kv_to_set_context(self, key, value):
        # type: (str, Any) -> None
        """
        Add a value to a set in the execution context.
        """
        if self.execution_context is None:
            self.execution_context = {}
        if key not in self.execution_context:
            self.execution_context[key] = set()
        self.execution_context[key].add(value)
