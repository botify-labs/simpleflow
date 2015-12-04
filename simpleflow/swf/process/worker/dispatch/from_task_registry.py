class RegistryDispatcher(object):
    """
    Map a name to a task handler wrt a task registry.

    The registry has the format ``{label: {name: task}}``.

    """
    def __init__(self, registry, label, workflow):
        """

        :param registry: of tasks.
        :type  registry: {str: {str: Task}}
        :param label: name of the task list.
        :type  label: str.
        :param workflow: definition.
        :type workflow: Workflow.

        """
        self._registry = registry
        self._label = label

    def dispatch(self, name):
        """
        :param name: of the task to dispatch.
        :type  name: str.

        :returns:
            :rtype: callable.

        """
        return self._registry[self._label][name]._callable

    def dispatch_activity(self, name):
        """
        :param name: of the task to dispatch.
        :type  name: str.

        :returns:
            :rtype: Activity

        """
        return self._registry[self._label][name]
