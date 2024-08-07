from __future__ import annotations

from . import exceptions


class ModuleDispatcher:
    """
    Dispatch in a given namespace.
    """

    def __init__(self, mod, mapping=None):
        self._module = mod
        self._mapping = mapping

    def dispatch(self, name):
        """
        Dispatch a *name* to a *function* by finding a module to load.

        It parses *name* as ``{module}.{function}`` and returns a function
        object.

        """
        submodule_name, func_name = name.rsplit(".", 1)
        if self._mapping is not None:
            try:
                submodule_name = self._mapping[submodule_name]
            except KeyError:
                raise exceptions.DispatchError(f"cannot dispatch {name}")

        submodule = getattr(self._module, submodule_name)
        return getattr(submodule, func_name)
