from __future__ import annotations

from .activity import Activity as Activity
from .runtime import logger as logger
from .signal import WaitForSignal as WaitForSignal
from .workflow import Workflow as Workflow

try:
    from importlib.metadata import version
except ImportError:

    def version(distribution_name: str) -> str:  # pyright: ignore[reportUnusedParameter]
        return "unknown"


# TODO: once we drop py3.7, fill this from importlib.metadata
__version__ = version("simpleflow")
__author__ = "Greg Leclercq, Yves Bastide"
__license__ = "MIT"
