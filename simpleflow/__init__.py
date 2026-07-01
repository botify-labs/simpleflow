from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .activity import Activity as Activity
from .runtime import logger as logger
from .signal import WaitForSignal as WaitForSignal
from .workflow import Workflow as Workflow

try:
    __version__ = version("simpleflow")
except PackageNotFoundError:
    # Running from a source checkout without the distribution installed.
    __version__ = "unknown"
__author__ = "Greg Leclercq, Yves Bastide"
__license__ = "MIT"
