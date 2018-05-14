# -*- coding: utf-8 -*-
import logging.config
from .activity import Activity  # NOQA
from .log import setup_logging
from .workflow import Workflow  # NOQA
from .signal import WaitForSignal  # NOQA

__version__ = '0.21.10'
__author__ = 'Greg Leclercq'
__license__ = "MIT"


setup_logging()
logger = logging.getLogger(__name__)
