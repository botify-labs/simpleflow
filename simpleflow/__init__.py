# -*- coding: utf-8 -*-

import logging.config

from .activity import Activity  # NOQA
from .workflow import Workflow  # NOQA

from . import settings


__version__ = '0.12.4'
__author__ = 'Greg Leclercq'
__license__ = "MIT"

logging.config.dictConfig(settings.base.load()['LOGGING'])
logger = logging.getLogger(__name__)
