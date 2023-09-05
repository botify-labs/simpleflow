from __future__ import annotations

from unittest.mock import patch

import simpleflow.swf.mapper.models

with patch("boto.swf.connect_to_region"):
    DOMAIN = simpleflow.swf.mapper.models.Domain("TestDomain")
DEFAULT_VERSION = "test"
