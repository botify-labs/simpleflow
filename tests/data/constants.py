from __future__ import annotations

from unittest.mock import patch

import swf.models

with patch("boto.swf.connect_to_region"):
    DOMAIN = swf.models.Domain("TestDomain")
DEFAULT_VERSION = "test"
