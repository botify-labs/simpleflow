# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from __future__ import annotations

REGISTERED = "REGISTERED"
DEPRECATED = "DEPRECATED"

# A SWF workflow cannot last more than a year, and workflows information are
# accessible for maximum 90 days (retention set at domain creation).
# Hence, this value is an upper limit to retrieve *all* open/closed workflow
# executions on a given region+domain.
MAX_WORKFLOW_AGE = 366 + 90 + 1
