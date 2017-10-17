# -*- coding: utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

REGISTERED = 'REGISTERED'
DEPRECATED = 'DEPRECATED'

MAX_REASON_LENGTH = 256
MAX_DETAILS_LENGTH = 32768
MAX_EXECUTION_CONTEXT_LENGTH = 32768
MAX_HEARTBEAT_DETAILS_LENGTH = 2048
MAX_IDENTITY_LENGTH = 256
# TODO: experiment with the 2 following limits:
# - the general limits doc states they're 32000: http://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-dg-limits.html
# - the API docs state they're 32768: http://docs.aws.amazon.com/amazonswf/latest/apireference/API_StartWorkflowExecution.html etc.
MAX_INPUT_LENGTH = 32000
MAX_RESULT_LENGTH = 32000

MAX_LOG_FIELD = 500 * 1024  # 500kB

# A SWF workflow cannot last more than a year, and workflows informations are
# accessible for maximum 90 days (retention set at domain creation).
# Hence this value is an upper limit to retrieve *all* open/closed workflow
# executions on a given region+domain.
MAX_WORKFLOW_AGE = 366 + 90 + 1

# Jumbo fields
JUMBO_FIELDS_PREFIX = "simpleflow+s3://"
JUMBO_FIELDS_MAX_SIZE = 5 * 1024 ** 2  # 5MB

# Cache directory
CACHE_DIR = "/tmp/simpleflow-cache"
