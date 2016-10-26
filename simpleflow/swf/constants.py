import os

from simpleflow.constants import SIMPLEFLOW_ENV


# We lower some constants in test environment so tests run faster
if SIMPLEFLOW_ENV == "test":
    MAX_DECISIONS = 10
    MAX_OPEN_ACTIVITY_COUNT = 15
else:
    MAX_DECISIONS = 100
    MAX_OPEN_ACTIVITY_COUNT = int(os.getenv("SWF_MAX_OPEN_ACTIVITY_COUNT", 1000))
