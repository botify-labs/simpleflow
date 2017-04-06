import os

SIMPLEFLOW_ENV = os.getenv("SIMPLEFLOW_ENV", "production")

# Constants useful for defining SWF timeouts
MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR
