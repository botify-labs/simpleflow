import logging

# module level logger
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger('comparison')
logger.setLevel(logging.INFO)