import time
from functools import wraps
import requests
import random
import logging
from cdf.log import logger

from cdf.features.sitemap.exceptions import DownloadError

#change request logger log level
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

def exponential_backoff(func):
    '''
    Decorator that implements exponentional backoff.
    '''
    @wraps(func)
    def wrapper(url, output_file_path):
        MAX_RETRIES = 5
        last_exception = None
        for i in range(MAX_RETRIES + 1):
            try:
                result = func(url, output_file_path)
                if last_exception is not None:
                    logger.info("{} successfully downloaded.".format(url))
                return result
            except DownloadError as e:
                last_exception = e
                logger.warning("Retrying to download: {}.".format(url))
                time.sleep((2 ** i) + random.random())
        raise last_exception

    return wrapper


@exponential_backoff
def download_url(url, output_file_path):
    """Download the content of a url in a file.
    :param url: the input url
    :type url: str
    :param output_file_path: the output file path
    :type output_file_path: str
    """
    response = requests.get(url)
    if response.status_code == 200:
        with open(output_file_path, 'wb') as output_file:
            for chunk in response.iter_content():
                output_file.write(chunk)
    else:
        raise DownloadError("Could not download {}: {}".format(url, response.status_code))


