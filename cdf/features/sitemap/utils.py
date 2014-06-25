import requests
import logging
from retrying import retry, RetryError

from cdf.log import logger
from cdf.features.sitemap.exceptions import DownloadError

#change request logger log level
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)


#retry with exponential backoff.
#FIXME this reponsibility should be delegated to the workflow.
@retry(wait_exponential_multiplier=1000,
       stop_max_attempt_number=7)
def download_url(url, output_file_path, user_agent=None):
    """Helper function to download the content of a url in a file.
    We have to use a helper function because "retry" throws a RetryError
    that encapsulates the true exception.
    :param url: the input url
    :type url: str
    :param output_file_path: the output file path
    :type output_file_path: str
    :param user_agent: the user agent to use for the query.
                       If None, uses 'requests' default user agent
    :type user_agent: str
    :raises: DownloadError
    """

    if user_agent is not None:
        headers = {
            'User-Agent': user_agent,
        }
    else:
        headers = None

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(output_file_path, 'wb') as output_file:
            for chunk in response.iter_content():
                output_file.write(chunk)
    else:
        error_message = "Could not download {}: {}".format(url, response.status_code)
        logger.warning(error_message)
        raise DownloadError(error_message)
