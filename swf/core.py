# -*- coding:utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.
import os

from boto.exception import NoAuthHandlerFound
import boto.swf

# NB: import logger directly from simpleflow so we benefit from the logging
# config hosted in simpleflow. This wouldn't be the case with a standard
# "logging.getLogger(__name__)" which would write logs under the "swf" namespace
from simpleflow import logger
from simpleflow.utils import retry

from . import settings


SETTINGS = settings.get()
RETRIES = int(os.environ.get('SWF_CONNECTION_RETRIES', '5'))


class ConnectedSWFObject(object):
    """Authenticated object interface

    Provides the instance attributes:

    :ivar region: name of the AWS region
    :type region: str
    :ivar connection: connection to the SWF endpoint
    :type connection: boto.swf.layer1.Layer1

    """
    __slots__ = [
        'region',
        'connection'
    ]

    @retry.with_delay(nb_times=RETRIES,
                      delay=retry.exponential,
                      on_exceptions=(TypeError, NoAuthHandlerFound))
    def __init__(self, *args, **kwargs):
        self.region = (SETTINGS.get('region') or
                       kwargs.get('region') or
                       boto.swf.layer1.Layer1.DefaultRegionName)
        # Use settings-provided keys if available, otherwise pass empty
        # dictionary to boto SWF client, which will use its default credentials
        # chain provider.
        cred_keys = ['aws_access_key_id', 'aws_secret_access_key']
        creds_ = {k: SETTINGS[k] for k in cred_keys if SETTINGS.get(k, None)}
        self.connection = (kwargs.pop('connection', None) or
                           boto.swf.connect_to_region(self.region, **creds_))
        if self.connection is None:
            raise ValueError('invalid region: {}'.format(self.region))

        logger.debug("initiated connection to region={}".format(self.region))
