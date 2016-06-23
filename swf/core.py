# -*- coding:utf-8 -*-

# Copyright (c) 2013, Theo Crevon
# Copyright (c) 2013, Greg Leclercq
#
# See the file LICENSE for copying permission.

from boto.exception import NoAuthHandlerFound
import boto.swf

from simpleflow.utils import retry

from . import settings


SETTINGS = settings.get()


class ConnectedSWFObject(object):
    """Authenticated object interface

    Provides the instance attributes:

    - `region`: name of the AWS region
    - `connection`: to the SWF endpoint (`boto.swf.layer1.Layer1` object):

    """
    __slots__ = [
        'region',
        'connection'
    ]

    @retry.with_delay(nb_times=5,
                      delay=retry.exponential,
                      on_exceptions=(TypeError, NoAuthHandlerFound))
    def __init__(self, *args, **kwargs):
        settings_ = {key: SETTINGS.get(key, kwargs.get(key)) for key in
                     ('aws_access_key_id',
                      'aws_secret_access_key')}

        self.region = (SETTINGS.get('region') or
                       kwargs.get('region') or
                       boto.swf.layer1.Layer1.DefaultRegionName)

        self.connection = (kwargs.pop('connection', None) or
                           boto.swf.connect_to_region(self.region, **settings_))
        if self.connection is None:
            raise ValueError('invalid region: {}'.format(self.region))
