import swf.models
from mock import patch

with patch('boto.swf.connect_to_region'):
    DOMAIN = swf.models.Domain('TestDomain')
DEFAULT_VERSION = 'test'
