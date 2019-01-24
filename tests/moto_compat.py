try:
    from moto import mock_s3_deprecated as mock_s3, mock_swf_deprecated as mock_swf
except ImportError:
    from moto import mock_s3, mock_swf
