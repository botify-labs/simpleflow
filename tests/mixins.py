import boto
import json


class TestBucketMixin(object):
    """
    Handle buckets and keys creation over S3
    """

    def _get_bucket(self):
        bucket = 'test_bucket'
        s3 = boto.connect_s3()
        test_bucket = s3.create_bucket(bucket)
        return test_bucket

    def _create_file(self, bucket, file_name):
        key = boto.s3.key.Key(bucket)
        key.name = file_name
        key.set_contents_from_string("")

    def _create_json_file(self, bucket, file_name, dct):
        key = boto.s3.key.Key(bucket)
        key.name = file_name
        key.set_contents_from_string(json.dumps(dct))
