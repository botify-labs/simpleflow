import inspect
import os

from vcr import VCR


# Default SWF parameters
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["SWF_DOMAIN"] = "TestDomain"


# VCR config with better defaults for SWF API calls
def test_name_to_cassette_path(function):
    name = function.__name__
    directory = os.path.dirname(inspect.getfile(function))
    return os.path.join(directory, "cassettes", name + ".yaml")


vcr = VCR(
    func_path_generator=test_name_to_cassette_path,
    filter_headers=[
        ("Authorization", "AWS4-HMAC-SHA256 Credential=1234AB/20160823/us-east-1/swf/aws4_request,SignedHeaders=host;x-amz-date;x-amz-target,Signature=foobar"),
    ],
)
