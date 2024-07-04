from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from simpleflow.swf.mapper.exceptions import ResponseError, translate


def test_translate():
    def raiser():
        raise ClientError(
            error_response={
                "Error": {
                    "Code": "UnknownError",
                    "Message": "unknown error",
                }
            },
            operation_name="Foo",
        )

    error_ = translate(ClientError, to=ResponseError)(raiser)
    with pytest.raises(ResponseError) as excinfo:
        error_()
    assert excinfo.value.error_code == "UnknownError"
