import os.path

import requests
import json

from botify.common import http


def release(url, tag, changelog, dry_run=False):
    """Create a github release

    :param tag: the tag corresponding to the release
    :type tag: str
    :param changelog: the release changelog
    :type changelog: str
    :param dry_run: if True, nothing is actually done.
                    the function just prints what it would do
    :type dry_run: bool

    """
    release_parameters = {
        "tag_name": tag,
        "name": tag,
        "body": changelog,
        "draft": False,
        "prerelease": False,
    }
    auth_msg = "\n{} Github authentication {}".format("*" * 20, "*" * 20)
    release_url = os.path.join(url, 'releases')

    if not dry_run:
        _, auth = http.authenticate(release_url, auth_msg, retry=True)
        response = requests.post(
            url,
            json.dumps(release_parameters),
            auth=auth,
        )
        if not response.ok:
            print "Could not create github version [{}]: {}".format(
                response.status_code,
                response.text,
            )
    else:
        username = "user"
        password = "password"
        print "POST {} ({}, {})".format(url, username, password)
