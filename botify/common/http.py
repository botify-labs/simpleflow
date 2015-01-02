import getpass

import requests


def authenticate(url, auth_msg=None, retry=True):
    """Interactive authenticate for a webservice

    :param url: url of the webservice endpoint
    :param auth_msg: message to show the user
    :param retry: if retry is needed

    :return: (success, auth)
        :rtype: (bool, `requests.auth.HTTPBasicAuth`)

    """
    def _auth():
        if auth_msg:
            print auth_msg
        username = raw_input("username: ")
        password = getpass.getpass()
        auth = requests.auth.HTTPBasicAuth(username, password)
        ok = requests.get(url, auth=auth).ok
        return ok, auth

    ok, auth = _auth()

    while retry and not ok:
        ok, auth = _auth()

    return ok, auth
