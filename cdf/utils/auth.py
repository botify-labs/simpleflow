import requests

from oauth2client.client import AccessTokenCredentials

from cdf.core.settings import SOCIAL_AUTH_GOOGLE_OAUTH2_KEY, SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET


def get_credentials(access_token, refresh_token=None):
    """
    Get oauth2 credentials from an `acess_token`
    If the token has expired, refresh it
    """
    if token_is_valid(access_token):
        credentials = AccessTokenCredentials(
            access_token,
            'my-user-agent/1.0',
        )
        return credentials
    elif refresh_token:
        access_token = refresh_access_token(refresh_token)
        return get_credentials(access_token)
    raise Exception('Token is not valid and refresh_token is not set')

def token_is_valid(access_token):
    token = requests.get(
        'https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={}'.format(access_token)
    )
    return token.status_code == 200


def refresh_access_token(refresh_token):
    token = requests.post(
        'https://accounts.google.com/o/oauth2/token',
        data={
            'client_id': SOCIAL_AUTH_GOOGLE_OAUTH2_KEY,
            'client_secret': SOCIAL_AUTH_GOOGLE_OAUTH2_SECRET,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token',
        }
    )
    return token.json()['access_token']
