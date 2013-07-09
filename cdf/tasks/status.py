import json
import urllib2


def call_api(api_endpoint, token, data):
    r = urllib2.Request(api_endpoint, data, {'Content-Type': 'application/json'})
    r.add_header('Authorization', 'Token %s' % token)
    r.add_data(json.dumps(data))

    response = urllib2.urlopen(r)
    print response.read()
    if response.getcode() != 201:
        raise Exception('Update has failed')
