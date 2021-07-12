# coding=utf-8
import rest_json as json
import rest_requests as requests


def debug(*args, **kwargs):
    print args
    import logger
    logger.debug(*args)


class CloudFoudryCredential(object):
    def __init__(self, username, password):
        super(CloudFoudryCredential, self).__init__()
        self.username = username
        self.password = password


class CloudFoundryClient(object):
    def __init__(self, api_endpoint, url_api_version, proxies=None, verify=False):
        super(CloudFoundryClient, self).__init__()
        self.api_endpoint = api_endpoint.strip("/")
        self.url_api_version = url_api_version
        self.proxies = proxies
        self.verify = verify
        self.headers = {
            'Accept-Encoding': 'deflate',
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic Y2Y6'
        }

    def login(self, credential):
        INFO_URL = '/%s/info' % self.url_api_version
        TOKEN_URL = '/oauth/token'
        params = {
            'grant_type': 'password',
            'username': credential.username,
            'password': credential.password,
        }

        rsp = requests.get(self.api_endpoint + INFO_URL, proxies=self.proxies, debug=False, verify=self.verify)
        json_rsp = rsp.json()
        authorization_endpoint = json_rsp['authorization_endpoint']
        self.api_version = json_rsp['api_version']

        rsp = requests.post(authorization_endpoint + TOKEN_URL, headers=self.headers, proxies=self.proxies, params=params, debug=False, verify=self.verify)
        json_rsp = rsp.json()

        if not json_rsp:
            raise Exception('Login failed')
        if rsp.status_code == 401:
            raise Exception('%s:%s' % (json_rsp['error'], json_rsp['error_description']))
        access_token = json_rsp['access_token']
        token_type = json_rsp['token_type']
        self.headers.update({'Authorization': '%s %s' % (token_type, access_token)})

    def getResources(self, url):
        next_url = url
        while next_url:
            rsp = requests.get(self.api_endpoint + next_url, proxies=self.proxies, headers=self.headers, verify=self.verify)
            json_rsp = rsp.json()
            if json_rsp:
                yield json_rsp['resources']
                next_url = json_rsp['next_url']

    def getJsonResponse(self, url):
        rsp = requests.get(self.api_endpoint + url, proxies=self.proxies, debug=False, verify=self.verify)
        json_rsp = rsp.json()
        return json_rsp

    def getJsonRsp(self, url):
        rsp = requests.get(self.api_endpoint + url, proxies=self.proxies, headers=self.headers, debug=False, verify=self.verify)
        if rsp.status_code == 404:
            debug('Fail to get URL: ', url)
            return None
        json_rsp = rsp.json()
        return json_rsp

    def getCFEvents(self, timestamp=None, apiVersion='v2'):
        params = {'q': 'timestamp>%s' % timestamp}
        if timestamp:
            rsp = requests.get(self.api_endpoint + '/%s/events' % apiVersion, proxies=self.proxies, headers=self.headers,
                               debug=False, verify=self.verify, stream=True, params=params)
        else:
            rsp = requests.get(self.api_endpoint + '/%s/events' % apiVersion, proxies=self.proxies, headers=self.headers,
                               debug=False, verify=self.verify, stream=True)
        return rsp

    def getCFNextEvents(self, url):
        rsp = requests.get(self.api_endpoint + url, proxies=self.proxies, headers=self.headers,
                           debug=False, verify=self.verify, stream=True)
        return rsp