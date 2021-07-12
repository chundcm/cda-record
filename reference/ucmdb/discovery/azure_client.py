import adal
import rest_requests as requests


def debug(*args, **kwargs):
    import logger

    logger.debug(*args)


class AzureCredential(object):
    def __init__(self, username, password):
        super(AzureCredential, self).__init__()
        self.username = username
        self.password = password


class AzureClientCredential(object):
    def __init__(self, client_id, client_key):
        super(AzureClientCredential, self).__init__()
        self.client_id = client_id
        self.client_key = client_key


class AzureClient(object):
    def __init__(self, api_endpoint, proxies=None, verify=False):
        super(AzureClient, self).__init__()
        self.api_endpoint = api_endpoint
        self.proxies = proxies
        self.verify = verify
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Basic Y2Y6',
        }

    def login_with_client_credentials(self, client_credential):
        token_response = adal.acquire_token_with_client_credentials(str(self.api_endpoint),
                                                                    str(client_credential.client_id),
                                                                    str(client_credential.client_key),
                                                                    proxies=self.proxies)
        access_token = token_response['accessToken']
        if not access_token:
            raise Exception('Login failed')
        self.headers.update({'Authorization': 'Bearer ' + access_token})

    def getValues(self, url):
        rsp = requests.get(url, proxies=self.proxies, headers=self.get_headers(), verify=self.verify)
        json_rsp = rsp.json()
        if json_rsp:
            return json_rsp['value']

    def getJsonResponse(self, url):
        rsp = requests.get(self.api_endpoint + url, proxies=self.proxies, debug=False, verify=self.verify)
        json_rsp = rsp.json()
        return json_rsp

    def get_headers(self):
        return self.headers