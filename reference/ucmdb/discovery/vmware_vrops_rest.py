# coding=utf-8

import piezo.ports.requests as requests

DEFAULT_PAGESIZE = 1000


class VROpsClient(object):
    """Class which provides methods for accessing the vROps REST API."""

    def __init__(self, baseurl, page_size=DEFAULT_PAGESIZE, verify=False):
        self.baseurl = baseurl
        self.page_size = page_size
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.verify = verify

    def authenticate(self, username, password):
        """Authenticate to vROps with valid credentials and receive access
        token."""
        url = '{}/auth/token/acquire'.format(self.baseurl)
        data = {
            "username": username,
            "password": password,
        }
        response = self.send_post_request(url, data)
        self.headers.update({
            'Authorization': 'vRealizeOpsToken {}'.format(response['token'])
        })

    def get_resources(self, resource_kind=None, page=0):
        """Request all resources or resources of a specific kind from vROps."""
        url = '{}/resources'.format(self.baseurl)
        params = {
            'pageSize': self.page_size,
            'page': page,
        }
        if resource_kind is not None:
            params.update({
                'resourceKind': resource_kind,
            })
        return self.send_get_request(url, params)

    def get_related_resources(self, resource_id, page=0):
        """Request all the related resources of the given CI."""
        url = '{}/resources/{}/relationships'.format(self.baseurl, resource_id)
        params = {
            'pageSize': self.page_size,
            'page': page,
        }
        return self.send_get_request(url, params)

    def get_resource_properties(self, resource_id, page=0):
        """Request properties of resource."""
        url = '{}/resources/{}/properties'.format(self.baseurl, resource_id)
        params = {
            'pageSize': self.page_size,
            'page': page,
        }
        return self.send_get_request(url, params)

    def get_adapters(self, adapter_type=None):
        """Request all vRpos adapters or all adapters of a specific type (No
        paging provided by API)."""

        url = '{}/adapters'.format(self.baseurl)
        params = {}
        if adapter_type is not None:
            params = {
                'adapterKindKey': adapter_type,
            }
        return self.send_get_request(url, params)

    def send_get_request(self, url, params=None):
        """Send get request to given URL and return response as JSON"""
        try:
            response = requests.get(url, headers=self.headers,
                                    params=params, verify=self.verify)
        except requests.ConnectionError:
            msg = "Network at {} is unreachable".format(url)
            raise requests.ConnectionError(msg)
        response.raise_for_status()
        try:
            json = response.json()
        except ValueError:
            msg = "Response from {} cannot be parsed as JSON".format(url)
            raise ValueError(msg)
        return json

    def send_post_request(self, url, data):
        """Send post request to given URL and return response as JSON"""
        try:
            response = requests.post(url, headers=self.headers, json=data,
                                     verify=self.verify)
        except requests.ConnectionError:
            msg = "Network at {} is unreachable".format(url)
            raise requests.ConnectionError(msg)
        response.raise_for_status()
        try:
            json = response.json()
        except ValueError:
            msg = "Response from {} cannot be parsed as JSON".format(url)
            raise ValueError(msg)
        return json
